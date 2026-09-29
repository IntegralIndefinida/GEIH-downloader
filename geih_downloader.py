"""
GEIH Downloader & Extractor
===========================
Automated pipeline to discover, download, and extract microdata from Colombia's
Gran Encuesta Integrada de Hogares (GEIH) and its official Empalme Series (CNPV 2018)
from DANE's National Data Archive (ANDA).

Features:
  - Exclusive CSV extraction for key modules (Housing, Demographics, Labor force,
    Employed, Other forms of work).
  - Preserves household, individual, and geographic identifiers (DIRECTORIO,
    SECUENCIA_P, ORDEN, HOGAR, DPTO, MPIO, CLASE, AREA, FEX_C).
  - Automatic handling of nested archives, non-breaking spaces, and historical naming typos.
  - Resilient downloads with retry backoff and incremental caching (skips already extracted months).

Author: Luis Ramírez
License: MIT
Version: 1.0.0
"""

import os
import io
import re
import sys
import json
import time
import zipfile
import logging
import argparse
import unicodedata
from pathlib import Path
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

__version__ = "1.0.0"

# Configuración del log (consola y archivo)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("geih_download.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# Catálogos oficiales verificados en ANDA (DANE) para la GEIH Principal Nacional
KNOWN_CATALOGS = {
    "2007": "317",
    "2008": "206",
    "2009": "207",
    "2010": "205",
    "2011": "182",
    "2012": "77",
    "2013": "68",
    "2014": "328",
    "2015": "356",
    "2016": "427",
    "2017": "458",
    "2018": "547",
    "2019": "599",
    "2020": "780",
    "2021": "701",
    "2022": "771",
    "2023": "782",
    "2024": "819",
    "2025": "853",
    "2026": "900",
}

# Catálogos oficiales verificados en ANDA (DANE) para la Serie de Empalme (Marco CNPV 2018)
EMPALME_CATALOGS = {
    "2010": "765",
    "2011": "755",
    "2012": "760",
    "2013": "761",
    "2014": "756",
    "2015": "762",
    "2016": "757",
    "2017": "763",
    "2018": "758",
    "2019": "759",
    "2020": "764",
}

# Palabras clave para identificar los módulos solicitados (sin tildes, minúsculas)
TARGET_MODULE_PATTERNS = [
    "vivienda",                # Vivienda y Hogares / Datos del hogar y la vivienda
    "hogar",
    "caracteristica",          # Características generales (Personas)
    "caracterica",             # Variación/typo de DANE en 2007 (Octubre, Noviembre, Diciembre)
    "personas",
    "fuerza de trabajo",       # Fuerza de trabajo
    "ocupados",                # Ocupados
    "otras formas de trabajo", # Otras formas de trabajo / Otras actividades
    "otras actividades"
]

# Exclusiones explícitas para evitar falsos positivos
EXCLUDE_MODULE_PATTERNS = [
    "no ocupados",
    "desocupados",
    "deocupados",              # Typo ocasional en DANE (ej. marzo 2010)
    "inactivos",
    "otros ingresos",
    "migracion",
    "tipo de investigacion"
]


def normalize_text(text: str) -> str:
    """Elimina acentos/diacríticos y normaliza espacios no separables (NBSP)."""
    text = text.replace("\xa0", " ").replace("\u202f", " ").replace("\u200b", "")
    return "".join(
        c for c in unicodedata.normalize("NFD", text)
        if unicodedata.category(c) != "Mn"
    ).lower()


def is_target_csv(filename: str) -> bool:
    """
    Determina si un archivo dentro del ZIP es un CSV correspondiente a uno de los
    módulos solicitados:
      - Identificación de vivienda y hogar
      - Características generales (Personas)
      - Fuerza de trabajo
      - Ocupados
      - Otras formas de trabajo / Otras actividades
    """
    if not filename.lower().endswith(".csv"):
        return False

    norm_name = normalize_text(filename)

    # Si coincide con exclusiones explícitas (e.g. 'no ocupados', 'desocupados')
    if any(ex in norm_name for ex in EXCLUDE_MODULE_PATTERNS):
        return False

    # Debe contener alguno de los patrones de los módulos objetivo
    return any(p in norm_name for p in TARGET_MODULE_PATTERNS)


def sanitize_member_name(raw_name: str) -> str:
    """Limpia caracteres corruptos habituales de codificación DOS/Windows en ZIPs."""
    name = Path(raw_name).name
    # Mapeo de caracteres comunes distorsionados por CP437 en ZIPs antiguos de Windows
    mojibake_map = {
        "╡": "A", "µ": "A", "Á": "A",
        "Ý": "i", "¡": "i", "í": "i",
        "¾": "o", "¢": "o", "ó": "o",
        "é": "e", "ú": "u", "ñ": "n", "Ñ": "N"
    }
    # Intentar decodificación limpia
    try:
        clean = name.encode("cp437").decode("utf-8")
    except Exception:
        clean = name

    # Reemplazar posibles caracteres de dibujo de cajas o fallos de tabla OEM
    for bad, good in [("╡rea", "Area"), ("µrea", "Area"), ("CaracterÝsticas", "Caracteristicas"), ("Caracter¡sticas", "Caracteristicas"), ("educaci¾n", "educacion"), ("educaci¢n", "educacion")]:
        clean = clean.replace(bad, good)

    return clean


class GEIHDownloader:
    def __init__(self, base_dir="data", keep_zips=True, extract_csv=True, is_empalme=False):
        self.is_empalme = is_empalme
        self.base_url = "https://microdatos.dane.gov.co"
        base_path = Path(base_dir)
        self.base_dir = base_path / "empalme" if is_empalme else base_path
        self.metadata_dir = self.base_dir / "metadata"
        self.raw_dir = self.base_dir / "raw_zips"
        self.csv_dir = self.base_dir / "datos_csv"
        self.keep_zips = keep_zips
        self.extract_csv = extract_csv

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
        })

        self._setup_directories()

    def _setup_directories(self):
        """Crea la estructura de carpetas local."""
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        if self.extract_csv:
            self.csv_dir.mkdir(parents=True, exist_ok=True)
        series_label = "Serie de Empalme (2010-2020)" if self.is_empalme else "Serie Principal (2007-2026)"
        logging.info(f"Directorio base configurado para [{series_label}] en: {self.base_dir.resolve()}")

    def discover_catalogs(self) -> dict:
        """
        Descubre dinámicamente los catálogos en ANDA mediante búsqueda AJAX.
        Si la búsqueda falla o está incompleta, usa los catálogos verificados como respaldo.
        """
        if self.is_empalme:
            logging.info("Buscando catálogos de la Serie de Empalme GEIH en el portal del DANE...")
            discovered = {}
            for page in [1, 2]:
                search_url = f"{self.base_url}/index.php/catalog/search"
                params = {"sk": "Empalme", "ps": 100, "page": page}
                headers = {"X-Requested-With": "XMLHttpRequest"}
                try:
                    res = self.session.get(search_url, params=params, headers=headers, timeout=20)
                    res.raise_for_status()
                    soup = BeautifulSoup(res.text, "html.parser")
                    for a in soup.find_all("a", href=True):
                        m = re.search(r"/catalog/(\d+)$", a["href"])
                        if not m:
                            continue
                        cat_id = m.group(1)
                        title = a.get_text(strip=True)
                        if "GEIH" in title.upper() and "EMPALME" in title.upper():
                            yr_match = re.search(r"\b(20\d\d)\b", title)
                            if yr_match:
                                discovered[yr_match.group(1)] = cat_id
                except Exception as e:
                    logging.warning(f"Error buscando catálogos de empalme en página {page}: {e}")
            final_catalogs = dict(EMPALME_CATALOGS)
            final_catalogs.update(discovered)
            logging.info(f"Catálogos de Empalme identificados para {len(final_catalogs)} años: {sorted(final_catalogs.keys())}")
            return final_catalogs

        logging.info("Buscando catálogos de la GEIH Nacional en el portal del DANE...")
        discovered = {}
        page = 1
        max_pages = 5

        while page <= max_pages:
            search_url = f"{self.base_url}/index.php/catalog/search"
            params = {"sk": "GEIH", "ps": 100, "page": page}
            headers = {"X-Requested-With": "XMLHttpRequest"}

            try:
                res = self.session.get(search_url, params=params, headers=headers, timeout=20)
                res.raise_for_status()
                soup = BeautifulSoup(res.text, "html.parser")

                found_on_page = 0
                for a in soup.find_all("a", href=True):
                    href = a["href"]
                    m = re.search(r"/catalog/(\d+)$", href)
                    if not m:
                        continue
                    cat_id = m.group(1)
                    title = a.get_text(strip=True)

                    if not title or not ("GEIH" in title.upper() or "GRAN ENCUESTA" in title.upper()):
                        continue

                    # Filtrar módulos satélite y submuestras territoriales
                    norm_title = normalize_text(title)
                    is_satellite = any(w in norm_title for w in [
                        "san andres", "amazon", "orinoqu", "ciudades intermedias",
                        "infantil", "mti", "mfpt", "formacion", "migracion",
                        "tic", "mtic", "etnia", "empalme", "factores de expansion", "ciiu"
                    ])

                    if not is_satellite:
                        year_match = re.search(r"\b(200[6-9]|20[1-3][0-9])\b", title)
                        if year_match:
                            year = year_match.group(1)
                            if year not in discovered:
                                discovered[year] = cat_id
                                found_on_page += 1

                if found_on_page == 0:
                    break
                page += 1

            except Exception as e:
                logging.warning(f"No se pudo completar la búsqueda en página {page}: {e}")
                break

        # Combinar con los catálogos verificados para garantizar cobertura completa
        final_catalogs = dict(KNOWN_CATALOGS)
        final_catalogs.update(discovered)

        logging.info(f"Catálogos identificados para {len(final_catalogs)} años: {sorted(final_catalogs.keys())}")
        return final_catalogs

    def fetch_metadata(self, year: str, catalog_id: str) -> dict:
        """Obtiene y almacena el JSON metodológico de ANDA."""
        metadata_path = self.metadata_dir / f"{year}.json"
        if metadata_path.exists():
            try:
                with open(metadata_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass

        json_url = f"{self.base_url}/index.php/metadata/export/{catalog_id}/json"
        try:
            res = self.session.get(json_url, timeout=20)
            if res.status_code == 200:
                data = res.json()
                with open(metadata_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=4, ensure_ascii=False)
                logging.info(f"Metadata metodológica guardada: {metadata_path.name}")
                return data
            else:
                logging.warning(f"Endpoint metadata JSON retornó HTTP {res.status_code} para catálogo {catalog_id}")
        except Exception as e:
            logging.warning(f"No se pudo descargar metadata JSON para {year}: {e}")
        return {}

    def get_csv_download_links(self, catalog_id: str) -> list:
        """
        Extrae los enlaces de descarga para archivos CSV desde /get-microdata.
        Maneja dos épocas del DANE:
          1. 2007-2021: ZIPs separados por formato (*.csv.zip)
          2. 2022-2026: ZIPs mensuales completos que contienen CSV, DTA y SAV en su interior
        """
        microdata_url = f"{self.base_url}/index.php/catalog/{catalog_id}/get-microdata"
        try:
            res = self.session.get(microdata_url, timeout=20)
            res.raise_for_status()
        except Exception as e:
            logging.error(f"Error accediendo a página de microdatos del catálogo {catalog_id}: {e}")
            return []

        # Extraer parámetros de onclick="mostrarModal('filename', 'download_url')"
        matches = re.findall(r"mostrarModal\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)", res.text)
        if not matches:
            logging.warning(f"No se encontraron enlaces de descarga en catálogo {catalog_id}")
            return []

        # Deduplicar por URL de descarga
        unique_files = {}
        for name, url in matches:
            clean_url = url.strip()
            clean_name = name.strip()
            if clean_url not in unique_files:
                unique_files[clean_url] = clean_name

        # 1. Verificar si existen archivos con extensión específica de CSV (ej. Enero.csv.zip)
        csv_specific = [
            {"name": name, "url": url}
            for url, name in unique_files.items()
            if "csv" in name.lower()
        ]

        if csv_specific:
            return csv_specific

        # 2. Si no hay archivos específicos de CSV (años 2022 en adelante),
        # los datos vienen empaquetados mensualmente (ej. Ene_2024.zip, Enero 2025.zip).
        # Descartamos archivos de documentación o complementarios no mensuales.
        monthly_bundles = []
        for url, name in unique_files.items():
            norm_name = normalize_text(name)
            # Ignorar FEX u otros zips de apoyo si no son los datos mensuales
            if "fex" in norm_name or "proyecciones" in norm_name:
                continue
            if norm_name.endswith(".zip"):
                monthly_bundles.append({"name": name, "url": url})

        return monthly_bundles

    def download_file(self, url: str, dest_path: Path, max_retries: int = 3) -> bool:
        """
        Descarga un archivo con streaming y verificación de integridad.
        """
        if dest_path.exists() and dest_path.stat().st_size > 0:
            logging.info(f"Archivo ya existente: {dest_path.name} ({dest_path.stat().st_size / 1024 / 1024:.1f} MB). Saltando descarga.")
            return True

        temp_dest = dest_path.with_suffix(dest_path.suffix + ".part")

        for attempt in range(1, max_retries + 1):
            try:
                logging.info(f"Descargando {dest_path.name} (intento {attempt}/{max_retries})...")
                with self.session.get(url, stream=True, timeout=45) as r:
                    r.raise_for_status()
                    total_size = int(r.headers.get("content-length", 0))
                    downloaded = 0

                    with open(temp_dest, "wb") as f:
                        for chunk in r.iter_content(chunk_size=65536):
                            if chunk:
                                f.write(chunk)
                                downloaded += len(chunk)

                    if total_size > 0 and downloaded < total_size:
                        logging.warning(f"Descarga incompleta ({downloaded}/{total_size} bytes). Reintentando...")
                        continue

                temp_dest.rename(dest_path)
                size_mb = dest_path.stat().st_size / 1024 / 1024
                logging.info(f"✓ Descarga completada: {dest_path.name} ({size_mb:.2f} MB)")
                return True

            except Exception as e:
                logging.error(f"Error descargando {dest_path.name} (intento {attempt}): {e}")
                if temp_dest.exists():
                    temp_dest.unlink()
                time.sleep(2 * attempt)

        return False

    def _extract_from_zip(self, z_obj: zipfile.ZipFile, dest_dir: Path) -> int:
        count = 0
        for member in z_obj.infolist():
            if member.is_dir():
                continue

            # Soporte para ZIPs anidados (ej. CSV.zip dentro de paquetes mensuales 2022-2024 o 1. Enero.zip en Empalme)
            if member.filename.lower().endswith(".zip") and not any(fmt in member.filename.lower() for fmt in ["dta", "sav", "spss"]):
                try:
                    nested_bytes = io.BytesIO(z_obj.read(member))
                    with zipfile.ZipFile(nested_bytes) as nested_z:
                        inner_stem = Path(member.filename).stem
                        # Si el zip anidado es CSV.zip, se extrae en el dest_dir actual
                        # Si es un mes (ej. 1. Enero.zip), se extrae en una subcarpeta para ese mes
                        if inner_stem.lower() in ["csv", "csv_zip"]:
                            target_dest = dest_dir
                        else:
                            target_dest = dest_dir / inner_stem
                        target_dest.mkdir(parents=True, exist_ok=True)
                        count += self._extract_from_zip(nested_z, target_dest)
                except Exception as e:
                    logging.warning(f"Error abriendo ZIP anidado {member.filename}: {e}")
                continue

            if is_target_csv(member.filename):
                clean_filename = sanitize_member_name(member.filename)
                target_file = dest_dir / clean_filename

                if target_file.exists() and target_file.stat().st_size > 0:
                    count += 1
                    continue

                with z_obj.open(member) as source, open(target_file, "wb") as dest:
                    dest.write(source.read())
                count += 1
        return count

    def extract_requested_modules(self, zip_path: Path, dest_dir: Path) -> int:
        """
        Inspecciona el archivo ZIP descargado y extrae EXCLUSIVAMENTE los archivos CSV
        de los módulos solicitados:
          - Vivienda y Hogares / Datos del hogar y la vivienda
          - Características generales (Personas)
          - Fuerza de trabajo
          - Ocupados
          - Otras formas de trabajo / Otras actividades
        Conserva los identificadores de personas (DIRECTORIO, SECUENCIA_P, ORDEN)
        y geográficos (DPTO, MPIO, CLASE, AREA) ya incluidos en dichas tablas.
        Soporta además archivos ZIP anidados internamente (ej. CSV.zip o meses).
        """
        if not zip_path.exists():
            return 0

        dest_dir.mkdir(parents=True, exist_ok=True)
        extracted_count = 0

        try:
            with zipfile.ZipFile(zip_path, "r") as z:
                extracted_count = self._extract_from_zip(z, dest_dir)

            logging.info(f"  → Extraídos {extracted_count} CSVs de módulos solicitados desde {zip_path.name}")
        except zipfile.BadZipFile:
            logging.error(f"El archivo {zip_path.name} está corrupto o no es un ZIP válido.")
        except Exception as e:
            logging.error(f"Error extrayendo módulos de {zip_path.name}: {e}")

        return extracted_count

    def process_year(self, year: str, catalog_id: str, limit_files: int = None):
        """Descarga y procesa un año específico de la GEIH o de la Serie de Empalme."""
        series_label = f"Empalme Año {year}" if self.is_empalme else f"Año {year}"
        logging.info(f"\n{'='*20} Procesando GEIH {series_label} (Catálogo {catalog_id}) {'='*20}")

        # 1. Metadata metodológica
        self.fetch_metadata(year, catalog_id)

        # 2. Obtener lista de archivos a descargar (solo CSV)
        files = self.get_csv_download_links(catalog_id)
        if not files:
            logging.warning(f"No se encontraron enlaces válidos de microdatos para {year}")
            return

        if limit_files and limit_files > 0:
            files = files[:limit_files]

        logging.info(f"Se procesarán {len(files)} archivos para {year}.")
        year_raw_dir = self.raw_dir / year
        year_raw_dir.mkdir(parents=True, exist_ok=True)

        year_csv_dir = self.csv_dir / year
        if self.extract_csv:
            year_csv_dir.mkdir(parents=True, exist_ok=True)

        # 3. Descargar y extraer selectivamente
        for item in files:
            safe_name = "".join(c for c in item["name"] if c.isalnum() or c in (" ", ".", "_", "-")).strip()
            zip_dest = year_raw_dir / safe_name
            month_stem = safe_name.replace(".csv.zip", "").replace(".zip", "").strip()

            if self.is_empalme:
                # En la serie de empalme, el archivo anual empaqueta los 12 meses adentro
                existing_csvs = list(year_csv_dir.rglob("*.csv")) + list(year_csv_dir.rglob("*.CSV"))
                if len(existing_csvs) >= 60:
                    logging.info(f"Año {year} de Empalme ya cuenta con {len(existing_csvs)} archivos extraídos. Saltando.")
                    continue

                success = self.download_file(item["url"], zip_dest)
                if success and self.extract_csv:
                    self.extract_requested_modules(zip_dest, year_csv_dir)
                    if not self.keep_zips and zip_dest.exists():
                        zip_dest.unlink()
            else:
                month_csv_dir = year_csv_dir / month_stem
                # Comprobar si ya fue extraído previamente para evitar descargas redundantes
                expected_min = 5 if int(year) >= 2022 else (3 if year == "2020" else 13)
                existing_csvs = list(month_csv_dir.glob("*.csv")) + list(month_csv_dir.glob("*.CSV"))
                if len(existing_csvs) >= expected_min:
                    logging.info(f"Mes {month_stem} ({year}) ya cuenta con {len(existing_csvs)} archivos extraídos. Saltando.")
                    continue

                success = self.download_file(item["url"], zip_dest)
                if success and self.extract_csv:
                    self.extract_requested_modules(zip_dest, month_csv_dir)
                    if not self.keep_zips and zip_dest.exists():
                        zip_dest.unlink()

            time.sleep(1)

    def run_pipeline(self, target_years=None, limit_files: int = None):
        """Ejecuta el pipeline de descarga para todos los años o los años seleccionados."""
        catalogs = self.discover_catalogs()

        years_to_process = sorted(catalogs.keys(), key=lambda y: int(y))
        if target_years:
            target_years = [str(y) for y in target_years]
            years_to_process = [y for y in years_to_process if y in target_years]

        series_name = "Empalme" if self.is_empalme else "Principal"
        logging.info(f"Iniciando descarga de la serie [{series_name}] para {len(years_to_process)} años: {years_to_process}")

        for year in years_to_process:
            self.process_year(year, catalogs[year], limit_files=limit_files)

        logging.info(f"\n¡Pipeline [{series_name}] finalizado exitosamente!")
        logging.info(f"Datos CSV organizados en: {self.csv_dir.resolve()}")


def main():
    parser = argparse.ArgumentParser(
        description="Descargador oficial de microdatos GEIH del DANE (Solo formato CSV y módulos clave)."
    )
    parser.add_argument(
            "--years",
        nargs="+",
        help="Años específicos a descargar (ej. --years 2016 o --years 2010 2011 2012)."
    )
    parser.add_argument(
        "--empalme",
        action="store_true",
        help="Descargar la serie oficial de Empalme (2010-2020) con los factores de expansión CNPV 2018."
    )
    parser.add_argument(
        "--all-series",
        action="store_true",
        help="Descargar tanto la serie principal (2007-2026) como la serie de Empalme (2010-2020)."
    )
    parser.add_argument(
        "--limit-files",
        type=int,
        default=None,
        help="Límite de archivos mensuales a descargar por año (útil para pruebas, ej. --limit-files 1)."
    )
    parser.add_argument(
        "--base-dir",
        default="data",
        help="Directorio base de descarga (por defecto: 'data')."
    )
    parser.add_argument(
        "--no-extract",
        action="store_true",
        help="Si se activa, solo descarga los ZIPs y no extrae los CSVs."
    )
    parser.add_argument(
        "--delete-zips",
        action="store_true",
        help="Elimina los archivos ZIP una vez extraídos los CSVs para ahorrar espacio en disco."
    )

    args = parser.parse_args()

    try:
        if args.all_series:
            logging.info("\n=== INICIANDO DESCARGA DE LA SERIE PRINCIPAL (2007-2026) ===")
            dl_main = GEIHDownloader(
                base_dir=args.base_dir,
                keep_zips=not args.delete_zips,
                extract_csv=not args.no_extract,
                is_empalme=False
            )
            dl_main.run_pipeline(target_years=args.years, limit_files=args.limit_files)

            logging.info("\n=== INICIANDO DESCARGA DE LA SERIE DE EMPALME (2010-2020) ===")
            dl_empalme = GEIHDownloader(
                base_dir=args.base_dir,
                keep_zips=not args.delete_zips,
                extract_csv=not args.no_extract,
                is_empalme=True
            )
            dl_empalme.run_pipeline(target_years=args.years, limit_files=args.limit_files)
        else:
            downloader = GEIHDownloader(
                base_dir=args.base_dir,
                keep_zips=not args.delete_zips,
                extract_csv=not args.no_extract,
                is_empalme=args.empalme
            )
            downloader.run_pipeline(target_years=args.years, limit_files=args.limit_files)
    except KeyboardInterrupt:
        logging.warning("\nProceso interrumpido manualmente por el usuario. Saliendo limpiamente...")
        sys.exit(0)


if __name__ == "__main__":
    main()
