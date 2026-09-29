# Descargador y Extractor de Microdatos GEIH 🇨🇴

[![Python Version](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/)
[![Licencia: MIT](https://img.shields.io/badge/Licencia-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Fuente de Datos: DANE](https://img.shields.io/badge/Fuente%20de%20Datos-DANE%20ANDA-green.svg)](https://microdatos.dane.gov.co/)

Herramienta en Python diseñada para automatizar el descubrimiento, descarga y extracción modular de microdatos de la **Gran Encuesta Integrada de Hogares (GEIH)** y su **Serie Oficial de Empalme (Marco CNPV 2018)** directamente desde el Archivo Nacional de Datos (ANDA) del Departamento Administrativo Nacional de Estadística (DANE).

---

## 📌 Descripción General

La **GEIH** es la principal investigación estadística de Colombia sobre mercado laboral, ingresos y condiciones de vida en los hogares. Sin embargo, descargar y procesar su serie histórica directamente desde el portal web del DANE suele presentar dificultades operativas:
- **Cambios metodológicos y de empaquetado:** Variaciones en la estructura de los archivos entre el Marco Censal 2005 y el Marco 2018.
- **Duplicación masiva de formatos:** Cada mes se publica simultáneamente en formatos CSV, Stata (`.dta`) y SPSS (`.sav`), generando descargas redundantes que superan con creces los 100 GB.
- **Inconsistencias técnicas en el servidor:** Archivos ZIP anidados internamente (como `CSV.zip` o meses individuales comprimidos dentro del paquete anual), erratas históricas en nombres de archivo y caracteres con espacios no separables (`\xa0`).

**GEIH Downloader** resuelve estos problemas ofreciendo un flujo automatizado, robusto e incremental que extrae **exclusivamente los módulos requeridos en formato CSV**, conservando íntegras todas las variables de identificación para cruces (*merges/joins*) y reduciendo el almacenamiento necesario en más de un **80%**.

---

## 🚀 Características Principales

- **Cobertura Histórica Completa:**
  - **Serie Nacional Principal:** Serie continua desde **2007 hasta 2026** (20 años completos).
  - **Serie Oficial de Empalme (Marco CNPV 2018):** Serie armonizada retrospectivamente desde **2010 hasta 2020** (11 años) con factores de expansión reponderados (`FEX_C`).
- **Extracción Selectiva de CSV:** Descarga y descomprime únicamente los archivos CSV correspondientes a los módulos centrales, descartando carpetas pesadas de otros formatos no solicitados.
- **Identificadores y Llaves Primarias Preservados:**
  - **Identificadores de Persona y Hogar:** `DIRECTORIO`, `SECUENCIA_P`, `ORDEN`, `HOGAR`.
  - **Variables Geográficas y Temporales:** `DPTO`, `MPIO`, `CLASE` (Cabecera vs. Resto), `AREA`, `MES`, `PERIODO`.
  - **Factores de Ponderación:** `FEX_C` (factor de expansión mensual/empalme), `FEX_DPTAL`, `PEX_C`.
- **Manejo Robusto de Archivos:**
  - Descompresión recursiva de paquetes ZIP anidados internamente (ej. `CSV.zip` y meses en la serie de empalme).
  - Normalización de codificación de caracteres (maneja tablas DOS/Windows CP437/CP850 y espacios no separables `\xa0`).
  - Reconocimiento de erratas históricas en los nombres de archivo del DANE (ej. `Caractericas generales`).
- **Descargas Incrementales Inteligentes:** Si se interrumpe o se vuelve a ejecutar, detecta qué meses ya fueron extraídos y los salta en fracciones de segundo.
- **Tolerancia a Fallos de Red:** Reintentos automáticos con pausas progresivas y descarga fragmentada (*streaming*) ante caídas de conexión.

---

## 📊 Módulos Extraídos

El extractor filtra de manera estandarizada los siguientes módulos clave para cada mes y año:

| Módulo Objetivo | Descripción del Contenido | Variables Clave de Ejemplo |
| :--- | :--- | :--- |
| **Vivienda y Hogares** | Tipo de vivienda, servicios públicos, tenencia del inmueble y composición del hogar. | `DIRECTORIO`, `SECUENCIA_P`, `P5000`, `P5010`, `HOGAR` |
| **Características Generales** | Variables sociodemográficas, edad, sexo, parentesco, educación y seguridad social. | `DIRECTORIO`, `SECUENCIA_P`, `ORDEN`, `P6020`, `P6040` |
| **Fuerza de Trabajo** | Condición de actividad laboral, población en edad de trabajar (PET), rama de actividad. | `DIRECTORIO`, `SECUENCIA_P`, `ORDEN`, `RAMA2D`, `pt` |
| **Ocupados** | Empleo detallado, horas laboradas, ingresos laborales, formalidad e informalidad. | `DIRECTORIO`, `SECUENCIA_P`, `ORDEN`, `P6430`, `INGLABO` |
| **Otras Formas de Trabajo** | Actividades secundarias, ayudas no remuneradas, labores de cuidado en el hogar. | `DIRECTORIO`, `SECUENCIA_P`, `ORDEN`, `P7040` |

---

## 🛠️ Instalación y Requisitos

1. **Clonar el repositorio:**
   ```bash
   git clone https://github.com/tu-usuario/geih-downloader.git
   cd geih-downloader
   ```

2. **Crear y activar un entorno virtual (recomendado):**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate   # En Windows: .venv\Scripts\activate
   ```

3. **Instalar dependencias:**
   ```bash
   pip install -r requirements.txt
   ```

---

## 💻 Guía de Uso y Ejemplos de Comandos

### 1. Descargar la Serie Principal Completa (2007–2026)
Descarga y extrae los 20 años en la carpeta `data/datos_csv/`:
```bash
python3 geih_downloader.py
```

### 2. Descargar la Serie Oficial de Empalme (2010–2020)
Descarga los microdatos con factores reponderados Marco CNPV 2018 en `data/empalme/datos_csv/`:
```bash
python3 geih_downloader.py --empalme
```

### 3. Descargar Ambas Series (Principal y Empalme)
```bash
python3 geih_downloader.py --all-series
```

### 4. Descargar Años Específicos
```bash
# Años puntuales de la serie principal
python3 geih_downloader.py --years 2021 2022 2023 2024

# Año puntual de la serie de empalme
python3 geih_downloader.py --empalme --years 2016
```

### 5. Ahorrar Espacio en Disco (`--delete-zips`)
Elimina automáticamente los archivos comprimidos `.zip` intermedios una vez extraídos los CSVs:
```bash
python3 geih_downloader.py --delete-zips
```

### 6. Personalizar Directorio y Descargar Solo ZIPs
```bash
# Especificar una carpeta de destino personalizada
python3 geih_downloader.py --base-dir "/ruta/a/mi_carpeta"

# Descargar únicamente los paquetes ZIP sin descomprimirlos
python3 geih_downloader.py --no-extract
```

---

## 📂 Estructura de Salida Generada

El directorio de datos se organiza automáticamente por año y mes:

```text
data/
├── metadata/                   # JSONs metodológicos oficiales del catálogo DANE
│   ├── 2007.json
│   └── ...
├── raw_zips/                   # Paquetes ZIP mensuales (si no se usa --delete-zips)
│   └── 2024/
│       └── Ene_2024.zip
├── datos_csv/                  # SERIE PRINCIPAL (2007-2026)
│   ├── 2007/
│   │   ├── Enero/
│   │   │   ├── Cabecera - Características generales (Personas).csv
│   │   │   ├── Cabecera - Fuerza de trabajo.csv
│   │   │   ├── Cabecera - Ocupados.csv
│   │   │   ├── Cabecera - Otras actividades y ayudas en la semana.csv
│   │   │   └── Cabecera - Vivienda y Hogares.csv
│   │   └── ...
│   └── 2024/
│       └── Ene_2024/
│           ├── Características generales, seguridad social en salud y educación.CSV
│           ├── Datos del hogar y la vivienda.CSV
│           ├── Fuerza de trabajo.CSV
│           ├── Ocupados.CSV
│           └── Otras formas de trabajo.CSV
│
└── empalme/                    # SERIE DE EMPALME (Marco CNPV 2018, 2010-2020)
    ├── metadata/
    └── datos_csv/
        └── 2016/
            ├── 1. Enero/
            │   ├── Características generales (personas).CSV
            │   ├── Fuerza de trabajo.CSV
            │   ├── Ocupados.CSV
            │   ├── Otras actividades y ayudas en la semana.CSV
            │   └── Vivienda y Hogares.CSV
            └── ... / 12. Diciembre/
```

---

## 📋 Parámetros de Línea de Comandos

| Parámetro | Valor por Defecto | Descripción |
| :--- | :--- | :--- |
| `--years` | `None` (Todos) | Lista de años a procesar separados por espacio (ej. `--years 2016 2017`). |
| `--empalme` | `False` | Descarga la serie de Empalme (2010–2020) con ponderaciones del Marco CNPV 2018. |
| `--all-series` | `False` | Ejecuta secuencialmente la Serie Principal y la Serie de Empalme. |
| `--base-dir` | `"data"` | Carpeta raíz donde se almacenan las descargas y extracciones. |
| `--delete-zips`| `False` | Borra los archivos `.zip` tras extraer los CSVs para liberar almacenamiento. |
| `--no-extract` | `False` | Descarga únicamente los archivos comprimidos sin descomprimirlos. |
| `--limit-files`| `None` | Límite de paquetes mensuales por año (útil para pruebas y depuración). |

---

## ⚖️ Licencia y Descargo de Responsabilidad

- **Licencia del Código:** Este software se distribuye bajo la licencia libre [MIT](LICENSE).
- **Titularidad de los Datos:** Todos los microdatos estadísticos son producidos y publicados por el **Departamento Administrativo Nacional de Estadística (DANE)** de Colombia bajo políticas públicas de datos abiertos. Los usuarios deben acatar la legislación sobre reserva estadística (Ley 79 de 1993).
- **Cita Sugerida para Investigaciones:**
  > *Departamento Administrativo Nacional de Estadística (DANE). Gran Encuesta Integrada de Hogares (GEIH) [Microdatos]. Archivo Nacional de Datos (ANDA), Colombia.*
