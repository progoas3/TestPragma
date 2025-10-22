# TestPragma

## Descripción
Este proyecto contiene un **pipeline de procesamiento de datos** diseñado para leer archivos CSV, almacenarlos en una base de datos SQLite y mantener estadísticas acumuladas sobre los precios de los registros. El pipeline también permite validar los datos mediante un archivo específico `validation.csv`.

El objetivo principal es demostrar cómo cargar datos de forma incremental, actualizar estadísticas en tiempo real y garantizar que los datos anteriores no se recalculen cada vez.

---

## Estructura del proyecto
- `extract/` → Módulo `extractData.py` encargado de leer los archivos CSV y ejecutar la validación.
- `load/` → Módulo `loadBDPragma.py` que maneja la carga a la base de datos y actualización de estadísticas.
- `functionExt/` → Contiene utilidades como:
  - `createBDPragma.py` → Crea la base de datos SQLite y las tablas necesarias.
  - `createSparkSession.py` → Inicializa la sesión de Spark.
- `data/` → Carpeta donde se ubican los archivos CSV y BD (`pragmaTest.db` y los archivos principales).
- `main.py` → Script principal que ejecuta todo el pipeline.

---

## Funcionalidades
1. **Carga de archivos CSV**  
   - Lee todos los archivos `.csv` de la carpeta `data`, excepto `validation.csv`.
   - Inserta los datos en la tabla `transactions` de la base de datos.
   - Actualiza estadísticas acumuladas (`count`, `avg`, `min`, `max`) del campo `price` sin recalcular todo el dataset.

2. **Validación de datos**  
   - Ejecuta el archivo `validation.csv` a través del mismo pipeline.
   - Actualiza las estadísticas y permite comparar los valores antes y después de la validación.

3. **Seguimiento de estadísticas**  
   - Cada inserción actualiza la tabla `stats` con:
     - Total de registros acumulados
     - Precio promedio
     - Precio mínimo
     - Precio máximo
   - Permite imprimir las estadísticas actuales después de cada archivo procesado.

---

## Requisitos
- Python 3.8 o superior
- PySpark
- Pandas
- SQLite3

