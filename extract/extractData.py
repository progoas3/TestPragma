"""
Este modulo se encarga de la logica principal de extraccion y llamado a funciones de escritura
que se encuentran en el apartado de "LOAD"

CLASES:
 extractData Clase principal para manejar la extraccion y llamada a funciones de escritura

Metodos:
    readCSVPragma(csv_folder, spark)
    Lee todos los archivos CSV del folder especificado excepto ('validation.csv') y los
    inserta en la base de datos usando la clase loadBDPragma

    runValidation(spark)
    Ejecuta la carga del archivo "validation.csv" y actualiza las estadísticas acumuladas


"""

import os
from load.loadBDPragma import loadBDPragma

class extractData:
    """
        Clase para la extraccion y carga de datos en la base de datos

        Atributos:
        ----------
        db_path : str
            Ruta de la base de datos SQLite
        db_loader : loadBDPragma
            Instancia del objeto encargado de escribir en la base de datos y actualizar estadísticas.
        """
    def __init__(self, db_path):
        self.db_path = db_path
        self.db_loader = loadBDPragma()

    def readCSVPragma(self, csv_folder, spark):
        """
                Lee todos los archivos CSV del folder especificado excepto ('validation.csv') y
                los inserta en la base de datos

                Parametros:
                -----------
                csv_folder : str
                    Carpeta donde se encuentran los archivos CSV.
                spark : SparkSession
                    Sesión de Spark para leer los archivos CSV.
                """
        for file_name in sorted(os.listdir(csv_folder)):
            # Ignorar archivos no válidos
            if not file_name.endswith(".csv") or "validation" in file_name:
                continue

            file_path = os.path.join(csv_folder, file_name)
            print(f"Archivo leído: {file_name}")

            # Leer CSV con Spark
            df = spark.read.csv(file_path, header=True, inferSchema=True)
            num_filas = df.count()
            print(f"Filas cargadas desde {file_name}: {num_filas}")

            # Convertir a pandas
            df_pd = df.toPandas()
            self.db_loader.loadBDPragmaNew(self.db_path, df_pd, num_filas)

    def runValidation(self, spark):
        """
                Ejecuta la carga del archivo 'validation.csv' y actualiza las estadísticas acumuladas.

                Parametros:
                -----------
                spark : SparkSession
                    Sesión de Spark para leer el archivo CSV.
                """
        print("\n------------Ejecutando validación con validation.csv------------------------\n")
        validation_path = "./data/validation.csv"

        # Leer archivo con Spark
        df_spark = spark.read.option("header", True).option("inferSchema", True).csv(validation_path)
        df_pd = df_spark.toPandas()

        num_filas = len(df_pd)
        print(f"Archivo leído: validation.csv")
        print(f"Filas cargadas desde validation.csv: {num_filas}")

        # Cargar en BD usando el mismo metodo ya usado anteriormeente
        self.db_loader.loadBDPragmaNew(self.db_path, df_pd, num_filas)
        print(f"{num_filas} filas insertadas en la base de datos (validación).")

        print("\nValidación final completada.")
