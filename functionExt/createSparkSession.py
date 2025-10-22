"""
Módulo para crear la sesión de Spark necesaria para el pipeline.

Lee las variables de entorno desde un archivo '.env' y configura
el intérprete de Python para PySpark.

Ejemplo de uso:
---------------
from createSparkSession import createSparkSession
spark = createSparkSession().createSparkCluster()
"""
import os
from pyspark.sql import SparkSession

class createSparkSession:
    """
    Clase para crear y configurar la sesión de Spark.
    """
    def __init__(self):
        pass

    def createSparkCluster(self):
        """
        Carga variables de entorno desde '.env', configura Python para Spark
        y devuelve una sesión de Spark inicializada.
        """
        with open('.env') as f:
            for line in f:
                if line.strip() and not line.startswith('#'):
                    key, value = line.strip().split('=', 1)
                    os.environ[key] = value

        os.environ["PYSPARK_PYTHON"] = r"C:\Users\USER\AppData\Local\Programs\Python\Python311\python.exe"
        os.environ["PYSPARK_DRIVER_PYTHON"] = r"C:\Users\USER\AppData\Local\Programs\Python\Python311\python.exe"

        spark = (
            SparkSession.builder
            .appName("TransformData")
            .config("spark.python.worker.faulthandler.enabled", "true")
            .config("spark.sql.execution.pyspark.udf.faulthandler.enabled", "true")
            .getOrCreate()
        )
        print("Correcto")
        return spark
