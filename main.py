"""
Script principal del pipeline.

Crea la base de datos, inicia la sesión de Spark y ejecuta
la carga de los archivos CSV (excepto validation.csv).
Luego ejecuta la carga del archivo validation.csv para
actualizar estadísticas.

Uso:
----
python main.py
"""
from functionExt.createSparkSession import createSparkSession
from extract.extractData import extractData
from functionExt.createBDPragma import createBDPragma


if __name__ == "__main__":

    #Creamos la BD donde alojaremos toda la informacion correspondiente
    createBDPragma().createBDPragmaTest()

    extractor = createSparkSession()

    spark = extractor.createSparkCluster()

    # Instanciar extractor
    c = extractData("./data/bd/pragmaTest.db")

    # Leer CSVs (por ejemplo, carpeta ./data)
    df = c.readCSVPragma("./data", spark)

    c.runValidation(spark)
