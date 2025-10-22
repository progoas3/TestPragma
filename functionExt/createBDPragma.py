"""
Modulo para crear la base de datos y las tablas necesarias para el pipeline

Crea:
- Base de datos SQLite 'pragmaTest.db'
- Tabla 'transactions' para los datos de los CSV
- Tabla 'stats' para las estadísticas de 'price'

Ejemplo de uso:
---------------
from createBDPragma import createBDPragma
db = createBDPragma()
db.createBDPragmaTest()
"""
import sqlite3
import os

class createBDPragma:
    """
        Clase para crear la base de datos y las tablas necesarias.
        """
    def __init__(self):
        pass


    def createBDPragmaTest(self):
        """
        Crea la base de datos './data/bd/pragmaTest.db' y las tablas 'transactions' y 'stats'.
               """
        os.makedirs("./data/bd", exist_ok=True)
        conn = sqlite3.connect("./data/bd/pragmaTest.db")
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            price REAL  NULL,
            user_id TEXT  NULL
        );
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            total_count INTEGER,
            avg_price REAL,
            min_price REAL,
            max_price REAL
        );
        """)

        conn.commit()
        conn.close()

        print("Base de datos 'pragma_data.db' creada con las tablas 'transactions' y 'stats'.")
