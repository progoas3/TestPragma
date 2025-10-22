"""
Modulo para cargar datos en la base de datos y actualizar estadísticas.

- Inserta los datos de un DataFrame en la tabla 'transactions'.
- Mantiene estadísticas acumuladas (total de filas, promedio, mínimo y máximo)
  en la tabla 'stats' sin recalcular todo desde cero.
- Muestra información de inserciones y estadísticas actualizadas en consola.
"""

import sqlite3
import pandas as pd


class loadBDPragma:

    def __init__(self):
        self.total_insertadas = 0  # atributo de clase

    def loadBDPragmaNew(self, db_path, df_pd, num_filas):
        """Inserta los datos en 'transactions' y actualiza estadísticas."""
        # Conexión a la base de datos
        with sqlite3.connect(db_path) as conn:
            df_pd.to_sql("transactions", conn, if_exists="append", index=False)

        # Actualizar contador total
        self.total_insertadas += num_filas
        print(f"{num_filas} filas insertadas en la base de datos.\n")

        print(f"Total de filas insertadas en esta ejecución: {self.total_insertadas}")

        self.loadBDStats(db_path, df_pd)

        # Verificar total acumulado en BD
        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM transactions")
            total_bd = cursor.fetchone()[0]
            print(f"Total de registros actualmente en la base de datos: {total_bd}")


    def loadBDStats(self, db_path, df_pd):
        """
        Actualiza la tabla 'stats' acumulando valores sin recalcular todo.
        """
        if "price" not in df_pd.columns:
            print("No se encontró la columna 'price' en el DataFrame, no se actualizarán estadísticas.")
            return

        # Convertir columna 'price' a numérica
        df_pd["price"] = pd.to_numeric(df_pd["price"], errors="coerce")
        df_pd = df_pd.dropna(subset=["price"])

        if df_pd.empty:
            print("DataFrame vacío tras limpieza. No se actualizan estadísticas.")
            return

        # Estadísticas del batch actual
        new_count = len(df_pd)
        new_avg = float(df_pd["price"].mean())
        new_min = float(df_pd["price"].min())
        new_max = float(df_pd["price"].max())
        print(f"Valor promedio: {new_avg}")
        print(f"Valor minimo: {new_min}")
        print(f"Valor maximo: {new_max}")

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()

            # Obtener el último registro de stats
            cursor.execute("""
                SELECT total_count, avg_price, min_price, max_price 
                FROM stats ORDER BY id DESC LIMIT 1
            """)
            prev = cursor.fetchone()

            if prev is None:
                total_count = new_count
                avg_price = new_avg
                min_price = new_min
                max_price = new_max
            else:
                """Convertir todo a float para evitar el error de tipos"""
                prev_count = int(prev[0])
                prev_avg = float(prev[1])
                prev_min = float(prev[2])
                prev_max = float(prev[3])

                total_count = prev_count + new_count
                avg_price = ((prev_avg * prev_count) + (new_avg * new_count)) / total_count
                min_price = float(min(prev_min, new_min))
                max_price = float(max(prev_max, new_max))

            # Insertar nueva fila con estadísticas actualizadas
            cursor.execute("""
                INSERT INTO stats (total_count, avg_price, min_price, max_price)
                VALUES (?, ?, ?, ?)
            """, (total_count, avg_price, min_price, max_price))

            conn.commit()

        print("\n📊 Estadísticas actualizadas:")
        print(f"   Total registros acumulados: {total_count}")
        print(f"   Precio mínimo: {min_price}")
        print(f"   Precio máximo: {max_price}")
        print(f"   Precio promedio: {avg_price:.2f}\n")

        with sqlite3.connect(db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM stats")
            filas = cursor.fetchall()  # trae todas las filas de la tabla

            print("Contenido actual de la tabla 'stats':")
            for fila in filas:
                print(fila)

            print(f"\nTotal de registros: {len(filas)}")

