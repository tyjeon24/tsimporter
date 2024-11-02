import pandas as pd
import psycopg2
import psycopg2.extras as extras
from psycopg2 import OperationalError, sql

import logging


class ParquetImporter:
    def __init__(
        self,
        dbname: str,
        user: str,
        password: str,
        host: str,
        port: int,
        table_name: str,
        column_mappings: dict,
    ):
        self.conn_params = {
            "dbname": dbname,
            "host": host,
            "port": port,
            "user": user,
            "password": password,
        }
        self.table_name = table_name
        self.column_mappings = column_mappings

        self.conn = None
        self.cur = None
        self.create_table()

    def connect(self):
        try:
            if self.conn is None or self.conn.closed == 1:
                self.conn = psycopg2.connect(**self.conn_params)
                self.cur = self.conn.cursor()
                logging.info(
                    f"Connection to {self.conn_params['host']}:{self.conn_params['port']} established successfully."
                )
        except OperationalError:
            logging.error(f"Connection to {self.conn_params['host']}:{self.conn_params['port']} failed.")
            raise

    def create_table(self):
        columns = []
        primary_key_columns = []
        for mapping in self.column_mappings:
            col_name = mapping["target"]
            data_type = mapping["dtype"].upper()
            col_def = f"{col_name} {data_type}"
            columns.append(col_def)
            if mapping.get("primary_key", False):
                primary_key_columns.append(col_name)

        columns_sql = ",\n    ".join(columns)
        primary_key_sql = f",\n    PRIMARY KEY ({', '.join(primary_key_columns)})"
        query = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            {columns_sql}
            {primary_key_sql}
        );
        """
        try:
            self.connect()
            self.cur.execute(query)
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            self.close_connection()

    def import_files(self, parquet_files):
        try:
            self.connect()
            for parquet_file in parquet_files:
                self.process_file(parquet_file)
                logging.info(f"{parquet_file} was imported to postgres successfully.")
        except Exception:
            logging.error("Error during importing files.")
            self.conn.rollback()
            raise
        else:
            self.cur.execute(f"SELECT COUNT(*) FROM {self.table_name}")
            self.conn.commit()
            logging.info(f"Data importing succeeded, total data now: {self.cur.fetchall()[0][0]}.")
        finally:
            self.close_connection()

    def process_file(self, parquet_file):
        df_parquet = pd.read_parquet(parquet_file)
        df_parquet = (
            df_parquet.pipe(self._rename_columns).pipe(self._convert_data_types).pipe(self._handle_missing_values)
        )
        self.batch_insert(df_parquet)

    def _rename_columns(self, df):
        rename_mapping = {mapping["source"]: mapping["target"] for mapping in self.column_mappings}
        return df.rename(columns=rename_mapping)

    def _convert_data_types(self, df):
        for mapping in self.column_mappings:
            col_name = mapping["target"]
            dtype = mapping["dtype"].upper()
            if dtype == "TIMESTAMP":
                df[col_name] = pd.to_datetime(df[col_name], errors="coerce")
            elif dtype == "FLOAT":
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
            else:
                df[col_name] = df[col_name].astype(bool)
        return df

    def _handle_missing_values(self, df):
        primary_key_cols = [mapping["target"] for mapping in self.column_mappings if mapping.get("primary_key", False)]
        return df.dropna(subset=primary_key_cols)

    def batch_insert(self, df):
        cols = df.columns.tolist()
        primary_key_cols = [mapping["target"] for mapping in self.column_mappings if mapping.get("primary_key", False)]
        conflict_target = sql.SQL(", ").join(map(sql.Identifier, primary_key_cols))
        query = sql.SQL("""
            INSERT INTO {table} ({fields})
            VALUES %s
            ON CONFLICT ({conflict_target}) DO NOTHING;
        """).format(
            table=sql.Identifier(self.table_name),
            fields=sql.SQL(", ").join(map(sql.Identifier, cols)),
            conflict_target=conflict_target,
        )
        tuples = [tuple(x) for x in df.to_numpy()]
        extras.execute_values(self.cur, query.as_string(self.cur), tuples)
        self.conn.commit()

    def close_connection(self):
        self.cur.close()
        self.conn.close()
