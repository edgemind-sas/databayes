import mysql.connector
from mysql.connector import Error
import pydantic
from .db_base import DBBase, DMBSConfigBase


class DBMySQL(DBBase):
    """
    The `DBMySQL` class provides methods for interacting with a MySQL database.
    It supports operations like connect, query, insert, update, and delete.
    """

    config: DMBSConfigBase = pydantic.Field(
        default=DMBSConfigBase(), description="MySQL database configuration."
    )

    def connect(self, **params):
        """
        Establishes a connection to the MySQL server using provided configuration.
        """
        try:
            self.bkd = mysql.connector.connect(
                host=self.config.host,
                port=int(self.config.port),
                user=self.config.username,
                password=self.config.password,
                database=self.config.database,
                **params,
            )
            if self.logger:
                self.logger.info("Connection to MySQL DB successful")

            return True
        except Error as e:
            if self.logger:
                self.logger.error(f"Error connecting to MySQL DB: {e}")

            return False

    def query(self, sql, params=None):
        """
        Executes a SQL query and returns the fetched results.
        """
        try:
            cursor = self.bkd.cursor(dictionary=True)
            cursor.execute(sql, params or ())
            result = cursor.fetchall()
            cursor.close()
            return result
        except Error as e:
            if self.logger:
                self.logger.error(f"Failed to execute query: {e}")
            return []

    def insert(self, table, data):
        """
        Inserts a new record or multiple records into a given table.
        """
        # If data is a single dictionary, make it a list of dictionaries
        if isinstance(data, dict):
            data = [data]

        placeholders = ", ".join(["%s"] * len(data[0]))
        columns = ", ".join(data[0].keys())
        sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        try:
            cursor = self.bkd.cursor()
            # Since data is now guaranteed to be a list of dictionaries, we can proceed.
            cursor.executemany(sql, [tuple(d.values()) for d in data])
            self.bkd.commit()
            cursor.close()
        except Error as e:
            if self.logger:
                self.logger.error(f"Failed to insert data into table {table}: {e}")

    def update(self, table, data, condition):
        """
        Updates records in the specified table that meet the given condition.
        """
        set_clause = ", ".join([f"{k} = %s" for k in data.keys()])
        sql = f"UPDATE {table} SET {set_clause} WHERE {condition}"
        try:
            cursor = self.bkd.cursor()
            cursor.execute(sql, list(data.values()))
            self.bkd.commit()
            cursor.close()
        except Error as e:
            if self.logger:
                self.logger.error(f"Failed to update table {table}: {e}")

    def delete(self, table, condition):
        """
        Deletes records from a table that meet the given condition.
        """
        sql = f"DELETE FROM {table} WHERE {condition}"
        try:
            cursor = self.bkd.cursor()
            cursor.execute(sql)
            self.bkd.commit()
            cursor.close()
        except Error as e:
            if self.logger:
                self.logger.error(f"Failed to delete from table {table}: {e}")

    def close(self):
        """Closes the database connection."""
        if self.bkd.is_connected():
            self.bkd.close()
            if self.logger:
                self.logger.info("MySQL DB connection closed.")
