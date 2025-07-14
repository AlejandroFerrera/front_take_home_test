import sqlalchemy as sa
import sqlalchemy.dialects.postgresql as sap
from config.config import Config


class DBHandler:
    """
    Database handler for managing PostgreSQL database connections and operations.
    """

    def __init__(self, config):
        """
        Initialize the database handler.

        Args:
            config: Configuration object containing database connection parameters.
        """
        self.config: Config = config
        self.connection_string = self.__create_connection_string()
        self.engine = self.__create_engine()
        self.conn = self.engine.connect()
        self.metadata = sa.MetaData()
        self.metadata.reflect(bind=self.engine)

    def __create_connection_string(self) -> str:
        """
        Create a PostgreSQL connection string from configuration parameters.

        Returns:
            str: The database connection string.
        """

        params = {
            "DB_USER": self.config.POSTGRES_USER,
            "DB_PASSWORD": self.config.POSTGRES_PASSWORD,
            "DB_HOST": self.config.POSTGRES_HOST,
            "DB_PORT": self.config.POSTGRES_PORT,
            "DB_NAME": self.config.POSTGRES_DB,
        }

        return f"postgresql+psycopg2://{params['DB_USER']}:{params['DB_PASSWORD']}@{params['DB_HOST']}:{params['DB_PORT']}/{params['DB_NAME']}"

    def __create_engine(self) -> sa.engine.Engine:
        """
        Create a SQLAlchemy engine from the connection string.

        Returns:
            sa.engine.Engine: The SQLAlchemy engine instance.
        """
        connection_string = self.__create_connection_string()
        return sa.create_engine(connection_string)

    def insert_many(
        self, table: sa.Table, data: list[dict], returning_cols: list[str] | None = None
    ) -> sa.CursorResult:
        """
        Inserts multiple records into the specified table.

        Args:
            table (sa.Table): The SQLAlchemy Table object where the data will be inserted.
            data (list[dict]): A list of dictionaries, each representing a row to insert.
            returning_cols (list[str] | None, optional): List of column names to return after insertion.
                If None, no columns are returned.

        Returns:
            sa.CursorResult: The result of the insert operation, optionally including the specified returning columns.
        """

        stmt = sa.insert(table).values(data)
        if returning_cols:
            stmt = stmt.returning(*[getattr(table.c, col) for col in returning_cols])

        result = self.conn.execute(stmt)
        self.conn.commit()

        return result

    def update(
        self,
        table: sa.Table,
        values: dict,
        matching_columns: list[str],
        fields_to_update: list[str],
        returning_columns: list[str] | None = None,
    ) -> sa.CursorResult:
        """
        Updates records in the specified table based on matching_columns.

        Args:
            table (sa.Table): The SQLAlchemy Table object.
            values (dict): The values to update (must include keys for matching_columns and fields_to_update).
            matching_columns (list[str]): Columns to match for the update condition.
            fields_to_update (list[str]): Columns to update.
            returning_columns (list[str] | None, optional): Columns to return after update.

        Returns:
            sa.CursorResult: The result of the update operation.
        """

        where_clause = sa.and_(
            *[getattr(table.c, col) == values[col] for col in matching_columns]
        )
        update_dict = {col: values[col] for col in fields_to_update if col in values}

        stmt = sa.update(table).where(where_clause).values(**update_dict)

        if returning_columns:
            stmt = stmt.returning(*[getattr(table.c, col) for col in returning_columns])

        result = self.conn.execute(stmt)
        self.conn.commit()
        return result

    def upsert(
        self,
        table: sa.Table,
        values: dict,
        conflict_columns: list[str],
        update_columns: list[str],
        returning_columns: list[str] | None = None,
    ) -> sa.CursorResult:
        """
        Upserts a single record into the specified table.

        Args:
            table (sa.Table): The SQLAlchemy table object.
            values (dict): The data to insert or update.
            conflict_columns (list[str]): Columns to check for conflicts.
            update_columns (list[str]): Columns to update on conflict.
            returning_columns (list[str] | None, optional): Columns to return after upsert.

        Returns:
            sa.CursorResult: The result of the upsert operation.
        """
        stmt = sap.insert(table).values(**values)

        stmt = stmt.on_conflict_do_update(
            index_elements=conflict_columns,
            set_={col: values[col] for col in update_columns},
        )

        if returning_columns:
            stmt = stmt.returning(*[getattr(table.c, col) for col in returning_columns])

        result = self.conn.execute(stmt)
        self.conn.commit()
        return result

    def close(self):
        """
        Closes the database connection
        """
        try:
            if hasattr(self, "conn") and self.conn:
                self.conn.close()
        except Exception:
            pass

        try:
            if hasattr(self, "engine") and self.engine:
                self.engine.dispose()
        except Exception:
            pass
