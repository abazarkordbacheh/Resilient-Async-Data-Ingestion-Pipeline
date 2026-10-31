# region Import Modules...
import logging
import asyncio
import pymssql
import sqlalchemy

# endregion

# region Configure Logger...
logger = logging.getLogger(__name__)


# endregion

# region SQL Server Connection...
class SQLServerConnection:
    """
    SQLAlchemy connection class
    """

    def __init__(
            self,
            host: str = 'localhost',
            port: int = 1433,
            user='root',
            password: str = '',
            database: str = 'master',
            windows_authentication: bool = True
    ) -> None:
        """
        initialization
        :param host: Server ip address
        :param port: Port number
        :param user: Username
        :param password: Password
        :param database: Database name
        :param windows_authentication: True or False
        :returns: None
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.windows_authentication = windows_authentication
        self.engine = None

    def connect(self) -> None:
        """
        Create engine and connection to the Database engine
        :return: None
        """
        if self.windows_authentication:
            url = sqlalchemy.engine.URL.create(
                drivername="mssql+pymssql",
                host=self.host,
                port=self.port,
                database=self.database,
            )
        else:
            url = sqlalchemy.engine.URL.create(
                drivername="mssql+pymssql",
                username=self.user,
                password=self.password,
                host=self.host,
                port=self.port,
                database=self.database,
            )
        self.engine = sqlalchemy.create_engine(url)
        logger.info("Database engine created successfully for host=%s", self.host)

    def disconnect(self) -> None:
        """
        Disconnect Database engine
        :return: None
        """
        if self.engine:
            self.engine.dispose()
            logger.info("Database engine disconnected.")

    async def insert_batch(self, records: list[dict], table, schema) -> None:
        """
        Insert a batch of records into the target SQL table asynchronously (via thread pool).
        :param records: List of records to insert
        :param table: Table name
        :param schema: Schema name
        :return: None
        """
        if not records:
            return

        if not self.engine:
            raise RuntimeError("Database engine is not connected. Call connect() first.")

        await asyncio.to_thread(self._sync_insert, records, table, schema)

    def _sync_insert(self, records: list[dict], table, schema) -> None:
        with self.engine.begin() as conn:
            meta = sqlalchemy.MetaData()
            table = sqlalchemy.Table(table, meta, schema=schema, autoload_with=self.engine)
            conn.execute(table.insert(), records)
        logger.info("Successfully inserted %d records into %s", len(records), table)

# endregion
