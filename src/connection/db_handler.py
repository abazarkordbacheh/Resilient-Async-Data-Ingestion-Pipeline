# ------------------------ Modules ------------------------
# Libraries =>
import logging
import asyncio
import pymssql
import sqlalchemy

# ------------------------ Logger ------------------------
logger = logging.getLogger(__name__)


# ------------------------ Class SQL Server Connection ------------------------

class SQLConnection:
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
        :param user: User name
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
        Create engine
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
        Disconnect engine
        :return: None
        """
        if self.engine:
            self.engine.dispose()
            logger.info("Database engine disconnected.")

    async def insert_batch(self, records: list[dict]) -> None:
        """Insert a batch of records into the target SQL table asynchronously (via thread pool)."""
        if not records:
            return

        if not self.engine:
            raise RuntimeError("Database engine is not connected. Call connect() first.")

        # Because SQLAlchemy is inherently sync, to prevent the Event Loop from blocking,
        # We use asyncio.to_thread
        await asyncio.to_thread(self._sync_insert, records)

    def _sync_insert(self, records: list[dict]) -> None:
        with self.engine.begin() as conn:
            # فرض بر این است که جدول متناظر وجود دارد و کلیدهای دیکشنری با ستون‌ها مچ هستند
            meta = sqlalchemy.MetaData()
            table = sqlalchemy.Table(self.table, meta, autoload_with=self.engine)
            conn.execute(table.insert(), records)
        logger.info("Successfully inserted %d records into %s", len(records), self.table)