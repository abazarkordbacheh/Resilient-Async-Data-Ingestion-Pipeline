# ------------------------ Modules ------------------------
# Libraries=>
import pytest
from unittest.mock import patch, MagicMock
# Modules=>
from src.connection.db_handler import SQLConnection

# ------------------------ test ------------------------
def test_sql_connection_windows_auth():
    """Testing initialization and URL construction for Windows Authentication mode"""
    db = SQLConnection(
        host="127.0.0.1",
        port=1433,
        database="TestDB",
        windows_authentication=True
    )

    assert db.host == "127.0.0.1"
    assert db.port == 1433
    assert db.database == "TestDB"
    assert db.windows_authentication is True
    assert db.engine is None


@patch("db_handler.sqlalchemy.create_engine")
def test_connect_windows_auth(mock_create_engine):
    """Test the connect method in Windows Authentication mode (without username and password)"""
    db = SQLConnection(
        host="localhost",
        port=1433,
        database="MasterDB",
        windows_authentication=True
    )

    db.connect()

    # Check that create_engine has been called and the database engine has been set.
    mock_create_engine.assert_called_once()
    assert db.engine is not None


@patch("db_handler.sqlalchemy.create_engine")
def test_connect_sql_auth(mock_create_engine):
    """Testing the connect method in authentication mode with username and password"""
    db = SQLConnection(
        host="192.168.1.50",
        port=1433,
        user="sa",
        password="my_secret_password",
        database="ProductionDB",
        windows_authentication=False
    )

    db.connect()

    # Check the correctness of the create_engine call
    mock_create_engine.assert_called_once()
    assert db.engine is not None


@patch("db_handler.sqlalchemy.Engine.dispose")
def test_disconnect(mock_dispose):
    """Testing the disconnect method to safely close the SQLAlchemy engine"""
    db = SQLConnection()
    # Simulating the creation of an engine
    db.engine = MagicMock()

    db.disconnect()

    # Checking the dispose method call on the engine
    mock_dispose.assert_called_once()
