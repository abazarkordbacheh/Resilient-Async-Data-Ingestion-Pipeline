# 🚀 Resilient Async Data Ingestion Pipeline

[![Python Version](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![AsyncIO](https://img.shields.io/badge/asyncio-enabled-green.svg)](https://docs.python.org/3/library/asyncio.html)
[![SQLAlchemy](https://img.shields.io/badge/SQLAlchemy-ORM-red.svg)](https://www.sqlalchemy.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

An enterprise-grade, high-throughput asynchronous data ingestion pipeline engineered to pull **~30 million records daily** from a REST web service and load them into a Microsoft SQL Server database while strictly adhering to a **5-minute SLA**.

---
## 🏗️ Architecture & Core Modules
The system is decoupled into modular, highly resilient components built on top of `asyncio` and `httpx`:
```text
Resilient-Async-Ingestor/
│
├── src/
│   ├── connection/
│   │   └── db_handler.py        # SQLConnection — SQLAlchemy engine manager for SQL Server
│   └── ...                      # further packages as the project grows
│
├── pipeline/
│   ├── pipeline.py              # main asynchronous orchestrator loop
│   ├── token_manager.py         # proactive JWT caching with expiration & asyncio.Lock
│   ├── retry_worker.py          # background worker for failed batches (asyncio.Queue)
│   └── monitor_lag.py           # real-time lag monitoring via /getlag/
│
├── test/
│   └── test_db_handler.py       # pytest suite: sys.path resolution + monkeypatched engine
│
├── config.py                    # centralized configuration (endpoints, credentials, DB params)
├── requirements.txt
└── README.md                    # Comprehensive unit & integration tests (Pytest)
```
## ⚡Key Architectural Features
High-Performance I/O Concurrency: Leverages Python’s asyncio and httpx to eliminate network I/O bottlenecks during batch fetching.
Proactive Token Caching & Safety: Manages jwt_login efficiently with an internal cache and asyncio.Lock to prevent frequency limits, rate-throttling, and race conditions.
Non-Blocking Fault Tolerance (Dead-Letter / Retry Queue):
If a batch fails to commit to SQL Server, it is intercepted and pushed to an isolated asyncio.Queue.
A dedicated background worker (retry_worker.py) re-processes and re-loads failed batches without halting or stalling the primary live ingestion stream.
Real-Time Lag Tracking: Continuously monitors remaining un-ingested records (/getlag/) to guarantee compliance with the 5-minute SLA.
## 🛠️ Tech Stack
Language: Python 3.10+
Concurrency: asyncio, httpx
Database & ORM: SQLAlchemy, pymssql, Microsoft SQL Server
Testing: Pytest, unittest.mock
🧪 Running Tests
```
Ensure your dependencies are installed, then run the test suite with pytest:

# Install dependencies
pip install -r requirements.txt

# Run tests with PYTHONPATH set
python -m pytest -v
```
## ⚙️Configuration
Configure your environment variables or update config.py with your target service endpoints, credentials, and database parameters:
```
BASE_URL = "http://{service_ip}:{service_port}"
USERNAME = "your_username"
PASSWORD = "your_password"
```
## 📄License
This project is licensed under the MIT License. See the LICENSE file for details.


