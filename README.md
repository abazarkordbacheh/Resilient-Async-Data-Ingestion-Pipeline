# 🚀 Resilient-Async-Ingestor

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![AsyncIO](https://img.shields.io/badge/AsyncIO-Native-green.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-Latest-teal.svg)
![Pytest](https://img.shields.io/badge/Pytest-7.4+-red.svg)
![MSSQL](https://img.shields.io/badge/MSSQL-SQLAlchemy-orange.svg)
![License](https://img.shields.io/badge/License-MIT-yellow.svg)

A high-throughput, fault-tolerant asynchronous ETL/ELT data ingestion pipeline engineered for extracting millions of records from REST APIs under strict SLAs and loading them safely into Microsoft SQL Server using modern Python async patterns.

---

## 📋 Executive Summary

**Resilient-Async-Ingestor** is a production-grade data pipeline designed to handle large-scale data ingestion with a **5-minute SLA guarantee**. The system leverages Python's `asyncio` for non-blocking I/O, `httpx` for efficient HTTP client operations, and implements sophisticated retry mechanisms with exponential backoff to ensure data integrity even during transient failures.

### Key Features

- ⚡ **High-Throughput Architecture**: Async producer-consumer pattern with configurable backpressure
- 🔐 **Smart Authentication**: Proactive JWT token caching with TTL and thread-safe refresh
- 🔄 **Fault-Tolerant Retry**: Background dead-letter queue with exponential backoff
- 📊 **Real-Time Monitoring**: Lag tracking against SLA thresholds
- 🗄️ **Database Resilience**: Connection pooling with SQLAlchemy for MSSQL
- 🧪 **Comprehensive Testing**: Unit and integration tests with mock FastAPI server

---

## 🏗️ System Architecture

### Component Mapping

#### [pipeline.py](cci:7://file:///E:/Project/Resilient-Async-Ingestor/pipline/pipeline.py:0:0-0:0) - Main Orchestrator
The central engine implementing the asynchronous producer-consumer loop. It coordinates between the external API producer, the database consumer, and the retry worker. Features include:
- Continuous batch fetching with configurable batch sizes
- Automatic backpressure management via `asyncio.Queue`
- Graceful error handling with dead-letter queue routing
- TaskGroup-based concurrent task management

#### [token_manager.py](cci:7://file:///E:/Project/Resilient-Async-Ingestor/src/connection/token_manager.py:0:0-0:0) - JWT Authentication Manager
Proactive token lifecycle manager ensuring authentication never becomes a bottleneck:
- **TTL-based caching**: Tokens are cached and refreshed proactively before expiration
- **Double-checked locking**: Uses `asyncio.Lock` to prevent race conditions during concurrent token refreshes
- **Rate-limit protection**: Prevents duplicate login requests from multiple coroutines
- Configurable TTL (default: 300 seconds)

#### [retry_worker.py](cci:7://file:///E:/Project/Resilient-Async-Ingestor/pipline/retry_worker.py:0:0-0:0) - Background Recovery Worker
Non-blocking dead-letter queue processor for failed batch insertions:
- **Exponential backoff**: `base_delay * (2^(attempt-1))` for intelligent retry spacing
- **Pointer reset mechanism**: Resets remote API batch pointer via `/reset_pointer/`
- **Record refetching**: Re-fetches failed batches and re-attempts insertion
- **Unbounded queue**: No backpressure on failed batches to prevent pipeline stalls

#### [monitor_lag.py](cci:7://file:///E:/Project/Resilient-Async-Ingestor/pipline/monitor_lag.py:0:0-0:0) - SLA Monitor
Real-time ingestion lag tracker:
- Polls `/getlag/` endpoint at configurable intervals
- Logs remaining record count for SLA compliance monitoring
- Runs as background task via `asyncio.TaskGroup`

#### `db_handler.py` - Database Connection Manager
High-performance SQL Server interaction layer:
- **SQLAlchemy ORM**: Type-safe database operations with connection pooling
- **Async bridge**: Uses `asyncio.to_thread()` for non-blocking database I/O
- **Windows Authentication**: Support for integrated security
- **Transaction integrity**: Automatic transaction management per batch

#### [test/mock_server.py](cci:7://file:///E:/Project/Resilient-Async-Ingestor/test/mock_server.py:0:0-0:0) - Local Laboratory Server
FastAPI-based mock server simulating the external API:
- **Stateful pagination**: Maintains server-side batch pointer
- **Authentication simulation**: `/jwt_login/` endpoint with credential validation
- **Batch retrieval**: `/getnextbatch/` with configurable record counts
- **Pointer reset**: `/reset_pointer/` for testing retry logic
- **Lag monitoring**: `/getlag/` for SLA testing

---

## 🔄 Architecture Diagram

```mermaid
sequenceDiagram
    participant API as External API
    participant Pipeline as Pipeline Orchestrator
    participant TokenMgr as TokenManager
    participant Queue as Async Queue
    participant Retry as RetryWorker
    participant DB as SQL Server
    participant Monitor as Lag Monitor

    Pipeline->>TokenMgr: get_valid_token()
    TokenMgr->>API: POST /jwt_login/
    API-->>TokenMgr: JWT Token
    TokenMgr-->>Pipeline: Cached Token

    Pipeline->>API: GET /getnextbatch/?count=N
    API-->>Pipeline: Batch Records
    Pipeline->>DB: insert_batch(records)
    
    alt Insertion Success
        DB-->>Pipeline: Success
    else Insertion Failure
        DB-->>Pipeline: Exception
        Pipeline->>Queue: enqueue(batch_id)
    end

    Queue->>Retry: batch_id
    Retry->>TokenMgr: get_valid_token()
    Retry->>API: POST /reset_pointer/
    Retry->>API: GET /getnextbatch/
    API-->>Retry: Refetched Records
    Retry->>DB: insert_batch(records)
    
    Monitor->>TokenMgr: get_valid_token()
    Monitor->>API: POST /getlag/
    API-->>Monitor: Lag Metrics