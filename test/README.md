# Test Suite Documentation

## Overview

This test suite provides comprehensive testing for the Resilient-Async-Ingestor pipeline, including unit tests for isolated logic and integration tests for end-to-end workflows.

## Test Structure

```
test/
├── conftest.py                    # Shared fixtures and test configuration
├── mock_server.py                 # FastAPI mock server for API simulation
├── unit/                          # Unit tests
│   ├── __init__.py
│   ├── test_token_manager.py      # TokenManager tests
│   └── test_retry_worker.py       # RetryWorker tests
└── integration/                   # Integration tests
    ├── __init__.py
    └── test_pipeline_integration.py  # End-to-end pipeline tests
```

## Dependencies

Install test dependencies:

```bash
pip install -r requirements.txt
```

Required packages:
- `pytest>=7.4.0` - Test framework
- `pytest-asyncio>=0.21.0` - Async test support
- `fastapi>=0.100.0` - Mock server
- `uvicorn[standard]>=0.23.0` - Mock server ASGI server
- `pydantic>=2.0.0` - Data validation

## Running Tests

### Run All Tests

```bash
pytest test/
```

### Run Unit Tests Only

```bash
pytest test/unit/
```

### Run Integration Tests Only

```bash
pytest test/integration/
```

### Run Specific Test File

```bash
pytest test/unit/test_token_manager.py
```

### Run Specific Test

```bash
pytest test/unit/test_token_manager.py::TestTokenManagerInitialization::test_token_manager_initialization
```

### Run with Verbose Output

```bash
pytest test/ -v
```

### Run with Coverage Report

```bash
pytest test/ --cov=src --cov=pipline --cov-report=html
```

## Test Fixtures

### Shared Fixtures (conftest.py)

- `sample_token` - Mock JWT token
- `sample_batch_data` - Sample API batch response
- `sample_empty_batch` - Empty batch response (end of stream)
- `sample_lag_response` - Sample lag monitoring response
- `mock_http_client` - httpx.AsyncClient for testing
- `mock_http_response` - Factory for creating mock responses
- `token_manager` - TokenManager instance with test credentials
- `expired_token_manager` - TokenManager with expired token
- `mock_db_connection` - Mock SQLServerConnection
- `retry_worker` - RetryWorker instance with mocked dependencies
- `mock_server_url` - Base URL for mock server (http://localhost:8000)

## Unit Tests

### Token Manager Tests (`test_token_manager.py`)

Tests for JWT token management:

- **Initialization**: Verifies correct attribute setup
- **Token Validation**: Tests TTL-based validity checking
- **Token Cache**: Tests caching behavior and refresh logic
- **Lock Mechanism**: Tests async double-checked locking for concurrent requests
- **Login Method**: Tests authentication success and failure scenarios
- **Edge Cases**: Tests boundary conditions around token expiration

Key test scenarios:
- Token refresh when expired
- Token refresh when None
- Double-checked locking prevents duplicate login requests
- Lock is released after token refresh
- Login raises on non-200 status
- Login raises on missing access token

### Retry Worker Tests (`test_retry_worker.py`)

Tests for batch retry logic:

- **Initialization**: Verifies correct attribute setup
- **Queue Operations**: Tests enqueue/dequeue and queue capacity
- **Exponential Backoff**: Tests delay calculation and application
- **Retry Attempts**: Tests max retry attempts and failure behavior
- **Process One Logic**: Tests reset pointer, fetch, and insert sequence
- **Run Forever**: Tests background task behavior

Key test scenarios:
- Exponential backoff calculation: `base_delay * (2^(attempt-1))`
- Max retry attempts exceeded raises RuntimeError
- Successful retry on first attempt
- Successful retry after initial failure
- Reset pointer API call verification
- Insert called with refetched records
- Run forever handles exceptions gracefully

## Integration Tests

### Pipeline Integration Tests (`test_pipeline_integration.py`)

End-to-end tests with mock server:

- **Token Manager with Mock Server**: Tests authentication against mock API
- **Retry Worker with Mock Server**: Tests reset pointer and refetch workflow
- **Pipeline End-to-End**: Tests batch fetching and empty response handling
- **Queue Handling**: Tests failed batch queuing and retry
- **Concurrent Operations**: Tests concurrent token requests and batch fetches
- **Error Scenarios**: Tests server unavailability and 500 errors

Key test scenarios:
- Token manager authenticates with mock server
- Token manager handles invalid credentials
- Retry worker resets pointer and refetches
- Pipeline fetches batches from mock server
- Pipeline handles empty response (end of stream)
- Lag monitoring with mock server
- Retry queue integration
- Concurrent token requests
- Concurrent batch fetches
- Pipeline handles mock server unavailability
- Pipeline handles 500 errors

## Mock Server

The mock server (`test/mock_server.py`) simulates the external API with the following endpoints:

### Endpoints

- `POST /jwt_login/` - JWT authentication
  - Returns mock token for valid credentials
  - Returns 401 for invalid credentials
  - Returns 500 for server errors

- `GET /getnextbatch/?count=N` - Batch retrieval
  - Returns next batch of records based on server-side pointer
  - Returns empty response when no more records
  - Simulates stateful batch pointer

- `POST /reset_pointer/` - Batch pointer reset
  - Resets server-side batch pointer to specified batch_id
  - Used by RetryWorker for recovery

- `POST /getlag/` - Lag monitoring
  - Returns remaining records count as lag metric

- `GET /health` - Health check
  - Returns server health status

### Mock Server State

The mock server maintains in-memory state:
- `current_batch_id` - Server-side batch pointer
- `batch_records` - Sample record data
- `records_per_batch` - Records per batch (default: 2)

Call `reset_state()` to reset server state between tests.

## Test Isolation

- Unit tests use mocks to isolate logic
- Integration tests use mock server for API simulation
- Mock server state is reset between tests
- Each test is independent and can run in any order

## Known Issues

1. **Function Name Mismatch**: `monitor_lag.py` defines `monitoring_lag_forever` but `pipeline.py` calls `monitor_lag_forever`. This will cause a `NameError` at runtime and should be fixed before running integration tests.

2. **Missing Import**: `pipeline.py` does not import the lag monitoring function. Add the import before running integration tests.

## Best Practices

1. **Descriptive Test Names**: Use clear, descriptive names like `test_token_refresh_when_expired`

2. **Arrange-Act-Assert**: Structure tests with clear setup, execution, and verification phases

3. **Mock External Dependencies**: Use mocks for external services to ensure tests are fast and reliable

4. **Test Edge Cases**: Test boundary conditions, error paths, and edge cases

5. **Keep Tests Independent**: Each test should be able to run in isolation

6. **Use Fixtures**: Leverage fixtures for common test setup and teardown

## Coverage Goals

- TokenManager: >90% coverage
- RetryWorker: >90% coverage
- Pipeline: >80% coverage (integration tests)
- Overall: >85% coverage

## Continuous Integration

To run tests in CI:

```yaml
- name: Install dependencies
  run: pip install -r requirements.txt

- name: Run unit tests
  run: pytest test/unit/ -v

- name: Run integration tests
  run: pytest test/integration/ -v

- name: Generate coverage report
  run: pytest test/ --cov=src --cov=pipline --cov-report=xml
```

## Troubleshooting

### Port Already in Use

If port 8000 is already in use, modify the mock server port in `test/mock_server.py` and update the `mock_server_url` fixture in `conftest.py`.

### Import Errors

Ensure the project root is in your Python path:

```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
```

### Async Test Issues

Ensure `pytest-asyncio` is installed and configured. The `conftest.py` includes the necessary `pytest_plugins` configuration.

## Contributing

When adding new tests:

1. Add unit tests for new logic in `test/unit/`
2. Add integration tests for new workflows in `test/integration/`
3. Add shared fixtures to `conftest.py` if needed
4. Update this README with new test descriptions
5. Ensure all tests pass before committing
