"""
Mock FastAPI server for testing the Resilient-Async-Ingestor pipeline.
Simulates external API endpoints: /jwt_login/, /getnextbatch/, /getlag/, /reset_pointer.
"""
# endregion Import Modules...
import time
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional, List, Dict, Any
# endregion


app = FastAPI(title="Mock API Server")


# region Data Models
class LoginRequest(BaseModel):
    username: str
    password: str


class ResetPointerRequest(BaseModel):
    function: str
    batch_id: int


class LagRequest(BaseModel):
    function: str


# endregion


# region In-Memory State
# Simulate server-side batch pointer state
current_batch_id = 1
batch_records = [
    {"id": 1, "name": "Record 1", "value": 100},
    {"id": 2, "name": "Record 2", "value": 200},
    {"id": 3, "name": "Record 3", "value": 300},
    {"id": 4, "name": "Record 4", "value": 400},
    {"id": 5, "name": "Record 5", "value": 500},
]
total_records = len(batch_records)
records_per_batch = 2


def reset_state():
    """Reset server state for test isolation."""
    global current_batch_id
    current_batch_id = 1


# endregion


# region Endpoints
@app.post("/jwt_login/")
async def jwt_login(request: LoginRequest):
    """
    Simulate JWT authentication endpoint.
    Returns a mock access token.
    """
    if request.username == "test_user" and request.password == "test_password":
        return {"access": "mock.jwt.token"}
    elif request.username == "invalid_user":
        raise HTTPException(status_code=401, detail="Invalid credentials")
    else:
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/getnextbatch/")
async def get_next_batch(count: int = 2):
    """
    Simulate batch retrieval endpoint.
    Returns next batch of records based on server-side batch pointer.
    """
    global current_batch_id
    
    # Simulate empty response when no more records
    if current_batch_id > (total_records // records_per_batch) + 1:
        return {"batch_id": None, "records": []}
    
    # Calculate record indices
    start_idx = (current_batch_id - 1) * records_per_batch
    end_idx = min(start_idx + count, total_records)
    
    if start_idx >= total_records:
        return {"batch_id": None, "records": []}
    
    records = batch_records[start_idx:end_idx]
    batch_id = current_batch_id
    current_batch_id += 1
    
    return {"batch_id": batch_id, "records": records}


@app.post("/reset_pointer/")
async def reset_pointer(request: ResetPointerRequest):
    """
    Simulate batch pointer reset endpoint.
    Resets server-side batch pointer to specified batch_id.
    """
    global current_batch_id
    
    if request.function == "getnextbatch":
        current_batch_id = request.batch_id
        return {"status": "success", "reset_to": request.batch_id}
    else:
        raise HTTPException(status_code=400, detail="Invalid function")


@app.post("/getlag/")
async def get_lag(request: LagRequest):
    """
    Simulate lag monitoring endpoint.
    Returns remaining records count as lag metric.
    """
    global current_batch_id
    
    remaining_batches = (total_records // records_per_batch) + 1 - current_batch_id
    remaining_records = max(0, remaining_batches * records_per_batch)
    
    return {"lag": remaining_records}


@app.get("/health")
async def health_check():
    """Health check endpoint for testing server availability."""
    return {"status": "healthy"}


# endregion


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
