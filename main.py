from sqlite3 import DatabaseError
from fastapi import FastAPI, Depends, HTTPException, status
from google.cloud import bigquery
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional

app = FastAPI()

PROJECT_ID = "uncle-joes-493215"
DATASET = "uncle_joes"

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],       # accept requests from any origin
    allow_methods=["GET", "POST", "PATCH", "DELETE"],
    allow_headers=["*"],       # accept any request headers
)

class TemplateRequest(BaseModel):
    template_id: int
    amount: float
    date: str
    description: str


# ---------------------------------------------------------------------------
# Dependency: BigQuery client
# ---------------------------------------------------------------------------

def get_bq_client():
    client = bigquery.Client(project=PROJECT_ID)
    try:
        yield client
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------

@app.get("/template")
def get_template(bq: bigquery.Client = Depends(get_bq_client)):
    """
    Returns all properties in the database.
    """
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.table`
        ORDER BY id
    """

    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    result = [dict(row) for row in results]
    return result