from fastapi import FastAPI, Depends, HTTPException, status
from google.cloud import bigquery
from pydantic import BaseModel

app = FastAPI()

PROJECT_ID = "hallowed-tape-489015-n2"
DATASET = "property_mgmt"


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
# Pydantic models
# ---------------------------------------------------------------------------
class Income(BaseModel):
    amount: float
    income_date: str
    category: str


class Expense(BaseModel):
    amount: float
    expense_date: str
    category: str


class Property(BaseModel):
    name: str
    address: str
    city: str
    state: str
    postal_code: str


# ---------------------------------------------------------------------------
# Properties
# ---------------------------------------------------------------------------
@app.get("/properties")
def get_properties(bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT
            property_id,
            name,
            address,
            city,
            state,
            postal_code,
            property_type,
            tenant_name,
            monthly_rent
        FROM `{PROJECT_ID}.{DATASET}.properties`
        ORDER BY property_id
    """

    try:
        results = bq.query(query).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    return [dict(row.items()) for row in results]


@app.get("/properties/{property_id}")
def get_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        results = list(bq.query(query, job_config=job_config).result())
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    if not results:
        raise HTTPException(status_code=404, detail="Property not found")

    return dict(results[0].items())


@app.post("/properties")
def create_property(property: Property, bq: bigquery.Client = Depends(get_bq_client)):
    table_id = f"{PROJECT_ID}.{DATASET}.properties"

    row = {
        "name": property.name,
        "address": property.address,
        "city": property.city,
        "state": property.state,
        "postal_code": property.postal_code
    }

    errors = bq.insert_rows_json(table_id, [row])

    if errors:
        raise HTTPException(status_code=500, detail=str(errors))

    return {"message": "Property created successfully"}


@app.put("/properties/{property_id}")
def update_property(property_id: int, property: Property, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        UPDATE `{PROJECT_ID}.{DATASET}.properties`
        SET
            name = @name,
            address = @address,
            city = @city,
            state = @state,
            postal_code = @postal_code
        WHERE property_id = @property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id),
            bigquery.ScalarQueryParameter("name", "STRING", property.name),
            bigquery.ScalarQueryParameter("address", "STRING", property.address),
            bigquery.ScalarQueryParameter("city", "STRING", property.city),
            bigquery.ScalarQueryParameter("state", "STRING", property.state),
            bigquery.ScalarQueryParameter("postal_code", "STRING", property.postal_code),
        ]
    )

    try:
        bq.query(query, job_config=job_config).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Update failed: {str(e)}"
        )

    return {"message": "Property updated successfully"}


@app.delete("/properties/{property_id}")
def delete_property(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        DELETE FROM `{PROJECT_ID}.{DATASET}.properties`
        WHERE property_id = @property_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        bq.query(query, job_config=job_config).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Delete failed: {str(e)}"
        )

    return {"message": "Property deleted successfully"}


# ---------------------------------------------------------------------------
# Income
# ---------------------------------------------------------------------------
@app.get("/income/{property_id}")
def get_income(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.income`
        WHERE property_id = @property_id
        ORDER BY income_date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        results = bq.query(query, job_config=job_config).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    return [dict(row.items()) for row in results]


@app.post("/income/{property_id}")
def add_income(property_id: int, income: Income, bq: bigquery.Client = Depends(get_bq_client)):
    table_id = f"{PROJECT_ID}.{DATASET}.income"

    row = {
        "property_id": property_id,
        "amount": income.amount,
        "income_date": income.income_date,
        "category": income.category
    }

    errors = bq.insert_rows_json(table_id, [row])

    if errors:
        raise HTTPException(status_code=500, detail=str(errors))

    return {"message": "Income added successfully"}


# ---------------------------------------------------------------------------
# Expenses
# ---------------------------------------------------------------------------
@app.get("/expenses/{property_id}")
def get_expenses(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT *
        FROM `{PROJECT_ID}.{DATASET}.expenses`
        WHERE property_id = @property_id
        ORDER BY expense_date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        results = bq.query(query, job_config=job_config).result()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database query failed: {str(e)}"
        )

    return [dict(row.items()) for row in results]


@app.post("/expenses/{property_id}")
def add_expense(property_id: int, expense: Expense, bq: bigquery.Client = Depends(get_bq_client)):
    table_id = f"{PROJECT_ID}.{DATASET}.expenses"

    row = {
        "property_id": property_id,
        "amount": expense.amount,
        "expense_date": expense.expense_date,
        "category": expense.category
    }

    errors = bq.insert_rows_json(table_id, [row])

    if errors:
        raise HTTPException(status_code=500, detail=str(errors))

    return {"message": "Expense added successfully"}


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
@app.get("/summary/{property_id}")
def get_summary(property_id: int, bq: bigquery.Client = Depends(get_bq_client)):
    query = f"""
        SELECT
            (SELECT IFNULL(SUM(amount), 0)
             FROM `{PROJECT_ID}.{DATASET}.income`
             WHERE property_id = @property_id) AS total_income,
            (SELECT IFNULL(SUM(amount), 0)
             FROM `{PROJECT_ID}.{DATASET}.expenses`
             WHERE property_id = @property_id) AS total_expenses
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("property_id", "INT64", property_id)
        ]
    )

    try:
        result = list(bq.query(query, job_config=job_config).result())[0]
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Summary query failed: {str(e)}"
        )

    return {
        "total_income": result["total_income"],
        "total_expenses": result["total_expenses"],
        "net_profit": result["total_income"] - result["total_expenses"]
    }