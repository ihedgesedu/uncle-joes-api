"""
Uncle Joe's Coffee Company — FastAPI login example

Demonstrates how to accept credentials over HTTP, hash the submitted
password with bcrypt, and construct a parameterized BigQuery query to
look up the matching member.

Setup:
    poetry install

Run:
    poetry run uvicorn main:app --reload

Then POST to http://127.0.0.1:8000/login
"""

import bcrypt
from fastapi import FastAPI, HTTPException
from google.cloud import bigquery
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional

app = FastAPI(title="Uncle Joe's Coffee API")

# Replace with your GCP project ID
GCP_PROJECT = "uncle-joes-493215"
DATASET = "uncle_joes"

client = bigquery.Client(project=GCP_PROJECT)

# CORS Middleware Setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_methods=["GET", "POST", "PATCH", "DELETE"],
    allow_headers=["*"],
)

# --- PYDANTIC MODELS FOR POST/PUT ---

class Location(BaseModel):
    id: str
    city: str
    state: str
    wifi: bool
    drive_thru: bool
    address_one: str
    zip_code: str

class MemberCreate(BaseModel):
    id: str
    first_name: str
    last_name: str
    email: str
    home_store: str
    password: str  # In a real app, you'd hash this before saving

class MenuItem(BaseModel):
    id: str
    name: str
    category: str
    size: str
    calories: int
    price: float

class Order(BaseModel):
    order_id: str
    member_id: Optional[str] = None
    store_id: str
    order_total: float

class OrderItem(BaseModel):
    id: str
    order_id: str
    menu_item_id: str
    quantity: int
    price: float

class LoginRequest(BaseModel):
    email: str
    password: str


@app.post("/login")
def login(body: LoginRequest):
    # 1. Hash the submitted password so we never handle it in plain text
    #    beyond this point.  bcrypt.hashpw produces a new hash every call
    #    (random salt), so we can't compare hashes directly — we use
    #    bcrypt.checkpw() against the stored hash retrieved from the DB.
    submitted_bytes = body.password.encode("utf-8")
    _ = bcrypt.hashpw(submitted_bytes, bcrypt.gensalt())  # shown for illustration

    # 2. Build a parameterized query to fetch the member's stored hash.
    #    Never interpolate user input directly into SQL strings.
    query = """
        SELECT id, first_name, last_name, email, password
        FROM `{project}.{dataset}.members`
        WHERE email = @email
        LIMIT 1
    """.format(project=GCP_PROJECT, dataset=DATASET)

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("email", "STRING", body.email),
        ]
    )

    results = list(client.query(query, job_config=job_config).result())

    if not results:
        raise HTTPException(status_code=401, detail="Invalid email or password.")

    row = results[0]
    stored_hash: str = row["password"]

    # 3. Verify the submitted password against the bcrypt hash from the DB.
    if not bcrypt.checkpw(submitted_bytes, stored_hash.encode("utf-8")):
        raise HTTPException(status_code=401, detail="Invalid email or password.")

    return {
        "authenticated": True,
        "member_id": row["id"],
        "name": f"{row['first_name']} {row['last_name']}",
        "email": row["email"],
    }


# Helper function to run queries and return results as a list of dicts
def run_query(query: str, params: list = []):
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    query_job = client.query(query, job_config=job_config)
    return [dict(row) for row in query_job]

# --- LOCATIONS ---

@app.get("/locations")
def get_locations():
    query = f"SELECT * FROM `{GCP_PROJECT}.{DATASET}.locations`"
    return run_query(query)

@app.get("/locations/{location_id}")
def get_location(location_id: str):
    query = f"SELECT * FROM `{GCP_PROJECT}.{DATASET}.locations` WHERE id = @id"
    params = [bigquery.ScalarQueryParameter("id", "STRING", location_id)]
    results = run_query(query, params)
    if not results:
        raise HTTPException(status_code=404, detail="Location not found")
    return results[0]

# --- MEMBERS ---

@app.get("/members")
def get_members():
    # We exclude passwords here for security best practices
    query = f"SELECT id, first_name, last_name, email, phone_number, home_store FROM `{GCP_PROJECT}.{DATASET}.members`"
    return run_query(query)

@app.get("/members/{member_id}")
def get_member(member_id: str):
    query = f"SELECT * FROM `{GCP_PROJECT}.{DATASET}.members` WHERE id = @id"
    params = [bigquery.ScalarQueryParameter("id", "STRING", member_id)]
    results = run_query(query, params)
    if not results:
        raise HTTPException(status_code=404, detail="Member not found")
    return results[0]

@app.get("/members/store/{location_id}")
def get_members_by_store(location_id: str):
    query = f"SELECT * FROM `{GCP_PROJECT}.{DATASET}.members` WHERE home_store = @store_id"
    params = [bigquery.ScalarQueryParameter("store_id", "STRING", location_id)]
    return run_query(query, params)

# --- ORDERS ---

@app.get("/orders")
def get_all_orders():
    query = f"SELECT * FROM `{GCP_PROJECT}.{DATASET}.orders` ORDER BY order_date DESC"
    return run_query(query)

@app.get("/orders/location/{location_id}")
def get_orders_by_location(location_id: str):
    query = f"SELECT * FROM `{GCP_PROJECT}.{DATASET}.orders` WHERE store_id = @store_id"
    params = [bigquery.ScalarQueryParameter("store_id", "STRING", location_id)]
    return run_query(query, params)

@app.get("/orders/member/{member_id}")
def get_orders_by_member(member_id: str):
    query = f"SELECT * FROM `{GCP_PROJECT}.{DATASET}.orders` WHERE member_id = @member_id"
    params = [bigquery.ScalarQueryParameter("member_id", "STRING", member_id)]
    return run_query(query, params)

# --- ORDER ITEMS & MENU ---

@app.get("/menu")
def get_menu():
    query = f"SELECT * FROM `{GCP_PROJECT}.{DATASET}.menu_items`"
    return run_query(query)

@app.get("/order-items/{order_id}")
def get_items_for_order(order_id: str):
    query = f"SELECT * FROM `{GCP_PROJECT}.{DATASET}.order_items` WHERE order_id = @order_id"
    params = [bigquery.ScalarQueryParameter("order_id", "STRING", order_id)]
    return run_query(query, params)

@app.get("/menu/order/{order_id}")
def get_menu_details_for_order(order_id: str):
    # This joins the order_items and menu_items tables to show what was actually bought
    query = f"""
        SELECT m.name, m.category, oi.quantity, oi.price, oi.size
        FROM `{GCP_PROJECT}.{DATASET}.order_items` AS oi
        JOIN `{GCP_PROJECT}.{DATASET}.menu_items` AS m ON oi.menu_item_id = m.id
        WHERE oi.order_id = @order_id
    """
    params = [bigquery.ScalarQueryParameter("order_id", "STRING", order_id)]
    return run_query(query, params)


# --- LOCATIONS (POST, PUT, DELETE) ---

@app.post("/locations")
def create_location(loc: Location):
    query = f"""
        INSERT INTO `{GCP_PROJECT}.{DATASET}.locations` (id, city, state, wifi, drive_thru, address_one, zip_code)
        VALUES (@id, @city, @state, @wifi, @drive_thru, @address_one, @zip_code)
    """
    params = [
        bigquery.ScalarQueryParameter("id", "STRING", loc.id),
        bigquery.ScalarQueryParameter("city", "STRING", loc.city),
        bigquery.ScalarQueryParameter("state", "STRING", loc.state),
        bigquery.ScalarQueryParameter("wifi", "BOOL", loc.wifi),
        bigquery.ScalarQueryParameter("drive_thru", "BOOL", loc.drive_thru),
        bigquery.ScalarQueryParameter("address_one", "STRING", loc.address_one),
        bigquery.ScalarQueryParameter("zip_code", "STRING", loc.zip_code),
    ]
    run_query(query, params)
    return {"message": "Location created successfully", "id": loc.id}

@app.put("/locations/{location_id}")
def update_location(location_id: str, loc: Location):
    query = f"""
        UPDATE `{GCP_PROJECT}.{DATASET}.locations`
        SET city = @city, state = @state, wifi = @wifi, drive_thru = @drive_thru
        WHERE id = @id
    """
    params = [
        bigquery.ScalarQueryParameter("id", "STRING", location_id),
        bigquery.ScalarQueryParameter("city", "STRING", loc.city),
        bigquery.ScalarQueryParameter("state", "STRING", loc.state),
        bigquery.ScalarQueryParameter("wifi", "BOOL", loc.wifi),
        bigquery.ScalarQueryParameter("drive_thru", "BOOL", loc.drive_thru),
    ]
    run_query(query, params)
    return {"message": "Location updated"}

@app.delete("/locations/{location_id}")
def delete_location(location_id: str):
    query = f"DELETE FROM `{GCP_PROJECT}.{DATASET}.locations` WHERE id = @id"
    params = [bigquery.ScalarQueryParameter("id", "STRING", location_id)]
    run_query(query, params)
    return {"message": "Location deleted"}


# --- MEMBERS (POST, PUT, DELETE) ---

@app.post("/members")
def create_member(mem: MemberCreate):
    # Note: In a real pilot, you'd use bcrypt.hashpw here before saving!
    query = f"""
        INSERT INTO `{GCP_PROJECT}.{DATASET}.members` (id, first_name, last_name, email, home_store, password)
        VALUES (@id, @first_name, @last_name, @email, @home_store, @password)
    """
    params = [
        bigquery.ScalarQueryParameter("id", "STRING", mem.id),
        bigquery.ScalarQueryParameter("first_name", "STRING", mem.first_name),
        bigquery.ScalarQueryParameter("last_name", "STRING", mem.last_name),
        bigquery.ScalarQueryParameter("email", "STRING", mem.email),
        bigquery.ScalarQueryParameter("home_store", "STRING", mem.home_store),
        bigquery.ScalarQueryParameter("password", "STRING", mem.password),
    ]
    run_query(query, params)
    return {"message": "Member created", "id": mem.id}

@app.put("/members/{member_id}")
def update_member(member_id: str, mem: MemberCreate):
    query = f"""
        UPDATE `{GCP_PROJECT}.{DATASET}.members`
        SET first_name = @first_name, 
            last_name = @last_name, 
            email = @email, 
            home_store = @home_store,
            password = @password
        WHERE id = @id
    """
    params = [
        bigquery.ScalarQueryParameter("id", "STRING", member_id),
        bigquery.ScalarQueryParameter("first_name", "STRING", mem.first_name),
        bigquery.ScalarQueryParameter("last_name", "STRING", mem.last_name),
        bigquery.ScalarQueryParameter("email", "STRING", mem.email),
        bigquery.ScalarQueryParameter("home_store", "STRING", mem.home_store),
        bigquery.ScalarQueryParameter("password", "STRING", mem.password),
    ]
    run_query(query, params)
    return {"message": "Member profile updated"}

@app.delete("/members/{member_id}")
def delete_member(member_id: str):
    query = f"DELETE FROM `{GCP_PROJECT}.{DATASET}.members` WHERE id = @id"
    params = [bigquery.ScalarQueryParameter("id", "STRING", member_id)]
    run_query(query, params)
    return {"message": "Member deleted"}


# --- MENU ITEMS (POST, PUT, DELETE) ---

@app.post("/menu")
def create_menu_item(item: MenuItem):
    query = f"""
        INSERT INTO `{GCP_PROJECT}.{DATASET}.menu_items` (id, name, category, size, calories, price)
        VALUES (@id, @name, @category, @size, @calories, @price)
    """
    params = [
        bigquery.ScalarQueryParameter("id", "STRING", item.id),
        bigquery.ScalarQueryParameter("name", "STRING", item.name),
        bigquery.ScalarQueryParameter("category", "STRING", item.category),
        bigquery.ScalarQueryParameter("size", "STRING", item.size),
        bigquery.ScalarQueryParameter("calories", "INT64", item.calories),
        bigquery.ScalarQueryParameter("price", "NUMERIC", item.price),
    ]
    run_query(query, params)
    return {"message": "Menu item added"}

@app.put("/menu/{item_id}")
def update_menu_item(item_id: str, item: MenuItem):
    query = f"""
        UPDATE `{GCP_PROJECT}.{DATASET}.menu_items`
        SET name = @name, 
            category = @category, 
            size = @size, 
            calories = @calories, 
            price = @price
        WHERE id = @id
    """
    params = [
        bigquery.ScalarQueryParameter("id", "STRING", item_id),
        bigquery.ScalarQueryParameter("name", "STRING", item.name),
        bigquery.ScalarQueryParameter("category", "STRING", item.category),
        bigquery.ScalarQueryParameter("size", "STRING", item.size),
        bigquery.ScalarQueryParameter("calories", "INT64", item.calories),
        bigquery.ScalarQueryParameter("price", "NUMERIC", item.price),
    ]
    run_query(query, params)
    return {"message": "Menu item updated"}

@app.delete("/menu/{item_id}")
def delete_menu_item(item_id: str):
    query = f"DELETE FROM `{GCP_PROJECT}.{DATASET}.menu_items` WHERE id = @id"
    params = [bigquery.ScalarQueryParameter("id", "STRING", item_id)]
    run_query(query, params)
    return {"message": "Item removed from menu"}


# --- ORDERS & ORDER ITEMS (POST, DELETE) ---

@app.post("/orders")
def create_order(order: Order):
    query = f"""
        INSERT INTO `{GCP_PROJECT}.{DATASET}.orders` (order_id, member_id, store_id, order_total, order_date)
        VALUES (@id, @member_id, @store_id, @total, CURRENT_TIMESTAMP())
    """
    params = [
        bigquery.ScalarQueryParameter("id", "STRING", order.order_id),
        bigquery.ScalarQueryParameter("member_id", "STRING", order.member_id),
        bigquery.ScalarQueryParameter("store_id", "STRING", order.store_id),
        bigquery.ScalarQueryParameter("total", "NUMERIC", order.order_total),
    ]
    run_query(query, params)
    return {"message": "Order placed", "order_id": order.order_id}

@app.post("/order-items")
def add_item_to_order(item: OrderItem):
    query = f"""
        INSERT INTO `{GCP_PROJECT}.{DATASET}.order_items` (id, order_id, menu_item_id, quantity, price)
        VALUES (@id, @order_id, @menu_id, @qty, @price)
    """
    params = [
        bigquery.ScalarQueryParameter("id", "STRING", item.id),
        bigquery.ScalarQueryParameter("order_id", "STRING", item.order_id),
        bigquery.ScalarQueryParameter("menu_id", "STRING", item.menu_item_id),
        bigquery.ScalarQueryParameter("qty", "INT64", item.quantity),
        bigquery.ScalarQueryParameter("price", "NUMERIC", item.price),
    ]
    run_query(query, params)
    return {"message": "Item added to order"}

@app.delete("/orders/{order_id}")
def cancel_order(order_id: str):
    # This deletes the order and all items associated with it
    item_query = f"DELETE FROM `{GCP_PROJECT}.{DATASET}.order_items` WHERE order_id = @id"
    order_query = f"DELETE FROM `{GCP_PROJECT}.{DATASET}.orders` WHERE order_id = @id"
    params = [bigquery.ScalarQueryParameter("id", "STRING", order_id)]
    run_query(item_query, params)
    run_query(order_query, params)
    return {"message": "Order and associated items deleted"}

# --- REWARDS / LOYALTY POINTS ---

@app.get("/members/{member_id}/rewards")
def get_member_rewards(member_id: str):
    """
    Calculates the total loyalty points for a member.
    Earns 1 point for every whole dollar spent per order, rounded down.
    """
    # This SQL query takes every order_total for the member, 
    # rounds it down to the nearest whole dollar (FLOOR), 
    # and then adds them all together (SUM).
    query = f"""
        SELECT CAST(SUM(FLOOR(order_total)) AS INT64) as total_points
        FROM `{GCP_PROJECT}.{DATASET}.orders`
        WHERE member_id = @member_id
    """
    params = [bigquery.ScalarQueryParameter("member_id", "STRING", member_id)]
    results = run_query(query, params)
    
    # If a member exists but hasn't placed any orders yet, 
    # BigQuery will return None, so we default to 0.
    points_balance = results[0].get("total_points") if results else 0
    
    return {
        "member_id": member_id,
        "points_balance": points_balance if points_balance is not None else 0
    }

