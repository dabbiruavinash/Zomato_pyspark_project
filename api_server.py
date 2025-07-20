from fastapi import FastAPI
from pyspark.sql import SparkSession
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
def startup_event():
    app.state.spark = SparkSession.builder \
        .appName("DashboardBackend") \
        .getOrCreate()

@app.get("/metrics/orders")
async def get_order_metrics():
    df = app.state.spark.sql("""
        SELECT date, COUNT(*) as order_count, SUM(amount) as total_amount
        FROM orders
        GROUP BY date
        ORDER BY date
    """)
    return df.toJSON().collect()

@app.get("/metrics/delivery")
async def get_delivery_metrics():
    df = app.state.spark.sql("""
        SELECT restaurant_id, AVG(delivery_time) as avg_delivery_time
        FROM deliveries
        GROUP BY restaurant_id
    """)
    return df.toJSON().collect()