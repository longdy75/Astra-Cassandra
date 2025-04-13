-- Dylan Long 4.11.25

from python.db_config import get_cassandra_session

-- Setup session
session = get_cassandra_session()


session.execute("""
CREATE KEYSPACE IF NOT EXISTS sales

// Load portion of ELT
CREATE TABLE IF NOT EXISTS sales.raw_sales_data (
    order_id TEXT PRIMARY KEY,
    region TEXT,
    country TEXT,
    item_type TEXT,
    sales_channel TEXT,
    order_priority TEXT,
    order_date TEXT,
    ship_date TEXT,
    units_sold TEXT,
    unit_price TEXT,
    unit_cost TEXT,
    total_revenue TEXT,
    total_cost TEXT,
    total_profit TEXT
""")