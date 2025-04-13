-- Dylan Long 4.12.25
CREATE TABLE IF NOT EXISTS sales.typed_data (
    order_id INT PRIMARY KEY,
    region TEXT,
    country TEXT,
    item_type TEXT,
    sales_channel TEXT,
    order_priority TEXT,
    order_date DATE,
    ship_date DATE,
    units_sold INT,
    unit_price DECIMAL,
    unit_cost DECIMAL,
    total_revenue DECIMAL,
    total_cost DECIMAL,
    total_profit DECIMAL
)