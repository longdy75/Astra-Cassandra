-- Dylan Long 4.11.25

-- Create tables for aggregation

CREATE TABLE IF NOT EXISTS sales.avg_profit_by_item_type (
    item_type TEXT PRIMARY KEY,
    avg_profit DECIMAL,
    total_units_sold INT
);


CREATE TABLE IF NOT EXISTS sales.revenue_profit_by_region_month (
    region TEXT,
    month TEXT, 
    total_revenue DECIMAL,
    total_profit DECIMAL,
    total_units_sold INT,
    PRIMARY KEY ((region), month)
);