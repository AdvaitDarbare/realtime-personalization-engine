-- ─── SOURCE TABLES ───

-- Clickstream source
CREATE TABLE clickstream (
    ip VARCHAR,
    userid INT,
    request VARCHAR,
    status VARCHAR,
    bytes VARCHAR,
    agent VARCHAR,
    event_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'clickstream',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-clickstream-group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);

-- Cart updates source
CREATE TABLE cart_updates (
    order_id INT,
    userid INT,
    productid VARCHAR,
    price DOUBLE,
    action VARCHAR,
    ts BIGINT,
    event_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'cart-updates',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-cart-group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);

-- Inventory source
CREATE TABLE inventory (
    productid VARCHAR,
    name VARCHAR,
    brand VARCHAR,
    category VARCHAR,
    price DOUBLE,
    sale_price DOUBLE,
    on_sale BOOLEAN,
    stock INT,
    updated_at BIGINT,
    event_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'inventory',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-inventory-group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);

-- ─── SINK TABLE ───
CREATE TABLE live_user_profile (
    userid INT,
    page_views BIGINT,
    total_orders BIGINT,
    total_purchases BIGINT,
    total_returns BIGINT,
    avg_order_price DOUBLE,
    price_sensitivity VARCHAR,
    updated_at VARCHAR,
    PRIMARY KEY (userid) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'live-user-profile',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- ─── FLINK JOB 1: Page views per user from clickstream ───
CREATE VIEW user_pageviews AS
SELECT
    userid,
    COUNT(*) AS page_views
FROM clickstream
GROUP BY userid;

-- ─── FLINK JOB 2: Order stats per user from cart_updates ───
CREATE VIEW user_orders AS
SELECT
    userid,
    COUNT(*) AS total_orders,
    SUM(CASE WHEN action = 'purchase' THEN 1 ELSE 0 END) AS total_purchases,
    SUM(CASE WHEN action = 'return' THEN 1 ELSE 0 END) AS total_returns,
    AVG(price) AS avg_order_price,
    CASE
        WHEN AVG(price) < 80  THEN 'high'
        WHEN AVG(price) < 120 THEN 'medium'
        ELSE 'low'
    END AS price_sensitivity
FROM cart_updates
GROUP BY userid;

-- ─── FLINK JOB 3: Join and write to live_user_profile ───
INSERT INTO live_user_profile
SELECT
    o.userid,
    COALESCE(p.page_views, 0) AS page_views,
    o.total_orders,
    o.total_purchases,
    o.total_returns,
    o.avg_order_price,
    o.price_sensitivity,
    CAST(CURRENT_TIMESTAMP AS VARCHAR) AS updated_at
FROM user_orders o
LEFT JOIN user_pageviews p ON o.userid = p.userid;