-- Let event-time windows advance even when a source subtask has no assigned
-- Kafka partitions or a quiet partition.
SET 'table.exec.source.idle-timeout' = '5 s';

-- ─── SOURCE TABLES ───

CREATE TABLE clickstream (
    event_id VARCHAR,
    userid INT,
    session_id VARCHAR,
    event_type VARCHAR,
    productid VARCHAR,
    category VARCHAR,
    query VARCHAR,
    ts BIGINT,
    event_time AS TO_TIMESTAMP_LTZ(ts, 3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'shoe-clickstream',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-clickstream-group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);

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

CREATE TABLE product_metadata (
    productid VARCHAR,
    name VARCHAR,
    avg_rating DOUBLE,
    review_count INT,
    updated_at BIGINT,
    event_time AS PROCTIME()
) WITH (
    'connector' = 'kafka',
    'topic' = 'product-metadata',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink-metadata-group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);

-- ─── SINK TABLES ───

CREATE TABLE live_user_profile (
    userid INT,
    recent_page_views BIGINT,
    recent_searches BIGINT,
    recent_cart_adds BIGINT,
    active_interest_category VARCHAR,
    active_interest_events BIGINT,
    intent_window_minutes INT,
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

CREATE TABLE live_product_profile (
    productid VARCHAR,
    name VARCHAR,
    brand VARCHAR,
    category VARCHAR,
    price DOUBLE,
    sale_price DOUBLE,
    on_sale BOOLEAN,
    stock INT,
    total_orders BIGINT,
    demand_score DOUBLE,
    stock_trend VARCHAR,
    avg_rating DOUBLE,
    updated_at VARCHAR,
    PRIMARY KEY (productid) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'live-product-profile',
    'properties.bootstrap.servers' = 'kafka:29092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- ─── VIEWS ───

CREATE VIEW user_pageviews AS
SELECT
    userid,
    window_end,
    COUNT(*) AS recent_page_views
FROM TABLE(
    HOP(
        TABLE clickstream,
        DESCRIPTOR(event_time),
        INTERVAL '1' MINUTE,
        INTERVAL '15' MINUTE
    )
)
WHERE event_type = 'product_view'
GROUP BY userid, window_start, window_end;

CREATE VIEW user_intent_totals AS
SELECT
    userid,
    window_end,
    SUM(CASE WHEN event_type = 'search' THEN 1 ELSE 0 END) AS recent_searches,
    SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END) AS recent_cart_adds
FROM TABLE(
    HOP(
        TABLE clickstream,
        DESCRIPTOR(event_time),
        INTERVAL '1' MINUTE,
        INTERVAL '15' MINUTE
    )
)
GROUP BY userid, window_start, window_end;

CREATE VIEW user_category_interest AS
SELECT
    userid,
    window_end,
    category,
    COUNT(*) AS active_interest_events
FROM TABLE(
    HOP(
        TABLE clickstream,
        DESCRIPTOR(event_time),
        INTERVAL '1' MINUTE,
        INTERVAL '15' MINUTE
    )
)
WHERE category IS NOT NULL
GROUP BY userid, window_start, window_end, category;

CREATE VIEW ranked_user_category_interest AS
SELECT
    userid,
    window_end,
    category,
    active_interest_events,
    ROW_NUMBER() OVER (
        PARTITION BY userid, window_end
        ORDER BY active_interest_events DESC, category ASC
    ) AS category_rank
FROM user_category_interest;

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

CREATE VIEW product_orders AS
SELECT
    productid,
    COUNT(*) AS total_orders,
    CASE
        WHEN COUNT(*) > 10 THEN 'high'
        WHEN COUNT(*) > 5  THEN 'medium'
        ELSE 'low'
    END AS demand_score
FROM cart_updates
GROUP BY productid;

-- ─── FLINK JOBS ───

INSERT INTO live_user_profile
SELECT
    t.userid,
    COALESCE(p.recent_page_views, 0) AS recent_page_views,
    COALESCE(t.recent_searches, 0) AS recent_searches,
    COALESCE(t.recent_cart_adds, 0) AS recent_cart_adds,
    COALESCE(c.category, 'unknown') AS active_interest_category,
    COALESCE(c.active_interest_events, 0) AS active_interest_events,
    15 AS intent_window_minutes,
    COALESCE(o.total_orders, 0) AS total_orders,
    COALESCE(o.total_purchases, 0) AS total_purchases,
    COALESCE(o.total_returns, 0) AS total_returns,
    COALESCE(o.avg_order_price, 0.0) AS avg_order_price,
    COALESCE(o.price_sensitivity, 'unknown') AS price_sensitivity,
    CAST(CURRENT_TIMESTAMP AS VARCHAR) AS updated_at
FROM user_intent_totals t
LEFT JOIN user_orders o ON t.userid = o.userid
LEFT JOIN user_pageviews p
    ON t.userid = p.userid AND t.window_end = p.window_end
LEFT JOIN ranked_user_category_interest c
    ON t.userid = c.userid
    AND t.window_end = c.window_end
    AND c.category_rank = 1;

INSERT INTO live_product_profile
SELECT
    i.productid,
    i.name,
    i.brand,
    i.category,
    i.price,
    i.sale_price,
    i.on_sale,
    i.stock,
    COALESCE(o.total_orders, 0) AS total_orders,
    CAST(COALESCE(o.total_orders, 0) AS DOUBLE) / 100.0 AS demand_score,
    CASE
        WHEN i.stock < 20 THEN 'low'
        WHEN i.stock < 50 THEN 'medium'
        ELSE 'high'
    END AS stock_trend,
    COALESCE(m.avg_rating, 0.0) AS avg_rating,
    CAST(CURRENT_TIMESTAMP AS VARCHAR) AS updated_at
FROM inventory i
LEFT JOIN product_orders o ON i.productid = o.productid
LEFT JOIN product_metadata m ON i.productid = m.productid;
