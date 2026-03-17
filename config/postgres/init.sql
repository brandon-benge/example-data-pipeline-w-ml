CREATE TABLE IF NOT EXISTS sales_rep (
    sales_rep_id BIGINT PRIMARY KEY,
    rep_name VARCHAR(255) NOT NULL,
    team_name VARCHAR(100),
    region VARCHAR(50),
    manager_name VARCHAR(255),
    status VARCHAR(30) NOT NULL DEFAULT 'active',
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS customer (
    customer_id BIGINT PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) NOT NULL,
    phone VARCHAR(50),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    status VARCHAR(30) NOT NULL DEFAULT 'active',
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS advertiser (
    advertiser_id BIGINT PRIMARY KEY,
    advertiser_name VARCHAR(255) NOT NULL,
    industry VARCHAR(100),
    account_tier VARCHAR(50),
    region VARCHAR(50),
    owner_sales_rep_id BIGINT,
    status VARCHAR(30) NOT NULL DEFAULT 'active',
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    CONSTRAINT fk_advertiser_sales_rep
        FOREIGN KEY (owner_sales_rep_id) REFERENCES sales_rep(sales_rep_id)
);

CREATE TABLE IF NOT EXISTS product (
    product_id BIGINT PRIMARY KEY,
    sku VARCHAR(100) NOT NULL UNIQUE,
    product_name VARCHAR(255) NOT NULL,
    brand VARCHAR(100),
    category VARCHAR(100) NOT NULL,
    subcategory VARCHAR(100),
    list_price NUMERIC(12,2) NOT NULL,
    cost NUMERIC(12,2),
    active_flag BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS campaign (
    campaign_id BIGINT PRIMARY KEY,
    advertiser_id BIGINT NOT NULL,
    campaign_name VARCHAR(255) NOT NULL,
    campaign_type VARCHAR(50) NOT NULL,
    objective VARCHAR(100) NOT NULL,
    budget_amount NUMERIC(14,2) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    status VARCHAR(30) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    CONSTRAINT fk_campaign_advertiser
        FOREIGN KEY (advertiser_id) REFERENCES advertiser(advertiser_id)
);

CREATE TABLE IF NOT EXISTS campaign_product (
    campaign_product_id BIGINT PRIMARY KEY,
    campaign_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    bid_amount NUMERIC(10,2) NOT NULL,
    priority INTEGER,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    CONSTRAINT fk_campaign_product_campaign
        FOREIGN KEY (campaign_id) REFERENCES campaign(campaign_id),
    CONSTRAINT fk_campaign_product_product
        FOREIGN KEY (product_id) REFERENCES product(product_id)
);

CREATE TABLE IF NOT EXISTS customer_session (
    session_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    session_start_ts TIMESTAMP NOT NULL,
    session_end_ts TIMESTAMP,
    device_type VARCHAR(50),
    channel VARCHAR(50),
    referrer_type VARCHAR(50),
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    CONSTRAINT fk_customer_session_customer
        FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
);

CREATE TABLE IF NOT EXISTS order_header (
    order_id BIGINT PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    order_ts TIMESTAMP NOT NULL,
    order_status VARCHAR(30) NOT NULL,
    subtotal_amount NUMERIC(12,2) NOT NULL,
    discount_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
    tax_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
    total_amount NUMERIC(12,2) NOT NULL,
    payment_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    CONSTRAINT fk_order_header_customer
        FOREIGN KEY (customer_id) REFERENCES customer(customer_id)
);

CREATE TABLE IF NOT EXISTS order_item (
    order_item_id BIGINT PRIMARY KEY,
    order_id BIGINT NOT NULL,
    product_id BIGINT NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(12,2) NOT NULL,
    line_amount NUMERIC(12,2) NOT NULL,
    attributed_campaign_id BIGINT,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    CONSTRAINT fk_order_item_order
        FOREIGN KEY (order_id) REFERENCES order_header(order_id),
    CONSTRAINT fk_order_item_product
        FOREIGN KEY (product_id) REFERENCES product(product_id)
);

CREATE TABLE IF NOT EXISTS sales_activity (
    sales_activity_id BIGINT PRIMARY KEY,
    advertiser_id BIGINT NOT NULL,
    sales_rep_id BIGINT NOT NULL,
    activity_ts TIMESTAMP NOT NULL,
    activity_type VARCHAR(50) NOT NULL,
    activity_outcome VARCHAR(50),
    notes TEXT,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    CONSTRAINT fk_sales_activity_advertiser
        FOREIGN KEY (advertiser_id) REFERENCES advertiser(advertiser_id),
    CONSTRAINT fk_sales_activity_sales_rep
        FOREIGN KEY (sales_rep_id) REFERENCES sales_rep(sales_rep_id)
);
