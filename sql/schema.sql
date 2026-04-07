-- ============================================================
-- E-Commerce Returns Analytics -- Database Schema
-- Compatible with: PostgreSQL, SQLite, MySQL
-- ============================================================

DROP TABLE IF EXISTS returns;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS customers;

CREATE TABLE products (
    product_id    VARCHAR(20)    PRIMARY KEY,
    product_name  VARCHAR(100)   NOT NULL,
    category      VARCHAR(50)    NOT NULL,
    unit_price    DECIMAL(10, 2) NOT NULL
);

CREATE TABLE customers (
    customer_id   VARCHAR(20)  PRIMARY KEY,
    customer_name VARCHAR(100) NOT NULL,
    region        VARCHAR(30)  NOT NULL,
    customer_tier VARCHAR(20)  NOT NULL  -- Bronze, Silver, Gold, Platinum
);

CREATE TABLE returns (
    return_id               VARCHAR(20)    PRIMARY KEY,
    order_id                VARCHAR(20)    NOT NULL,
    customer_id             VARCHAR(20)    NOT NULL REFERENCES customers(customer_id),
    product_id              VARCHAR(20)    NOT NULL REFERENCES products(product_id),
    category                VARCHAR(50)    NOT NULL,
    region                  VARCHAR(30)    NOT NULL,
    return_reason           VARCHAR(60)    NOT NULL,
    resolution_type         VARCHAR(30)    NOT NULL,
    return_date             DATE           NOT NULL,
    resolution_date         DATE           NOT NULL,
    resolution_days         INT            NOT NULL,
    product_price           DECIMAL(10,2)  NOT NULL,
    return_cost             DECIMAL(10,2)  NOT NULL,
    customer_satisfaction   SMALLINT       NOT NULL CHECK (customer_satisfaction BETWEEN 1 AND 5),
    customer_tier           VARCHAR(20)    NOT NULL,
    quarter                 VARCHAR(5)     NOT NULL,
    month                   VARCHAR(10)    NOT NULL,
    year                    SMALLINT       NOT NULL
);

CREATE INDEX idx_returns_date       ON returns(return_date);
CREATE INDEX idx_returns_category   ON returns(category);
CREATE INDEX idx_returns_region     ON returns(region);
CREATE INDEX idx_returns_reason     ON returns(return_reason);
CREATE INDEX idx_returns_resolution ON returns(resolution_type);
CREATE INDEX idx_returns_year_month ON returns(year, month);
CREATE INDEX idx_returns_customer   ON returns(customer_id);
CREATE INDEX idx_returns_product    ON returns(product_id);
