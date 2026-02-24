-- =============================================================================
-- REDSHIFT DDL — FULL CREATE (drop and recreate)
-- Run this once to set up the entire DW from scratch.
-- =============================================================================

-- ─── SCHEMAS ──────────────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS public;


-- =============================================================================
-- STAGING TABLES
-- Overwritten on every Glue gold job run.
-- No constraints, no sort/dist keys — raw landing zone.
-- =============================================================================

DROP TABLE IF EXISTS staging.stg_dim_date;
CREATE TABLE staging.stg_dim_date (
    date_key            INTEGER,
    full_date           DATE,
    day_of_week         INTEGER,
    day_of_week_name    VARCHAR(10),
    day_of_month        INTEGER,
    day_of_year         INTEGER,
    week_of_year        INTEGER,
    month_number        INTEGER,
    month_name          VARCHAR(10),
    quarter             INTEGER,
    year                INTEGER,
    is_weekend          BOOLEAN,
    is_holiday          BOOLEAN,
    fiscal_year         INTEGER,
    fiscal_quarter      INTEGER
);

DROP TABLE IF EXISTS staging.stg_merchant;
CREATE TABLE staging.stg_merchant (
    cdc_op                      VARCHAR(1),
    cdc_timestamp           TIMESTAMP,
    merchant_id             BIGINT,
    merchant_name           VARCHAR(200),
    legal_entity_name       VARCHAR(200),
    tax_id                  VARCHAR(50),
    business_category_code  INTEGER,
    merchant_tier_code      INTEGER,
    onboarding_status_code  INTEGER,
    region                  VARCHAR(50),
    city                    VARCHAR(100),
    state                   VARCHAR(50),
    country                 VARCHAR(50),
    postal_code             VARCHAR(20),
    timezone                VARCHAR(50),
    commission_rate         NUMERIC(5,2),
    monthly_fee             NUMERIC(10,2),
    onboarding_date         INTEGER,
    contract_start_date     INTEGER,
    contract_end_date       INTEGER,
    valid_from              DATE,
    valid_to                DATE,
    is_active               BOOLEAN,
    created_at              TIMESTAMP,
    updated_at              TIMESTAMP
);

DROP TABLE IF EXISTS staging.stg_fact_transactions;
CREATE TABLE staging.stg_fact_transactions (
    transaction_id          VARCHAR(100),
    merchant_key            BIGINT,
    customer_id             BIGINT,
    payment_junk_key        INTEGER,
    card_device_junk_key    INTEGER,
    transaction_timestamp   TIMESTAMP,
    transaction_date_key    INTEGER,
    processed_timestamp     TIMESTAMP,
    processed_date_key      INTEGER,
    gateway_timestamp       TIMESTAMP,
    gateway_date_key        INTEGER,
    gross_amount            NUMERIC(12,2),
    discount_amount         NUMERIC(12,2),
    tax_amount              NUMERIC(12,2),
    net_amount              NUMERIC(12,2),
    refund_amount           NUMERIC(12,2),
    emi_enabled             BOOLEAN,
    emi_tenure_months       INTEGER,
    emi_interest_rate       NUMERIC(5,2),
    ip_address              VARCHAR(45),
    payment_latitude        NUMERIC(10,8),
    payment_longitude       NUMERIC(11,8),
    processing_time_ms      INTEGER,
    gateway_fee             NUMERIC(10,2),
    gateway_status          VARCHAR(50),
    gateway_retry_count     INTEGER,
    discount_applied        BOOLEAN,
    created_at              TIMESTAMP,
    year                    INTEGER,
    month                   INTEGER
);


-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

DROP TABLE IF EXISTS public.dim_date;
CREATE TABLE public.dim_date (
    date_key            INTEGER         NOT NULL ENCODE az64,
    full_date           DATE            NOT NULL ENCODE az64,
    day_of_week         INTEGER         NOT NULL ENCODE az64,
    day_of_week_name    VARCHAR(10)     NOT NULL ENCODE lzo,
    day_of_month        INTEGER         NOT NULL ENCODE az64,
    day_of_year         INTEGER         NOT NULL ENCODE az64,
    week_of_year        INTEGER         NOT NULL ENCODE az64,
    month_number        INTEGER         NOT NULL ENCODE az64,
    month_name          VARCHAR(10)     NOT NULL ENCODE lzo,
    quarter             INTEGER         NOT NULL ENCODE az64,
    year                INTEGER         NOT NULL ENCODE az64,
    is_weekend          BOOLEAN         NOT NULL,
    is_holiday          BOOLEAN         NOT NULL,
    fiscal_year         INTEGER                  ENCODE az64,
    fiscal_quarter      INTEGER                  ENCODE az64,
    PRIMARY KEY (date_key)
)
DISTSTYLE ALL
SORTKEY (date_key);


DROP TABLE IF EXISTS public.dim_merchant;
CREATE TABLE public.dim_merchant (
    merchant_key            BIGINT          IDENTITY(1,1),
    merchant_id             BIGINT          NOT NULL ENCODE az64,
    merchant_name           VARCHAR(200)             ENCODE lzo,
    legal_entity_name       VARCHAR(200)             ENCODE lzo,
    tax_id                  VARCHAR(50)              ENCODE lzo,
    business_category_code  INTEGER                  ENCODE az64,
    merchant_tier_code      INTEGER                  ENCODE az64,
    onboarding_status_code  INTEGER                  ENCODE az64,
    region                  VARCHAR(50)              ENCODE lzo,
    city                    VARCHAR(100)             ENCODE lzo,
    state                   VARCHAR(50)              ENCODE lzo,
    country                 VARCHAR(50)              ENCODE lzo,
    postal_code             VARCHAR(20)              ENCODE lzo,
    timezone                VARCHAR(50)              ENCODE lzo,
    commission_rate         NUMERIC(5,2)             ENCODE az64,
    monthly_fee             NUMERIC(10,2)            ENCODE az64,
    onboarding_date         INTEGER                     ENCODE az64,
    contract_start_date     INTEGER                     ENCODE az64,
    contract_end_date       INTEGER                     ENCODE az64,
    valid_from              DATE            NOT NULL,
    valid_to                DATE            NOT NULL ENCODE az64,
    is_active               BOOLEAN         NOT NULL,
    created_at              TIMESTAMP       NOT NULL ENCODE az64,
    updated_at              TIMESTAMP       NOT NULL ENCODE az64,
    PRIMARY KEY (merchant_key)
)
DISTSTYLE ALL
SORTKEY (merchant_key, valid_from);





-- =============================================================================
-- FACT TABLE
-- =============================================================================

DROP TABLE IF EXISTS public.fact_transactions;
CREATE TABLE public.fact_transactions (
    transaction_key         BIGINT          IDENTITY(1,1) ENCODE az64,
    transaction_id          VARCHAR(100)    NOT NULL ENCODE lzo,
    merchant_key            BIGINT          NOT NULL,
    customer_id             BIGINT                   ENCODE az64,
    payment_junk_key        INTEGER         NOT NULL ENCODE az64,
    card_device_junk_key    INTEGER         NOT NULL ENCODE az64,
    transaction_timestamp   TIMESTAMP       NOT NULL ENCODE az64,
    transaction_date_key    INTEGER         NOT NULL ENCODE az64,
    processed_timestamp     TIMESTAMP                ENCODE az64,
    processed_date_key      INTEGER                  ENCODE az64,
    gateway_timestamp       TIMESTAMP                ENCODE az64,
    gateway_date_key        INTEGER                  ENCODE az64,
    gross_amount            NUMERIC(12,2)            ENCODE az64,
    discount_amount         NUMERIC(12,2)            ENCODE az64,
    tax_amount              NUMERIC(12,2)            ENCODE az64,
    net_amount              NUMERIC(12,2)            ENCODE az64,
    refund_amount           NUMERIC(12,2)            ENCODE az64,
    emi_enabled             BOOLEAN,
    emi_tenure_months       INTEGER                  ENCODE az64,
    emi_interest_rate       NUMERIC(5,2)             ENCODE az64,
    ip_address              VARCHAR(45)              ENCODE lzo,
    payment_latitude        NUMERIC(10,8)            ENCODE az64,
    payment_longitude       NUMERIC(11,8)            ENCODE az64,
    processing_time_ms      INTEGER                  ENCODE az64,
    gateway_fee             NUMERIC(10,2)            ENCODE az64,
    gateway_status          VARCHAR(50)              ENCODE lzo,
    gateway_retry_count     INTEGER                  ENCODE az64,
    discount_applied        BOOLEAN,
    created_at              TIMESTAMP       NOT NULL ENCODE az64,
    year                    INTEGER         NOT NULL ENCODE az64,
    month                   INTEGER         NOT NULL ENCODE az64,
    PRIMARY KEY (transaction_key)
)
DISTKEY (merchant_key)
SORTKEY (transaction_date_key, merchant_key);






-- DROP TABLE IF EXISTS public.junk_dim_payment_context;
-- CREATE TABLE public.junk_dim_payment_context (
--     payment_junk_key        INTEGER         NOT NULL,
--     payment_method_code     INTEGER                  ENCODE az64,
--     payment_method_desc     VARCHAR(50)              ENCODE lzo,
--     transaction_status_code INTEGER                  ENCODE az64,
--     transaction_status_desc VARCHAR(50)              ENCODE lzo,
--     currency_code           INTEGER                  ENCODE az64,
--     currency_name           VARCHAR(20)              ENCODE lzo,
--     is_online_flag          BOOLEAN,
--     gateway_name            VARCHAR(50)              ENCODE lzo,
--     PRIMARY KEY (payment_junk_key)
-- )
-- DISTSTYLE ALL
-- SORTKEY (payment_junk_key);

-- DROP TABLE IF EXISTS public.junk_dim_card_device;
-- CREATE TABLE public.junk_dim_card_device (
--     card_device_junk_key    INTEGER         NOT NULL,
--     card_type               VARCHAR(20)              ENCODE lzo,
--     card_brand_name         VARCHAR(50)              ENCODE lzo,
--     device_type_desc        VARCHAR(50)              ENCODE lzo,
--     is_mac                  BOOLEAN,
--     PRIMARY KEY (card_device_junk_key)
-- )
-- DISTSTYLE ALL
-- SORTKEY (card_device_junk_key);