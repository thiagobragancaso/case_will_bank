
-- Date Dimension
CREATE TABLE minio.gold.dim_date
COMMENT 'Date Dimension Table'
WITH (format = 'parquet')
AS
SELECT
	CAST(DATE_FORMAT("date", '%Y%m%d') AS BIGINT) sk_date
,	"date"
,	YEAR("date") "year"
,	QUARTER("date") "quarter"
,	MONTH("date") "month"
,	DATE_FORMAT("date", '%M') month_name
,	WEEK("date") "week"
,	DAY("date") "day"
,	DAY_OF_WEEK("date") "day_of_week"
,	DATE_FORMAT("date", '%W') day_name
,	CAST(DATE_FORMAT("date", '%Y%m') AS INTEGER) month_year
FROM UNNEST(SEQUENCE(DATE '2023-01-01', CURRENT_DATE, INTERVAL '1' DAY)) t ("date");


-- Customer Dimension
CREATE TABLE minio.gold.dim_customer
COMMENT 'Customer Dimension Table'
WITH (format = 'parquet')
AS
SELECT
	customer_key AS sk_customer
,	full_name AS customer_name
,	uf_name AS state
,	uf
,	street_name
,	birth_date
,	entry_date
FROM minio.silver.customer;

-- Transaction Fact
CREATE TABLE minio.gold.ft_transaction
COMMENT 'Transaction Fact Table'
WITH (format = 'parquet')
AS
SELECT
	id_transaction
,	customer_key sk_customer
,	dim.sk_date
,	cd_seqlan
,	ds_transaction_type
,	vl_transaction
,	fl_transaction_completed
FROM minio.silver.transactions t
LEFT JOIN minio.gold.dim_date dim
	ON t.dt_transaction = dim."date"
;

-- Total transaction value per month
CREATE TABLE minio.gold.value_per_month
COMMENT 'Table with total transaction value per month'
WITH (format = 'parquet')
AS
SELECT
	dim."year"
,	dim.month_name
,	dim."month"
,	ROUND(SUM(vl_transaction), 2) total_vl_transaction
FROM minio.gold.ft_transaction fct
INNER JOIN minio.gold.dim_date dim
	ON fct.sk_date = dim.sk_date
GROUP BY 1, 2, 3
;

-- Top customer
CREATE TABLE minio.gold.value_per_customer
COMMENT 'Table with total transaction value per customer'
WITH (format = 'parquet')
AS
SELECT
	dim.customer_name
,	ROUND(SUM(vl_transaction), 2) total_value_transaction
FROM minio.gold.ft_transaction fct
INNER JOIN minio.gold.dim_customer dim
	ON fct.sk_customer = dim.sk_customer
GROUP BY 1; 
