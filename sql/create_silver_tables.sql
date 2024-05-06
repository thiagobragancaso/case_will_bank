CREATE TABLE minio.silver.transactions
COMMENT 'Table with all transactions including the transctions that have not been completed'
WITH (format = 'parquet')

AS

WITH cte_core_account AS (
	SELECT
		* 
	,	COUNT(*) OVER(PARTITION BY id_transaction) qty_records
	FROM minio.bronze.core_account 
),

cte_core_pix AS (
	SELECT 
		*
	,	COUNT(*) OVER(PARTITION BY id_transaction) qty_records
	FROM minio.bronze.core_pix 
)

SELECT
	ca.id_transaction
,	ca.cd_seqlan
,	ca.surrogate_key AS customer_key
,	ca.ds_transaction_type
,	ca.vl_transaction
,	ca.dt_transaction
,	ca.dt_month
,	CASE 
		WHEN cp.id_transaction IS NOT NULL THEN TRUE ELSE FALSE
	END fl_transaction_completed
FROM cte_core_account ca
LEFT JOIN cte_core_pix cp
	ON ca.id_transaction = cp.id_transaction
	AND cp.qty_records = 1 
WHERE ca.qty_records = 1
;

-- CUSTOMER

CREATE TABLE minio.silver.customer
COMMENT 'Table with all customers.'
WITH (format = 'parquet')
AS
SELECT
	surrogate_key customer_key
,	hash_customer_name
,	uf_name
,	uf
,	birth_year
,	birth_month
,	entry_date
FROM minio.bronze.customer;


-- CUSTOMER SENSITIVE
CREATE TABLE minio.silver.customer_sensitive
COMMENT 'Table with all customers (sensitive data).'
WITH (format = 'parquet')
AS
SELECT
	surrogate_key customer_key
,	full_name
,	uf_name
,	uf
,	street_name
,	birth_date
,	entry_date
FROM minio.bronze.customer_sensitive;