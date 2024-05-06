-- Core Account
CREATE TABLE minio.bronze.core_account
COMMENT 'Table core_account with correct data types'
WITH (format = 'parquet')
AS
SELECT DISTINCT
	id_transaction,
	CAST(cd_seqlan AS DOUBLE) cd_seqlan,
	CAST(surrogate_key AS BIGINT) surrogate_key,
	ds_transaction_type,
	CAST(vl_transaction AS DOUBLE) vl_transaction,
	CAST(dt_transaction AS DATE) dt_transaction,
	CAST(dt_month AS INTEGER) dt_month
FROM minio.raw.core_account;

-- Core Pix
CREATE TABLE minio.bronze.core_pix
COMMENT 'Table core_pix with correct data types'
WITH (format = 'parquet')
AS
SELECT DISTINCT
	id_transaction,
	CAST(cd_seqlan AS DOUBLE) cd_seqlan,
	ds_transaction_type,
	CAST(vl_transaction AS DOUBLE) vl_transaction,
	CAST(dt_transaction AS DATE) dt_transaction,
	CAST(dt_month AS INTEGER) dt_month
FROM minio.raw.core_pix;

-- Customer Sensitive
CREATE TABLE minio.bronze.customer_sensitive
COMMENT 'Table customer with correct data types and sensitive data'
WITH (format = 'parquet')
AS
SELECT
	CAST(surrogate_key AS BIGINT) surrogate_key,
	full_name,
	uf_name,
	uf,
	street_name,
	CAST(birth_date AS DATE) birth_date,
	CAST(entry_date AS DATE) entry_date
FROM minio.raw.customer;

-- Customer
CREATE TABLE minio.bronze.customer
COMMENT 'Table customer with correct data types and data masking'
WITH (format = 'parquet')
AS
SELECT
	CAST(surrogate_key AS BIGINT) surrogate_key,
	TO_HEX(MD5(TO_UTF8(full_name))) hash_customer_name,
	uf_name,
	uf,
	CAST(YEAR(CAST(birth_date AS DATE)) AS INTEGER) birth_year,
	CAST(MONTH(CAST(birth_date AS DATE)) AS INTEGER) birth_month,
	CAST(entry_date AS DATE) entry_date
FROM minio.raw.customer;

-- Core Account Inconsistency

CREATE TABLE minio.bronze.core_account_inconsistency
COMMENT 'Table with inconsistency records from core_account'
WITH (format = 'parquet')
AS
WITH cte_core_account AS (
	SELECT 
		*
	,	COUNT(*) OVER(PARTITION BY id_transaction) qty_records
	FROM minio.bronze.core_account 
)

SELECT 
	*
,	'Id transaction is not unique' inconsistency_type
FROM cte_core_account
WHERE qty_records <> 1
;

-- Core Pix Inconsistency

CREATE TABLE minio.bronze.core_pix_inconsistency
COMMENT 'Table with inconsistency records from core_pix'
WITH (format = 'parquet')
AS
WITH cte_core_pix AS (
	SELECT 
		*
	,	COUNT(*) OVER(PARTITION BY id_transaction) qty_records
	FROM minio.bronze.core_pix 
)

SELECT
	cp.id_transaction
,	cp.cd_seqlan
,	cp.ds_transaction_type
,	cp.vl_transaction
,	cp.dt_transaction
,	cp.dt_month
,	'Transaction not found in core_account' inconsistency_type
FROM minio.bronze.core_account  ca
RIGHT JOIN cte_core_pix cp
	ON ca.id_transaction = cp.id_transaction
WHERE ca.id_transaction IS NULL

UNION ALL

SELECT
	cp.id_transaction
,	cp.cd_seqlan
,	cp.ds_transaction_type
,	cp.vl_transaction
,	cp.dt_transaction
,	cp.dt_month
,	'Id transaction is not unique' inconsistency_type
FROM cte_core_pix cp
WHERE qty_records <> 1
;

