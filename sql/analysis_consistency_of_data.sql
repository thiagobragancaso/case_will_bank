-- Verifica se existem divergência no valor dos campos entre core_account e core_pix
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

SELECT *
FROM cte_core_account ca
LEFT JOIN cte_core_pix cp
	ON ca.id_transaction = cp.id_transaction
	AND cp.qty_records = 1
WHERE ca.qty_records = 1
AND (
	ca.vl_transaction <> cp.vl_transaction
	OR ca.dt_transaction <> cp.dt_transaction
	OR ca.cd_seqlan <> cp.cd_seqlan
	OR ca.dt_month <> cp.dt_month
	OR ca.ds_transaction_type <> cp.ds_transaction_type
);

-- Verifica se os valores do campo dt_month são coerentes com a data de transação (dt_transaction)
SELECT 
    dt_transaction
,   dt_month
,   DATE_FORMAT(dt_transaction, '%Y%m')
FROM minio.bronze.core_account
WHERE dt_month <> CAST(DATE_FORMAT(dt_transaction, '%Y%m') AS INTEGER)

SELECT 
    dt_transaction
,   dt_month
,   DATE_FORMAT(dt_transaction, '%Y%m')
FROM minio.bronze.core_pix
WHERE dt_month <> CAST(DATE_FORMAT(dt_transaction, '%Y%m') AS INTEGER)