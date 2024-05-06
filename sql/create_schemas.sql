CREATE SCHEMA IF NOT EXISTS minio.raw
    WITH (location = 's3a://1.raw/');

CREATE SCHEMA IF NOT EXISTS minio.bronze
    WITH (location = 's3a://2.bronze/');

CREATE SCHEMA IF NOT EXISTS minio.silver
    WITH (location = 's3a://3.silver/');

CREATE SCHEMA IF NOT EXISTS minio.gold
    WITH (location = 's3a://4.gold/');

-- CREATE TABLES OF RAW BUCKET
   
CREATE TABLE IF NOT EXISTS minio.raw.core_account (
    dt_transaction VARCHAR,
    dt_month VARCHAR,
    surrogate_key VARCHAR,
    cd_seqlan VARCHAR, 
    ds_transaction_type VARCHAR,
    vl_transaction VARCHAR,
    id_transaction VARCHAR
) WITH (
    format = 'csv',
    skip_header_line_count = 1,
    external_location = 's3a://1.raw/core_account'
);

CREATE TABLE IF NOT EXISTS minio.raw.core_pix (
	dt_transaction VARCHAR,
	dt_month VARCHAR,
	cd_seqlan VARCHAR,
	ds_transaction_type VARCHAR,
	vl_transaction VARCHAR,
	id_transaction VARCHAR
) WITH (
	format = 'csv',
	skip_header_line_count = 1,
	external_location = 's3a://1.raw/core_pix'
);

CREATE TABLE IF NOT EXISTS minio.raw.customer (
	entry_date VARCHAR,
	surrogate_key VARCHAR,
	full_name VARCHAR,
	birth_date VARCHAR,
	uf_name VARCHAR,
	uf VARCHAR,
	street_name VARCHAR
) WITH (
	format = 'csv',
	skip_header_line_count = 1,
	external_location = 's3a://1.raw/customer'
);
