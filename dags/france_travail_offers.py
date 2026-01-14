from airflow.sdk import (
    asset,
    Asset,
    BaseHook
)

frt_offers_raw = Asset('s3://amzn-s3-frt-offres/raw/france_travail_raw_2025.csv')

@asset(
    name='france_travail_latest_offers',
    schedule=frt_offers_raw,
    uri='s3://amzn-s3-frt-offres/silver/france_travail_latest_offers_2025.csv'
)
def france_travail_latest_offers():
    import duckdb

    s3_details = BaseHook.get_connection('aws_ak__exerani_eop')

    con = duckdb.connect(
        ':memory:',
        config = {
            's3_access_key_id': s3_details.login,
            's3_secret_access_key': s3_details.password,
            's3_region': 'eu-west-3'}
    )

    offers = con.sql(
        """
        FROM read_csv(
            's3://amzn-s3-frt-offres/raw/france_travail_raw_2025.csv',
            strict_mode = false,
            delim = ',',
            header = true,
            escape = '"',
            columns = {
                'hash_id': VARCHAR,
                'id': VARCHAR,
                'rome_code': VARCHAR,
                'json_record': JSON,
                'created_at': DATETIME
            }
        );
        """
    )

    filter_latest = con.sql(
        """
        WITH extracted AS (
            SELECT
                hash_id,
                json_extract(
                    json_record,
                    ['id', 'dateCreation', 'dateActualisation']
                ) AS extracted_list
            FROM offers
        ),
        denormalize AS (
            SELECT
                hash_id,
                extracted_list[1] AS id,
                extracted_list[2]::DATETIME AS created_at,
                extracted_list[3]::DATETIME AS updated_at
            FROM extracted
        ),
        latest_offers AS (
            SELECT
                hash_id,
                CAST(id AS VARCHAR) AS id,
                updated_at,
                MAX(updated_at) OVER (PARTITION BY id) AS latest_offer
            FROM denormalize
        )
        SELECT hash_id
        FROM latest_offers
        WHERE updated_at = latest_offer;
        """
        )

    latest_offers = con.sql(
        """
        CREATE OR REPLACE TEMPORARY TABLE latest_offers AS
        FROM offers
        WHERE hash_id IN (SELECT hash_id FROM filter_latest);
        """
    )

    con.sql(
        """
        COPY latest_offers TO 's3://amzn-s3-frt-offres/silver/france_travail_latest_offers_2025.csv' (
        OVERWRITE_OR_IGNORE true
        );
        """)

    con.close()
