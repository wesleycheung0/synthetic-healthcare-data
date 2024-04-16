-- SQL model for syhdr_medicaid_pharmacy_2016.CSV
{{ config(materialized='external', location=var('output_path') + '/' + this.name + '.parquet') }}

SELECT
    PERSON_ID::UBIGINT AS PERSON_ID,
    CASE PERSON_WGHT WHEN '.' THEN 'Missing' END::ENUM ('Missing') AS "Person
weight",
    CASE PHMCY_CLM_NUM WHEN '.' THEN 'Missing' END::ENUM ('Missing') AS "Pharmacy claim
number",
    CASE CLM_CNTL_NUM WHEN '.' THEN 'Missing' END::ENUM ('Missing') AS "Claim number",
    LINE_NBR::VARCHAR,
    FILL_DT::DATE AS FILL_DT,
    SYNTHETIC_DRUG_ID::VARCHAR,
    CASE GENERIC_DRUG_NAME WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "Generic drug
name",
    replace(replace(PLAN_PMT_AMT, '$', ''), ',', '')::FLOAT AS PLAN_PMT_AMT,
    replace(replace(TOT_CHRG_AMT, '$', ''), ',', '')::FLOAT AS TOT_CHRG_AMT
FROM read_csv('~/data/syh_dr/syhdr_medicaid_pharmacy_2016.CSV', header=True, null_padding=true)