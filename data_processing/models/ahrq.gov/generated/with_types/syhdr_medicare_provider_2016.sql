-- SQL model for syhdr_medicare_provider_2016.csv
{{ config(materialized='external', location=var('output_path') + '/' + this.name + '.parquet') }}

SELECT
    Facility_ID::VARCHAR AS Facility_ID,
    Prvdr_Ctgry_Cd::VARCHAR AS Prvdr_Ctgry_Cd,
    Prvdr_Ownrshp_Cd::VARCHAR AS Prvdr_Ownrshp_Cd,
    Prvdr_Prtcptn_Cd::VARCHAR AS Prvdr_Prtcptn_Cd
FROM read_csv('~/data/syh_dr/syhdr_medicare_provider_2016.csv', header=True, null_padding=true)