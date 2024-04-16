-- SQL model for syhdr_commercial_person_2016.CSV
{{ config(materialized='external', location=var('output_path') + '/' + this.name + '.parquet') }}

SELECT
    PERSON_ID::UBIGINT AS PERSON_ID,
    CASE PERSON_WGHT WHEN '.' THEN 'Missing' END::ENUM ('Missing') AS "Person
weight",
    AGE_LOW::NUMERIC AS AGE_LOW,
    AGE_HIGH::NUMERIC AS AGE_HIGH,
    CASE SEX_IDENT_CD WHEN 'M' THEN 'Male' WHEN 'F' THEN 'Female' WHEN 'U' THEN 'Unknown' END::ENUM ('Male', 'Female', 'Unknown') AS "Sex/Gender",
    CASE STATE_CD WHEN 'AL' THEN 'Alabama' WHEN 'AK' THEN 'Alaska' WHEN 'AZ' THEN 'Arizona' WHEN 'AR' THEN 'Arkansas' WHEN 'CA' THEN 'California' WHEN 'CO' THEN 'Colorado' WHEN 'CT' THEN 'Connecticut' WHEN 'DE' THEN 'Delaware' WHEN 'DC' THEN 'District of
Columbia' WHEN 'FL' THEN 'Florida' WHEN 'GA' THEN 'Georgia' WHEN 'HI' THEN 'Hawaii' WHEN 'ID' THEN 'Idaho' WHEN 'IL' THEN 'Illinois' WHEN 'IN' THEN 'Indiana' WHEN 'IA' THEN 'Iowa' WHEN 'KS' THEN 'Kansas' WHEN 'KY' THEN 'Kentucky' WHEN 'LA' THEN 'Louisiana' WHEN 'ME' THEN 'Maine' WHEN 'MD' THEN 'Maryland' WHEN 'MA' THEN 'Massachusetts' WHEN 'MI' THEN 'Michigan' WHEN 'MN' THEN 'Minnesota' WHEN 'MS' THEN 'Mississippi' WHEN 'MO' THEN 'Missouri' WHEN 'MT' THEN 'Montana' WHEN 'NE' THEN 'Nebraska' WHEN 'NV' THEN 'Nevada' WHEN 'NH' THEN 'New Hampshire' WHEN 'NJ' THEN 'New Jersey' WHEN 'NM' THEN 'New Mexico' WHEN 'NY' THEN 'New York' WHEN 'NC' THEN 'North Carolina' WHEN 'ND' THEN 'North Dakota' WHEN 'OH' THEN 'Ohio' WHEN 'OK' THEN 'Oklahoma' WHEN 'OR' THEN 'Oregon' WHEN 'PA' THEN 'Pennsylvania' WHEN 'RI' THEN 'Rhode Island' WHEN 'SC' THEN 'South Carolina' WHEN 'SD' THEN 'South Dakota' WHEN 'TN' THEN 'Tennessee' WHEN 'TX' THEN 'Texas' WHEN 'UT' THEN 'Utah' WHEN 'VT' THEN 'Vermont' WHEN 'VA' THEN 'Virginia' WHEN 'WA' THEN 'Washington' WHEN 'WV' THEN 'West Virginia' WHEN 'WI' THEN 'Wisconsin' WHEN 'WY' THEN 'Wyoming' END::ENUM ('Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District of
Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming') AS "State code",
    CASE COUNTY_FIPS_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "County FIPS
code",
    CASE ZIP_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "5-Digit ZIP
code",
    CASE PHRMCY_CVRG_1 –
PHRMCY_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Pharmacy coverage –
January–December",
    CASE PHRMCY_CVRG_1 –
PHRMCY_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Pharmacy coverage –
January–December",
    CASE PHRMCY_CVRG_1 –
PHRMCY_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Pharmacy coverage –
January–December",
    CASE PHRMCY_CVRG_1 –
PHRMCY_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Pharmacy coverage –
January–December",
    CASE PHRMCY_CVRG_1 –
PHRMCY_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Pharmacy coverage –
January–December",
    CASE PHRMCY_CVRG_1 –
PHRMCY_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Pharmacy coverage –
January–December",
    CASE PHRMCY_CVRG_1 –
PHRMCY_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Pharmacy coverage –
January–December",
    CASE PHRMCY_CVRG_1 –
PHRMCY_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Pharmacy coverage –
January–December",
    CASE PHRMCY_CVRG_1 –
PHRMCY_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Pharmacy coverage –
January–December",
    CASE PHRMCY_CVRG_1 –
PHRMCY_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Pharmacy coverage –
January–December",
    CASE PHRMCY_CVRG_1 –
PHRMCY_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Pharmacy coverage –
January–December",
    CASE PHRMCY_CVRG_1 –
PHRMCY_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Pharmacy coverage –
January–December",
    CMRCL_INSRC_1::NUMERIC AS CMRCL_INSRC_1,
    CMRCL_INSRC_2::NUMERIC AS CMRCL_INSRC_2,
    CMRCL_INSRC_3::NUMERIC AS CMRCL_INSRC_3,
    CMRCL_INSRC_4::NUMERIC AS CMRCL_INSRC_4,
    CMRCL_INSRC_5::NUMERIC AS CMRCL_INSRC_5,
    CMRCL_INSRC_6::NUMERIC AS CMRCL_INSRC_6,
    CMRCL_INSRC_7::NUMERIC AS CMRCL_INSRC_7,
    CMRCL_INSRC_8::NUMERIC AS CMRCL_INSRC_8,
    CMRCL_INSRC_9::NUMERIC AS CMRCL_INSRC_9,
    CMRCL_INSRC_10::NUMERIC AS CMRCL_INSRC_10,
    CMRCL_INSRC_11::NUMERIC AS CMRCL_INSRC_11,
    CMRCL_INSRC_12::NUMERIC AS CMRCL_INSRC_12
FROM read_csv('~/data/syh_dr/syhdr_commercial_person_2016.CSV', header=True, null_padding=true)