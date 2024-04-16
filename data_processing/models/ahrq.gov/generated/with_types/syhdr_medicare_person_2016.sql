-- SQL model for syhdr_medicare_person_2016.CSV
{{ config(materialized='external', location=var('output_path') + '/' + this.name + '.parquet') }}

SELECT
    PERSON_ID::UBIGINT AS PERSON_ID,
    CASE PERSON_WGHT WHEN '.' THEN 'Missing' END::ENUM ('Missing') AS "Person
weight",
    AGE_LOW::NUMERIC AS AGE_LOW,
    AGE_HIGH::NUMERIC AS AGE_HIGH,
    CASE SEX_IDENT_CD WHEN 'M' THEN 'Male' WHEN 'F' THEN 'Female' WHEN 'U' THEN 'Unknown' END::ENUM ('Male', 'Female', 'Unknown') AS "Sex/Gender",
    CASE RACE_CD WHEN '0' THEN 'Unknown' WHEN '1' THEN 'White' WHEN '2' THEN 'Black' WHEN '3' THEN 'Hispanic' WHEN '9' THEN 'Other or Asian or North American Native' WHEN 'Blank' THEN 'Missing' END::ENUM ('Unknown', 'White', 'Black', 'Hispanic', 'Other or Asian or North American Native', 'Missing') AS "Race code",
    CASE STATE_CD WHEN 'AL' THEN 'Alabama' WHEN 'AK' THEN 'Alaska' WHEN 'AZ' THEN 'Arizona' WHEN 'AR' THEN 'Arkansas' WHEN 'CA' THEN 'California' WHEN 'CO' THEN 'Colorado' WHEN 'CT' THEN 'Connecticut' WHEN 'DE' THEN 'Delaware' WHEN 'DC' THEN 'District of
Columbia' WHEN 'FL' THEN 'Florida' WHEN 'GA' THEN 'Georgia' WHEN 'HI' THEN 'Hawaii' WHEN 'ID' THEN 'Idaho' WHEN 'IL' THEN 'Illinois' WHEN 'IN' THEN 'Indiana' WHEN 'IA' THEN 'Iowa' WHEN 'KS' THEN 'Kansas' WHEN 'KY' THEN 'Kentucky' WHEN 'LA' THEN 'Louisiana' WHEN 'ME' THEN 'Maine' WHEN 'MD' THEN 'Maryland' WHEN 'MA' THEN 'Massachusetts' WHEN 'MI' THEN 'Michigan' WHEN 'MN' THEN 'Minnesota' WHEN 'MS' THEN 'Mississippi' WHEN 'MO' THEN 'Missouri' WHEN 'MT' THEN 'Montana' WHEN 'NE' THEN 'Nebraska' WHEN 'NV' THEN 'Nevada' WHEN 'NH' THEN 'New Hampshire' WHEN 'NJ' THEN 'New Jersey' WHEN 'NM' THEN 'New Mexico' WHEN 'NY' THEN 'New York' WHEN 'NC' THEN 'North Carolina' WHEN 'ND' THEN 'North Dakota' WHEN 'OH' THEN 'Ohio' WHEN 'OK' THEN 'Oklahoma' WHEN 'OR' THEN 'Oregon' WHEN 'PA' THEN 'Pennsylvania' WHEN 'RI' THEN 'Rhode Island' WHEN 'SC' THEN 'South Carolina' WHEN 'SD' THEN 'South Dakota' WHEN 'TN' THEN 'Tennessee' WHEN 'TX' THEN 'Texas' WHEN 'UT' THEN 'Utah' WHEN 'VT' THEN 'Vermont' WHEN 'VA' THEN 'Virginia' WHEN 'WA' THEN 'Washington' WHEN 'WV' THEN 'West Virginia' WHEN 'WI' THEN 'Wisconsin' WHEN 'WY' THEN 'Wyoming' END::ENUM ('Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'District of
Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming') AS "State code",
    CASE COUNTY_FIPS_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "County FIPS
code",
    CASE ZIP_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "5-Digit ZIP
code",
    CASE RSN_ENRLMT_CD WHEN '1' THEN 'Children' WHEN '2' THEN 'CHIP' WHEN '3' THEN 'Adult' WHEN '4' THEN 'Disabled' WHEN '5' THEN 'Aged' WHEN '6' THEN 'Expansion' WHEN '7' THEN 'End-stage renal
disease (ESRD)' WHEN '8' THEN 'ESRD and disabled' WHEN 'Blank' THEN 'Missing' END::ENUM ('Children', 'CHIP', 'Adult', 'Disabled', 'Aged', 'Expansion', 'End-stage renal
disease (ESRD)', 'ESRD and disabled', 'Missing') AS "Reason for entitlement/eligibility
code",
    CASE MDCR_ENTLMT_IND_1–
MDCR_ENTLMT_IND WHEN '0' THEN 'Not entitled' WHEN '1' THEN 'Part A only' WHEN '2' THEN 'Part B only' WHEN '3' THEN 'Part A and Part B' WHEN 'A' THEN 'Part A state buy-in' WHEN 'B' THEN 'Part B state buy-in' WHEN 'C' THEN 'Part A and Part B state
buy-in' WHEN 'Blank' THEN 'Missing' END::ENUM ('Not entitled', 'Part A only', 'Part B only', 'Part A and Part B', 'Part A state buy-in', 'Part B state buy-in', 'Part A and Part B state
buy-in', 'Missing') AS "Medicare entitlement indicator –
J anuary–December",
    CASE MDCR_ENTLMT_IND_1–
MDCR_ENTLMT_IND WHEN '0' THEN 'Not entitled' WHEN '1' THEN 'Part A only' WHEN '2' THEN 'Part B only' WHEN '3' THEN 'Part A and Part B' WHEN 'A' THEN 'Part A state buy-in' WHEN 'B' THEN 'Part B state buy-in' WHEN 'C' THEN 'Part A and Part B state
buy-in' WHEN 'Blank' THEN 'Missing' END::ENUM ('Not entitled', 'Part A only', 'Part B only', 'Part A and Part B', 'Part A state buy-in', 'Part B state buy-in', 'Part A and Part B state
buy-in', 'Missing') AS "Medicare entitlement indicator –
J anuary–December",
    CASE MDCR_ENTLMT_IND_1–
MDCR_ENTLMT_IND WHEN '0' THEN 'Not entitled' WHEN '1' THEN 'Part A only' WHEN '2' THEN 'Part B only' WHEN '3' THEN 'Part A and Part B' WHEN 'A' THEN 'Part A state buy-in' WHEN 'B' THEN 'Part B state buy-in' WHEN 'C' THEN 'Part A and Part B state
buy-in' WHEN 'Blank' THEN 'Missing' END::ENUM ('Not entitled', 'Part A only', 'Part B only', 'Part A and Part B', 'Part A state buy-in', 'Part B state buy-in', 'Part A and Part B state
buy-in', 'Missing') AS "Medicare entitlement indicator –
J anuary–December",
    CASE MDCR_ENTLMT_IND_1–
MDCR_ENTLMT_IND WHEN '0' THEN 'Not entitled' WHEN '1' THEN 'Part A only' WHEN '2' THEN 'Part B only' WHEN '3' THEN 'Part A and Part B' WHEN 'A' THEN 'Part A state buy-in' WHEN 'B' THEN 'Part B state buy-in' WHEN 'C' THEN 'Part A and Part B state
buy-in' WHEN 'Blank' THEN 'Missing' END::ENUM ('Not entitled', 'Part A only', 'Part B only', 'Part A and Part B', 'Part A state buy-in', 'Part B state buy-in', 'Part A and Part B state
buy-in', 'Missing') AS "Medicare entitlement indicator –
J anuary–December",
    CASE MDCR_ENTLMT_IND_1–
MDCR_ENTLMT_IND WHEN '0' THEN 'Not entitled' WHEN '1' THEN 'Part A only' WHEN '2' THEN 'Part B only' WHEN '3' THEN 'Part A and Part B' WHEN 'A' THEN 'Part A state buy-in' WHEN 'B' THEN 'Part B state buy-in' WHEN 'C' THEN 'Part A and Part B state
buy-in' WHEN 'Blank' THEN 'Missing' END::ENUM ('Not entitled', 'Part A only', 'Part B only', 'Part A and Part B', 'Part A state buy-in', 'Part B state buy-in', 'Part A and Part B state
buy-in', 'Missing') AS "Medicare entitlement indicator –
J anuary–December",
    CASE MDCR_ENTLMT_IND_1–
MDCR_ENTLMT_IND WHEN '0' THEN 'Not entitled' WHEN '1' THEN 'Part A only' WHEN '2' THEN 'Part B only' WHEN '3' THEN 'Part A and Part B' WHEN 'A' THEN 'Part A state buy-in' WHEN 'B' THEN 'Part B state buy-in' WHEN 'C' THEN 'Part A and Part B state
buy-in' WHEN 'Blank' THEN 'Missing' END::ENUM ('Not entitled', 'Part A only', 'Part B only', 'Part A and Part B', 'Part A state buy-in', 'Part B state buy-in', 'Part A and Part B state
buy-in', 'Missing') AS "Medicare entitlement indicator –
J anuary–December",
    CASE MDCR_ENTLMT_IND_1–
MDCR_ENTLMT_IND WHEN '0' THEN 'Not entitled' WHEN '1' THEN 'Part A only' WHEN '2' THEN 'Part B only' WHEN '3' THEN 'Part A and Part B' WHEN 'A' THEN 'Part A state buy-in' WHEN 'B' THEN 'Part B state buy-in' WHEN 'C' THEN 'Part A and Part B state
buy-in' WHEN 'Blank' THEN 'Missing' END::ENUM ('Not entitled', 'Part A only', 'Part B only', 'Part A and Part B', 'Part A state buy-in', 'Part B state buy-in', 'Part A and Part B state
buy-in', 'Missing') AS "Medicare entitlement indicator –
J anuary–December",
    CASE MDCR_ENTLMT_IND_1–
MDCR_ENTLMT_IND WHEN '0' THEN 'Not entitled' WHEN '1' THEN 'Part A only' WHEN '2' THEN 'Part B only' WHEN '3' THEN 'Part A and Part B' WHEN 'A' THEN 'Part A state buy-in' WHEN 'B' THEN 'Part B state buy-in' WHEN 'C' THEN 'Part A and Part B state
buy-in' WHEN 'Blank' THEN 'Missing' END::ENUM ('Not entitled', 'Part A only', 'Part B only', 'Part A and Part B', 'Part A state buy-in', 'Part B state buy-in', 'Part A and Part B state
buy-in', 'Missing') AS "Medicare entitlement indicator –
J anuary–December",
    CASE MDCR_ENTLMT_IND_1–
MDCR_ENTLMT_IND WHEN '0' THEN 'Not entitled' WHEN '1' THEN 'Part A only' WHEN '2' THEN 'Part B only' WHEN '3' THEN 'Part A and Part B' WHEN 'A' THEN 'Part A state buy-in' WHEN 'B' THEN 'Part B state buy-in' WHEN 'C' THEN 'Part A and Part B state
buy-in' WHEN 'Blank' THEN 'Missing' END::ENUM ('Not entitled', 'Part A only', 'Part B only', 'Part A and Part B', 'Part A state buy-in', 'Part B state buy-in', 'Part A and Part B state
buy-in', 'Missing') AS "Medicare entitlement indicator –
J anuary–December",
    CASE MDCR_ENTLMT_IND_1–
MDCR_ENTLMT_IND WHEN '0' THEN 'Not entitled' WHEN '1' THEN 'Part A only' WHEN '2' THEN 'Part B only' WHEN '3' THEN 'Part A and Part B' WHEN 'A' THEN 'Part A state buy-in' WHEN 'B' THEN 'Part B state buy-in' WHEN 'C' THEN 'Part A and Part B state
buy-in' WHEN 'Blank' THEN 'Missing' END::ENUM ('Not entitled', 'Part A only', 'Part B only', 'Part A and Part B', 'Part A state buy-in', 'Part B state buy-in', 'Part A and Part B state
buy-in', 'Missing') AS "Medicare entitlement indicator –
J anuary–December",
    CASE MDCR_ENTLMT_IND_1–
MDCR_ENTLMT_IND WHEN '0' THEN 'Not entitled' WHEN '1' THEN 'Part A only' WHEN '2' THEN 'Part B only' WHEN '3' THEN 'Part A and Part B' WHEN 'A' THEN 'Part A state buy-in' WHEN 'B' THEN 'Part B state buy-in' WHEN 'C' THEN 'Part A and Part B state
buy-in' WHEN 'Blank' THEN 'Missing' END::ENUM ('Not entitled', 'Part A only', 'Part B only', 'Part A and Part B', 'Part A state buy-in', 'Part B state buy-in', 'Part A and Part B state
buy-in', 'Missing') AS "Medicare entitlement indicator –
J anuary–December",
    CASE MDCR_ENTLMT_IND_1–
MDCR_ENTLMT_IND WHEN '0' THEN 'Not entitled' WHEN '1' THEN 'Part A only' WHEN '2' THEN 'Part B only' WHEN '3' THEN 'Part A and Part B' WHEN 'A' THEN 'Part A state buy-in' WHEN 'B' THEN 'Part B state buy-in' WHEN 'C' THEN 'Part A and Part B state
buy-in' WHEN 'Blank' THEN 'Missing' END::ENUM ('Not entitled', 'Part A only', 'Part B only', 'Part A and Part B', 'Part A state buy-in', 'Part B state buy-in', 'Part A and Part B state
buy-in', 'Missing') AS "Medicare entitlement indicator –
J anuary–December",
    CASE MDCR_HMO_CVRG_1–
MDCR_HMO_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Medicare Advan tage (MA) enrollment
indica tor – January–Dece mber",
    CASE MDCR_HMO_CVRG_1–
MDCR_HMO_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Medicare Advan tage (MA) enrollment
indica tor – January–Dece mber",
    CASE MDCR_HMO_CVRG_1–
MDCR_HMO_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Medicare Advan tage (MA) enrollment
indica tor – January–Dece mber",
    CASE MDCR_HMO_CVRG_1–
MDCR_HMO_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Medicare Advan tage (MA) enrollment
indica tor – January–Dece mber",
    CASE MDCR_HMO_CVRG_1–
MDCR_HMO_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Medicare Advan tage (MA) enrollment
indica tor – January–Dece mber",
    CASE MDCR_HMO_CVRG_1–
MDCR_HMO_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Medicare Advan tage (MA) enrollment
indica tor – January–Dece mber",
    CASE MDCR_HMO_CVRG_1–
MDCR_HMO_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Medicare Advan tage (MA) enrollment
indica tor – January–Dece mber",
    CASE MDCR_HMO_CVRG_1–
MDCR_HMO_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Medicare Advan tage (MA) enrollment
indica tor – January–Dece mber",
    CASE MDCR_HMO_CVRG_1–
MDCR_HMO_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Medicare Advan tage (MA) enrollment
indica tor – January–Dece mber",
    CASE MDCR_HMO_CVRG_1–
MDCR_HMO_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Medicare Advan tage (MA) enrollment
indica tor – January–Dece mber",
    CASE MDCR_HMO_CVRG_1–
MDCR_HMO_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Medicare Advan tage (MA) enrollment
indica tor – January–Dece mber",
    CASE MDCR_HMO_CVRG_1–
MDCR_HMO_CVRG WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN 'Blank' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Medicare Advan tage (MA) enrollment
indica tor – January–Dece mber",
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
    CASE DUAL_ELGBL_1–
DUAL_ELGBL WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN '.' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Dual eligibility code –
January–December",
    CASE DUAL_ELGBL_1–
DUAL_ELGBL WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN '.' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Dual eligibility code –
January–December",
    CASE DUAL_ELGBL_1–
DUAL_ELGBL WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN '.' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Dual eligibility code –
January–December",
    CASE DUAL_ELGBL_1–
DUAL_ELGBL WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN '.' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Dual eligibility code –
January–December",
    CASE DUAL_ELGBL_1–
DUAL_ELGBL WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN '.' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Dual eligibility code –
January–December",
    CASE DUAL_ELGBL_1–
DUAL_ELGBL WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN '.' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Dual eligibility code –
January–December",
    CASE DUAL_ELGBL_1–
DUAL_ELGBL WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN '.' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Dual eligibility code –
January–December",
    CASE DUAL_ELGBL_1–
DUAL_ELGBL WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN '.' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Dual eligibility code –
January–December",
    CASE DUAL_ELGBL_1–
DUAL_ELGBL WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN '.' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Dual eligibility code –
January–December",
    CASE DUAL_ELGBL_1–
DUAL_ELGBL WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN '.' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Dual eligibility code –
January–December",
    CASE DUAL_ELGBL_1–
DUAL_ELGBL WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN '.' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Dual eligibility code –
January–December",
    CASE DUAL_ELGBL_1–
DUAL_ELGBL WHEN '0' THEN 'No' WHEN '1' THEN 'Yes' WHEN '.' THEN 'Missing' END::ENUM ('No', 'Yes', 'Missing') AS "Dual eligibility code –
January–December"
FROM read_csv('~/data/syh_dr/syhdr_medicare_person_2016.CSV', header=True, null_padding=true)