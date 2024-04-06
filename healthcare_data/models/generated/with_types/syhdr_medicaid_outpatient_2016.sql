-- SQL model for syhdr_medicaid_outpatient_2016.CSV
{{ config(materialized='external', location=var('output_path') + '/' + this.name + '.parquet') }}

SELECT
    PERSON_ID::VARCHAR,
    PERSON_WGHT::VARCHAR,
    FACILITY_ID::VARCHAR,
    CLM_CNTL_NUM::VARCHAR,
    AT_SPCLTY::VARCHAR,
    SRVC_BEG_DATE::VARCHAR,
    SRVC_END_DATE::VARCHAR,
    LOS::VARCHAR,
    ADMSN_TYPE::VARCHAR,
    TOB_CD::VARCHAR,
    CLM_TYPE_CD::VARCHAR,
    DSCHRG_STUS::VARCHAR,
    PRMRY_DX_IMPUTED::VARCHAR,
    PRMRY_DX_CD::VARCHAR,
    ICD_DX_CD_1::VARCHAR,
    ICD_DX_CD_2::VARCHAR,
    ICD_DX_CD_3::VARCHAR,
    ICD_DX_CD_4::VARCHAR,
    ICD_DX_CD_5::VARCHAR,
    ICD_DX_CD_6::VARCHAR,
    ICD_DX_CD_7::VARCHAR,
    ICD_DX_CD_8::VARCHAR,
    ICD_DX_CD_9::VARCHAR,
    ICD_DX_CD_10::VARCHAR,
    ICD_DX_CD_11::VARCHAR,
    ICD_DX_CD_12::VARCHAR,
    ICD_DX_CD_13::VARCHAR,
    ICD_DX_CD_14::VARCHAR,
    ICD_DX_CD_15::VARCHAR,
    ICD_DX_CD_16::VARCHAR,
    ICD_DX_CD_17::VARCHAR,
    ICD_DX_CD_18::VARCHAR,
    ICD_DX_CD_19::VARCHAR,
    ICD_DX_CD_20::VARCHAR,
    ICD_DX_CD_21::VARCHAR,
    ICD_DX_CD_22::VARCHAR,
    ICD_DX_CD_23::VARCHAR,
    ICD_DX_CD_24::VARCHAR,
    ICD_DX_CD_25::VARCHAR,
    ICD_PRCDR_CD_1::VARCHAR,
    ICD_PRCDR_CD_2::VARCHAR,
    ICD_PRCDR_CD_3::VARCHAR,
    ICD_PRCDR_CD_4::VARCHAR,
    ICD_PRCDR_CD_5::VARCHAR,
    ICD_PRCDR_CD_6::VARCHAR,
    ICD_PRCDR_CD_7::VARCHAR,
    ICD_PRCDR_CD_8::VARCHAR,
    ICD_PRCDR_CD_9::VARCHAR,
    ICD_PRCDR_CD_10::VARCHAR,
    ICD_PRCDR_CD_11::VARCHAR,
    ICD_PRCDR_CD_12::VARCHAR,
    ICD_PRCDR_CD_13::VARCHAR,
    ICD_PRCDR_CD_14::VARCHAR,
    ICD_PRCDR_CD_15::VARCHAR,
    ICD_PRCDR_CD_16::VARCHAR,
    ICD_PRCDR_CD_17::VARCHAR,
    ICD_PRCDR_CD_18::VARCHAR,
    ICD_PRCDR_CD_19::VARCHAR,
    ICD_PRCDR_CD_20::VARCHAR,
    ICD_PRCDR_CD_21::VARCHAR,
    ICD_PRCDR_CD_22::VARCHAR,
    ICD_PRCDR_CD_23::VARCHAR,
    ICD_PRCDR_CD_24::VARCHAR,
    ICD_PRCDR_CD_25::VARCHAR,
    CPT_PRCDR_CD_1::VARCHAR,
    CPT_PRCDR_CD_2::VARCHAR,
    CPT_PRCDR_CD_3::VARCHAR,
    CPT_PRCDR_CD_4::VARCHAR,
    CPT_PRCDR_CD_5::VARCHAR,
    CPT_PRCDR_CD_6::VARCHAR,
    CPT_PRCDR_CD_7::VARCHAR,
    CPT_PRCDR_CD_8::VARCHAR,
    CPT_PRCDR_CD_9::VARCHAR,
    CPT_PRCDR_CD_10::VARCHAR,
    CPT_PRCDR_CD_11::VARCHAR,
    CPT_PRCDR_CD_12::VARCHAR,
    CPT_PRCDR_CD_13::VARCHAR,
    CPT_PRCDR_CD_14::VARCHAR,
    CPT_PRCDR_CD_15::VARCHAR,
    CPT_PRCDR_CD_16::VARCHAR,
    CPT_PRCDR_CD_17::VARCHAR,
    CPT_PRCDR_CD_18::VARCHAR,
    CPT_PRCDR_CD_19::VARCHAR,
    CPT_PRCDR_CD_20::VARCHAR,
    CPT_PRCDR_CD_21::VARCHAR,
    CPT_PRCDR_CD_22::VARCHAR,
    CPT_PRCDR_CD_23::VARCHAR,
    CPT_PRCDR_CD_24::VARCHAR,
    CPT_PRCDR_CD_25::VARCHAR,
    CPT_PRCDR_CD_26::VARCHAR,
    CPT_PRCDR_CD_27::VARCHAR,
    CPT_PRCDR_CD_28::VARCHAR,
    CPT_PRCDR_CD_29::VARCHAR,
    CPT_PRCDR_CD_30::VARCHAR,
    CPT_PRCDR_CD_31::VARCHAR,
    CPT_PRCDR_CD_32::VARCHAR,
    CPT_PRCDR_CD_33::VARCHAR,
    CPT_PRCDR_CD_34::VARCHAR,
    CPT_PRCDR_CD_35::VARCHAR,
    PLAN_PMT_AMT::VARCHAR,
    TOT_CHRG_AMT::VARCHAR
FROM read_csv('/Users/me/data/syh_dr/syhdr_medicaid_outpatient_2016.CSV', header=True, null_padding=true, types={'CPT_PRCDR_CD_1': 'VARCHAR', 'CPT_PRCDR_CD_2': 'VARCHAR', 'CPT_PRCDR_CD_3': 'VARCHAR', 'CPT_PRCDR_CD_4': 'VARCHAR', 'CPT_PRCDR_CD_5': 'VARCHAR', 'CPT_PRCDR_CD_6': 'VARCHAR', 'CPT_PRCDR_CD_7': 'VARCHAR', 'CPT_PRCDR_CD_8': 'VARCHAR', 'CPT_PRCDR_CD_9': 'VARCHAR', 'CPT_PRCDR_CD_10': 'VARCHAR', 'CPT_PRCDR_CD_11': 'VARCHAR', 'CPT_PRCDR_CD_12': 'VARCHAR', 'CPT_PRCDR_CD_13': 'VARCHAR', 'CPT_PRCDR_CD_14': 'VARCHAR', 'CPT_PRCDR_CD_15': 'VARCHAR', 'CPT_PRCDR_CD_16': 'VARCHAR', 'CPT_PRCDR_CD_17': 'VARCHAR', 'CPT_PRCDR_CD_18': 'VARCHAR', 'CPT_PRCDR_CD_19': 'VARCHAR', 'CPT_PRCDR_CD_20': 'VARCHAR', 'CPT_PRCDR_CD_21': 'VARCHAR', 'CPT_PRCDR_CD_22': 'VARCHAR', 'CPT_PRCDR_CD_23': 'VARCHAR', 'CPT_PRCDR_CD_24': 'VARCHAR', 'CPT_PRCDR_CD_25': 'VARCHAR', 'CPT_PRCDR_CD_26': 'VARCHAR', 'CPT_PRCDR_CD_27': 'VARCHAR', 'CPT_PRCDR_CD_28': 'VARCHAR', 'CPT_PRCDR_CD_29': 'VARCHAR', 'CPT_PRCDR_CD_30': 'VARCHAR', 'CPT_PRCDR_CD_31': 'VARCHAR', 'CPT_PRCDR_CD_32': 'VARCHAR', 'CPT_PRCDR_CD_33': 'VARCHAR', 'CPT_PRCDR_CD_34': 'VARCHAR', 'CPT_PRCDR_CD_35': 'VARCHAR'}, ignore_errors=true)