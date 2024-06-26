-- SQL model for syhdr_medicare_person_2016.CSV
{{ config(materialized='external', location=var('output_path') + '/' + this.name + '.parquet') }}

SELECT
    PERSON_ID::UBIGINT AS PERSON_ID,
    PERSON_WGHT::NUMERIC AS PERSON_WGHT,
    AGE_LOW::NUMERIC AS AGE_LOW,
    AGE_HIGH::NUMERIC AS AGE_HIGH,
    SEX_IDENT_CD::UBIGINT AS SEX_IDENT_CD,
    RACE_CD::VARCHAR AS RACE_CD,
    STATE_CD::VARCHAR AS STATE_CD,
    COUNTY_FIPS_CD::VARCHAR AS COUNTY_FIPS_CD,
    ZIP_CD::VARCHAR AS ZIP_CD,
    RSN_ENRLMT_CD::VARCHAR AS RSN_ENRLMT_CD,
    MDCR_ENTLMT_IND_1::VARCHAR AS MDCR_ENTLMT_IND_1,
    MDCR_ENTLMT_IND_2::VARCHAR AS MDCR_ENTLMT_IND_2,
    MDCR_ENTLMT_IND_3::VARCHAR AS MDCR_ENTLMT_IND_3,
    MDCR_ENTLMT_IND_4::VARCHAR AS MDCR_ENTLMT_IND_4,
    MDCR_ENTLMT_IND_5::VARCHAR AS MDCR_ENTLMT_IND_5,
    MDCR_ENTLMT_IND_6::VARCHAR AS MDCR_ENTLMT_IND_6,
    MDCR_ENTLMT_IND_7::VARCHAR AS MDCR_ENTLMT_IND_7,
    MDCR_ENTLMT_IND_8::VARCHAR AS MDCR_ENTLMT_IND_8,
    MDCR_ENTLMT_IND_9::VARCHAR AS MDCR_ENTLMT_IND_9,
    MDCR_ENTLMT_IND_10::VARCHAR AS MDCR_ENTLMT_IND_10,
    MDCR_ENTLMT_IND_11::VARCHAR AS MDCR_ENTLMT_IND_11,
    MDCR_ENTLMT_IND_12::VARCHAR AS MDCR_ENTLMT_IND_12,
    MDCR_HMO_CVRG_1::NUMERIC AS MDCR_HMO_CVRG_1,
    MDCR_HMO_CVRG_2::NUMERIC AS MDCR_HMO_CVRG_2,
    MDCR_HMO_CVRG_3::NUMERIC AS MDCR_HMO_CVRG_3,
    MDCR_HMO_CVRG_4::NUMERIC AS MDCR_HMO_CVRG_4,
    MDCR_HMO_CVRG_5::NUMERIC AS MDCR_HMO_CVRG_5,
    MDCR_HMO_CVRG_6::NUMERIC AS MDCR_HMO_CVRG_6,
    MDCR_HMO_CVRG_7::NUMERIC AS MDCR_HMO_CVRG_7,
    MDCR_HMO_CVRG_8::NUMERIC AS MDCR_HMO_CVRG_8,
    MDCR_HMO_CVRG_9::NUMERIC AS MDCR_HMO_CVRG_9,
    MDCR_HMO_CVRG_10::NUMERIC AS MDCR_HMO_CVRG_10,
    MDCR_HMO_CVRG_11::NUMERIC AS MDCR_HMO_CVRG_11,
    MDCR_HMO_CVRG_12::NUMERIC AS MDCR_HMO_CVRG_12,
    PHRMCY_CVRG_1::NUMERIC AS PHRMCY_CVRG_1,
    PHRMCY_CVRG_2::NUMERIC AS PHRMCY_CVRG_2,
    PHRMCY_CVRG_3::NUMERIC AS PHRMCY_CVRG_3,
    PHRMCY_CVRG_4::NUMERIC AS PHRMCY_CVRG_4,
    PHRMCY_CVRG_5::NUMERIC AS PHRMCY_CVRG_5,
    PHRMCY_CVRG_6::NUMERIC AS PHRMCY_CVRG_6,
    PHRMCY_CVRG_7::NUMERIC AS PHRMCY_CVRG_7,
    PHRMCY_CVRG_8::NUMERIC AS PHRMCY_CVRG_8,
    PHRMCY_CVRG_9::NUMERIC AS PHRMCY_CVRG_9,
    PHRMCY_CVRG_10::NUMERIC AS PHRMCY_CVRG_10,
    PHRMCY_CVRG_11::NUMERIC AS PHRMCY_CVRG_11,
    PHRMCY_CVRG_12::NUMERIC AS PHRMCY_CVRG_12,
    DUAL_ELGBL_1::NUMERIC AS DUAL_ELGBL_1,
    DUAL_ELGBL_2::NUMERIC AS DUAL_ELGBL_2,
    DUAL_ELGBL_3::NUMERIC AS DUAL_ELGBL_3,
    DUAL_ELGBL_4::NUMERIC AS DUAL_ELGBL_4,
    DUAL_ELGBL_5::NUMERIC AS DUAL_ELGBL_5,
    DUAL_ELGBL_6::NUMERIC AS DUAL_ELGBL_6,
    DUAL_ELGBL_7::NUMERIC AS DUAL_ELGBL_7,
    DUAL_ELGBL_8::NUMERIC AS DUAL_ELGBL_8,
    DUAL_ELGBL_9::NUMERIC AS DUAL_ELGBL_9,
    DUAL_ELGBL_10::NUMERIC AS DUAL_ELGBL_10,
    DUAL_ELGBL_11::NUMERIC AS DUAL_ELGBL_11,
    DUAL_ELGBL_12::NUMERIC AS DUAL_ELGBL_12
FROM read_csv('~/data/syh_dr/syhdr_medicare_person_2016.CSV', header=True, null_padding=true, types={ 'PERSON_ID': 'UBIGINT', 'PERSON_WGHT': 'NUMERIC', 'AGE_LOW': 'NUMERIC', 'AGE_HIGH': 'NUMERIC', 'SEX_IDENT_CD': 'UBIGINT', 'RACE_CD': 'VARCHAR', 'STATE_CD': 'VARCHAR', 'COUNTY_FIPS_CD': 'VARCHAR', 'ZIP_CD': 'VARCHAR', 'RSN_ENRLMT_CD': 'VARCHAR', 'MDCR_ENTLMT_IND_1': 'VARCHAR', 'MDCR_ENTLMT_IND_2': 'VARCHAR', 'MDCR_ENTLMT_IND_3': 'VARCHAR', 'MDCR_ENTLMT_IND_4': 'VARCHAR', 'MDCR_ENTLMT_IND_5': 'VARCHAR', 'MDCR_ENTLMT_IND_6': 'VARCHAR', 'MDCR_ENTLMT_IND_7': 'VARCHAR', 'MDCR_ENTLMT_IND_8': 'VARCHAR', 'MDCR_ENTLMT_IND_9': 'VARCHAR', 'MDCR_ENTLMT_IND_10': 'VARCHAR', 'MDCR_ENTLMT_IND_11': 'VARCHAR', 'MDCR_ENTLMT_IND_12': 'VARCHAR', 'MDCR_HMO_CVRG_1': 'NUMERIC', 'MDCR_HMO_CVRG_2': 'NUMERIC', 'MDCR_HMO_CVRG_3': 'NUMERIC', 'MDCR_HMO_CVRG_4': 'NUMERIC', 'MDCR_HMO_CVRG_5': 'NUMERIC', 'MDCR_HMO_CVRG_6': 'NUMERIC', 'MDCR_HMO_CVRG_7': 'NUMERIC', 'MDCR_HMO_CVRG_8': 'NUMERIC', 'MDCR_HMO_CVRG_9': 'NUMERIC', 'MDCR_HMO_CVRG_10': 'NUMERIC', 'MDCR_HMO_CVRG_11': 'NUMERIC', 'MDCR_HMO_CVRG_12': 'NUMERIC', 'PHRMCY_CVRG_1': 'NUMERIC', 'PHRMCY_CVRG_2': 'NUMERIC', 'PHRMCY_CVRG_3': 'NUMERIC', 'PHRMCY_CVRG_4': 'NUMERIC', 'PHRMCY_CVRG_5': 'NUMERIC', 'PHRMCY_CVRG_6': 'NUMERIC', 'PHRMCY_CVRG_7': 'NUMERIC', 'PHRMCY_CVRG_8': 'NUMERIC', 'PHRMCY_CVRG_9': 'NUMERIC', 'PHRMCY_CVRG_10': 'NUMERIC', 'PHRMCY_CVRG_11': 'NUMERIC', 'PHRMCY_CVRG_12': 'NUMERIC', 'DUAL_ELGBL_1': 'NUMERIC', 'DUAL_ELGBL_2': 'NUMERIC', 'DUAL_ELGBL_3': 'NUMERIC', 'DUAL_ELGBL_4': 'NUMERIC', 'DUAL_ELGBL_5': 'NUMERIC', 'DUAL_ELGBL_6': 'NUMERIC', 'DUAL_ELGBL_7': 'NUMERIC', 'DUAL_ELGBL_8': 'NUMERIC', 'DUAL_ELGBL_9': 'NUMERIC', 'DUAL_ELGBL_10': 'NUMERIC', 'DUAL_ELGBL_11': 'NUMERIC', 'DUAL_ELGBL_12': 'NUMERIC' }, ignore_errors=true)

