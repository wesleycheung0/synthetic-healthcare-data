-- SQL model for syhdr_medicaid_inpatient_2016.CSV
{{ config(materialized='external', location=var('output_path') + '/' + this.name + '.parquet') }}

SELECT
    PERSON_ID::UBIGINT AS PERSON_ID,
    CASE PERSON_WGHT WHEN '.' THEN 'Missing' END::ENUM ('Missing') AS "Person
weight",
    CASE FACILITY_ID WHEN '.' THEN 'Missing' END::ENUM ('Missing') AS "Facility ID",
    CASE CLM_CNTL_NUM WHEN '.' THEN 'Missing' END::ENUM ('Missing') AS "Claim number",
    CASE AT_SPCLTY WHEN '0' THEN 'Carrierwide' WHEN '1' THEN 'General practice' WHEN '2' THEN 'General surgery' WHEN '3' THEN 'Allergy/immunology' WHEN '4' THEN 'Otolaryngology' WHEN '5' THEN 'Anesthesiology' WHEN '6' THEN 'Cardiology' WHEN '7' THEN 'Dermatology' WHEN '8' THEN 'Family practice' WHEN '9' THEN 'Interventional pain management (IPM) (eff. 4/1/03)' WHEN '10' THEN 'Gastroenterology' WHEN '11' THEN 'Internal medicine' WHEN '12' THEN 'Osteopathic manipulative therapy' WHEN '13' THEN 'Neurology' WHEN '14' THEN 'Neurosurgery' WHEN '15' THEN 'Speech/language pathology' WHEN '16' THEN 'Obstetrics/gynecology' WHEN '17' THEN 'Hospice and palliative care' WHEN '18' THEN 'Ophthalmology' WHEN '19' THEN 'Oral surgery (dentists only)' WHEN '20' THEN 'Orthopedic surgery' WHEN '21' THEN 'Cardiac electrophysiology' WHEN '22' THEN 'Pathology' WHEN '24' THEN 'Plastic and reconstructive surgery' WHEN '25' THEN 'Physical medicine and rehabilitation' WHEN '26' THEN 'Psychiatry' WHEN '27' THEN 'General psychiatry' WHEN '28' THEN 'Colorectal surgery (formerly proctology)' WHEN '29' THEN 'Pulmonary disease' WHEN '30' THEN 'Diagnostic radiology' WHEN '31' THEN 'Intensive cardiac rehabilitation' WHEN '32' THEN 'Anesthesiologist assistants (eff. 4/1/03—previously
grouped with certified registered nurse anesthetists
[CRNAs] )' WHEN '33' THEN 'Thoracic surgery' WHEN '34' THEN 'Urology' WHEN '35' THEN 'Chiropractic' WHEN '36' THEN 'Nuclear medicine' WHEN '37' THEN 'Pediatric medicine' WHEN '38' THEN 'Geriatric medicine' WHEN '39' THEN 'Nephrology' WHEN '40' THEN 'Hand surgery' WHEN '41' THEN 'Optometrist' WHEN '42' THEN 'Certified nurse midwife' WHEN '43' THEN 'CRNA (anesthesiologist assistants were removed from this
specialty 4/1/03)' WHEN '44' THEN 'Infectious disease' WHEN '45' THEN 'Mammography screening center' WHEN '46' THEN 'Endocrinology' WHEN '47' THEN 'Independent diagnostic testing facility (IDTF)' WHEN '48' THEN 'Podiatry' WHEN '49' THEN 'Ambulatory surgical center (formerly miscellaneous)' WHEN '50' THEN 'Nurse practitioner' WHEN '51' THEN 'Medical supply company with certified orthotist (certified by
American Board for Certification in Prosthetics and
Orthotics)' WHEN '52' THEN 'Medical supply company with certified prosthetist (certified
by American Board for Certification in Prosthetics and
Orthotics)' WHEN '53' THEN 'Medical supply company with certified prosthetist-orthotist
(certified by American Board for Certification in Prosthetics
and Orthotics)' WHEN '54' THEN 'Medical supply company for durable medical equipment
regional carrier (DMERC) (and not included in 51–53)' WHEN '55' THEN 'Individual certified orthotist' WHEN '56' THEN 'Individual certified prosthetist' WHEN '57' THEN 'Individual certified prosthetist-orthotist' WHEN '58' THEN 'Medical supply company with registered pharmacist' WHEN '59' THEN 'Ambulance service supplier, (e.g., private ambulance
companies, funeral homes)' WHEN '60' THEN 'Public health or welfare agencies (federal, state, and local)' WHEN '61' THEN 'Voluntary health or charitable agencies (e.g., National
Cancer Society, National Heart Association, Catholic
Charities)' WHEN '62' THEN 'Psychologist (billing independently)' WHEN '63' THEN 'Portable x ray supplier' WHEN '64' THEN 'Audiologist (billing independently)' WHEN '65' THEN 'Physical therapist (private practice added 4/1/03)
(independently practicing removed 4/1/03)' WHEN '66' THEN 'Rheumatology' WHEN '67' THEN 'Occupational therapist (private practice added 4/1/03)
(independently practicing removed 4/1/03)' WHEN '68' THEN 'Clinical psychologist' WHEN '69' THEN 'Clinical laboratory (billing independently)' WHEN '70' THEN 'Multispecialty clinic or group practice' WHEN '71' THEN 'Registered dietitian/nutrition professional (eff. 1/1/02)' WHEN '72' THEN 'Pain management (eff. 1/1/02)' WHEN '73' THEN 'Mass immunization roster biller' WHEN '74' THEN 'Radiation therapy centers (prior to 4/2003, this
include d IDTFs)' WHEN '75' THEN 'Slide preparation facilities (added to differentiate them from
IDTFs—eff. 4/1/03)' WHEN '76' THEN 'Peripheral vascular disease' WHEN '77' THEN 'Vascular surgery' WHEN '78' THEN 'Cardiac surgery' WHEN '79' THEN 'Addiction medicine' WHEN '80' THEN 'Licensed clinical social worker' WHEN '81' THEN 'Critical care (intensivists)' WHEN '82' THEN 'Hematology' WHEN '83' THEN 'Hematology/oncology' WHEN '84' THEN 'Preventive medicine' WHEN '85' THEN 'Maxillofacial surgery' WHEN '86' THEN 'Neuropsychiatry' WHEN '87' THEN 'All other suppliers (e.g., drug and department stores)' WHEN '88' THEN 'Unknown supplier/provider specialty' WHEN '89' THEN 'Certified clinical nurse specialist' WHEN '90' THEN 'Medical oncology' WHEN '91' THEN 'Surgical oncology' WHEN '92' THEN 'Radiation oncology' WHEN '93' THEN 'Emergency medicine' WHEN '94' THEN 'Interventional radiology' WHEN '95' THEN 'Competitive Acquisition Program (CAP) vendor (eff.
07/01/06). Prior to 07/01/06, known as independent
physiological laboratory' WHEN '96' THEN 'Optician' WHEN '97' THEN 'Physician assistant' WHEN '98' THEN 'Gynecologist/oncologist' WHEN '99' THEN 'Unknown physician specialty' WHEN 'A0' THEN 'Hospital (DMERCs only)' WHEN 'A1' THEN 'SNF (DMERCs only)' WHEN 'A2' THEN 'Intermediate care nursing facility (DMERCs only)' WHEN 'A3' THEN 'Nursing facility, other (DMERCs only)' WHEN 'A4' THEN 'Home health agency (DMERCs only)' WHEN 'A5' THEN 'Pharmacy (DMERC)' WHEN 'A6' THEN 'Medical supply company with respiratory therapist
(DMERCs only)' WHEN 'A7' THEN 'Department store (DMERC)' WHEN 'A8' THEN 'Grocery store (DMERC)' WHEN 'A9' THEN 'Indian Health Service (IHS), tribe, and tribal organizations
(non-hospital or non-hospital-based facilities, eff. 1/2005)' WHEN 'B1' THEN 'Supplier of oxygen and/or oxygen-related equipment (eff.
10/2/07)' WHEN 'B2' THEN 'Pedorthic personnel (eff. 10/2/07)' WHEN 'B3' THEN 'Medical supply company with pedorthic personnel (eff.
10/2/07)' WHEN 'B4' THEN 'Does not meet definition of healthcare provider (e.g.,
rehabilitation agency, organ procurement organization,
histocompatibility lab) (eff. 10/2/07)' WHEN 'B5' THEN 'Ocularist' WHEN 'C0' THEN 'Sleep medicine' WHEN 'C1' THEN 'Centralized flu' WHEN 'C2' THEN 'Indirect payment procedure' WHEN 'C3' THEN 'Interventional cardiology' WHEN 'C5' THEN 'Dentist (eff. 7/2016)' END::ENUM ('Carrierwide', 'General practice', 'General surgery', 'Allergy/immunology', 'Otolaryngology', 'Anesthesiology', 'Cardiology', 'Dermatology', 'Family practice', 'Interventional pain management (IPM) (eff. 4/1/03)', 'Gastroenterology', 'Internal medicine', 'Osteopathic manipulative therapy', 'Neurology', 'Neurosurgery', 'Speech/language pathology', 'Obstetrics/gynecology', 'Hospice and palliative care', 'Ophthalmology', 'Oral surgery (dentists only)', 'Orthopedic surgery', 'Cardiac electrophysiology', 'Pathology', 'Plastic and reconstructive surgery', 'Physical medicine and rehabilitation', 'Psychiatry', 'General psychiatry', 'Colorectal surgery (formerly proctology)', 'Pulmonary disease', 'Diagnostic radiology', 'Intensive cardiac rehabilitation', 'Anesthesiologist assistants (eff. 4/1/03—previously
grouped with certified registered nurse anesthetists
[CRNAs] )', 'Thoracic surgery', 'Urology', 'Chiropractic', 'Nuclear medicine', 'Pediatric medicine', 'Geriatric medicine', 'Nephrology', 'Hand surgery', 'Optometrist', 'Certified nurse midwife', 'CRNA (anesthesiologist assistants were removed from this
specialty 4/1/03)', 'Infectious disease', 'Mammography screening center', 'Endocrinology', 'Independent diagnostic testing facility (IDTF)', 'Podiatry', 'Ambulatory surgical center (formerly miscellaneous)', 'Nurse practitioner', 'Medical supply company with certified orthotist (certified by
American Board for Certification in Prosthetics and
Orthotics)', 'Medical supply company with certified prosthetist (certified
by American Board for Certification in Prosthetics and
Orthotics)', 'Medical supply company with certified prosthetist-orthotist
(certified by American Board for Certification in Prosthetics
and Orthotics)', 'Medical supply company for durable medical equipment
regional carrier (DMERC) (and not included in 51–53)', 'Individual certified orthotist', 'Individual certified prosthetist', 'Individual certified prosthetist-orthotist', 'Medical supply company with registered pharmacist', 'Ambulance service supplier, (e.g., private ambulance
companies, funeral homes)', 'Public health or welfare agencies (federal, state, and local)', 'Voluntary health or charitable agencies (e.g., National
Cancer Society, National Heart Association, Catholic
Charities)', 'Psychologist (billing independently)', 'Portable x ray supplier', 'Audiologist (billing independently)', 'Physical therapist (private practice added 4/1/03)
(independently practicing removed 4/1/03)', 'Rheumatology', 'Occupational therapist (private practice added 4/1/03)
(independently practicing removed 4/1/03)', 'Clinical psychologist', 'Clinical laboratory (billing independently)', 'Multispecialty clinic or group practice', 'Registered dietitian/nutrition professional (eff. 1/1/02)', 'Pain management (eff. 1/1/02)', 'Mass immunization roster biller', 'Radiation therapy centers (prior to 4/2003, this
include d IDTFs)', 'Slide preparation facilities (added to differentiate them from
IDTFs—eff. 4/1/03)', 'Peripheral vascular disease', 'Vascular surgery', 'Cardiac surgery', 'Addiction medicine', 'Licensed clinical social worker', 'Critical care (intensivists)', 'Hematology', 'Hematology/oncology', 'Preventive medicine', 'Maxillofacial surgery', 'Neuropsychiatry', 'All other suppliers (e.g., drug and department stores)', 'Unknown supplier/provider specialty', 'Certified clinical nurse specialist', 'Medical oncology', 'Surgical oncology', 'Radiation oncology', 'Emergency medicine', 'Interventional radiology', 'Competitive Acquisition Program (CAP) vendor (eff.
07/01/06). Prior to 07/01/06, known as independent
physiological laboratory', 'Optician', 'Physician assistant', 'Gynecologist/oncologist', 'Unknown physician specialty', 'Hospital (DMERCs only)', 'SNF (DMERCs only)', 'Intermediate care nursing facility (DMERCs only)', 'Nursing facility, other (DMERCs only)', 'Home health agency (DMERCs only)', 'Pharmacy (DMERC)', 'Medical supply company with respiratory therapist
(DMERCs only)', 'Department store (DMERC)', 'Grocery store (DMERC)', 'Indian Health Service (IHS), tribe, and tribal organizations
(non-hospital or non-hospital-based facilities, eff. 1/2005)', 'Supplier of oxygen and/or oxygen-related equipment (eff.
10/2/07)', 'Pedorthic personnel (eff. 10/2/07)', 'Medical supply company with pedorthic personnel (eff.
10/2/07)', 'Does not meet definition of healthcare provider (e.g.,
rehabilitation agency, organ procurement organization,
histocompatibility lab) (eff. 10/2/07)', 'Ocularist', 'Sleep medicine', 'Centralized flu', 'Indirect payment procedure', 'Interventional cardiology', 'Dentist (eff. 7/2016)') AS "Attending physician
specialty",
    CASE SRVC_BEG_DATE WHEN '.' THEN 'Missing' END::ENUM ('Missing') AS "Service begin
date",
    CASE SRVC_END_DATE WHEN '.' THEN 'Missing' END::ENUM ('Missing') AS "Service end
date",
    CASE LOS WHEN '.' THEN 'Missing' END::ENUM ('Missing') AS "Length of
stay",
    CASE ADMSN_TYPE WHEN '1' THEN 'Emergency: The patient requires immediate medical intervention
as a result of severe, life-threatening, or potentially disabling
conditions. Generally, the patient is admitted through the
emergency room.' WHEN '2' THEN 'Urgent: The patient requires immediate attention for the care and
treatment of a physical or mental disorder. Generally, the patient
is admitted to the first available and suitable accommodation.' WHEN '3' THEN 'Elective: The patient’s condition permits adequate time to
schedule the availability of a suitable accommodation.' WHEN '5' THEN 'Trauma: The patient visits a trauma center. (A trauma center
means a facility licensed or designated by the state or local
government authority authorized to do so, or as verified by the
American College of Surgeons and involving a trauma
activation.)' WHEN 'Blank' THEN 'Missing' END::ENUM ('Emergency: The patient requires immediate medical intervention
as a result of severe, life-threatening, or potentially disabling
conditions. Generally, the patient is admitted through the
emergency room.', 'Urgent: The patient requires immediate attention for the care and
treatment of a physical or mental disorder. Generally, the patient
is admitted to the first available and suitable accommodation.', 'Elective: The patient’s condition permits adequate time to
schedule the availability of a suitable accommodation.', 'Trauma: The patient visits a trauma center. (A trauma center
means a facility licensed or designated by the state or local
government authority authorized to do so, or as verified by the
American College of Surgeons and involving a trauma
activation.)', 'Missing') AS "Admission type",
    CASE TOB_CD WHEN '011' THEN 'Hospital inpatient' WHEN '013' THEN 'Hospital outpatient' WHEN '041' THEN 'Religious nonmedical hospital i npatient' WHEN '083' THEN 'Special facility or hospital ambulatory surgery
center (ASC) surgery—outpatient' WHEN '085' THEN 'Critical access hospital' WHEN 'Blank' THEN 'Missing' END::ENUM ('Hospital inpatient', 'Hospital outpatient', 'Religious nonmedical hospital i npatient', 'Special facility or hospital ambulatory surgery
center (ASC) surgery—outpatient', 'Critical access hospital', 'Missing') AS "Type of bill
code",
    CASE CLM_TYPE_CD WHEN 'OP' THEN 'Outpatient claim' WHEN 'IP' THEN 'Inpatient claim' WHEN 'ED' THEN 'Emergency department
claim' WHEN 'Blank' THEN 'Missing' END::ENUM ('Outpatient claim', 'Inpatient claim', 'Emergency department
claim', 'Missing') AS "Claim Type
Code",
    DSCHRG_STUS::VARCHAR AS DSCHRG_STUS,
    CASE PRMRY_DX_IMPUTED WHEN '0' THEN 'Not imputed' WHEN '1' THEN 'Imputed' WHEN '.' THEN 'No DX Codes' END::ENUM ('Not imputed', 'Imputed', 'No DX Codes') AS "Primary Diagnosis Code
(ICD-10) Imputed Flag",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE PRMRY_DX_CD and ICD_DX_CD_1–
ICD_DX_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-CM diagnosis
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE ICD_PRCDR_CD_1–
ICD_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "ICD-10-PCS procedure
code",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    CASE CPT_PRCDR_CD_1–
CPT_PRCDR_CD WHEN 'Blank' THEN 'Missing' END::ENUM ('Missing') AS "CPT procedure code 1-35",
    replace(replace(PLAN_PMT_AMT, '$', ''), ',', '')::FLOAT AS PLAN_PMT_AMT,
    replace(replace(TOT_CHRG_AMT, '$', ''), ',', '')::FLOAT AS TOT_CHRG_AMT
FROM read_csv('~/data/syh_dr/syhdr_medicaid_inpatient_2016.CSV', header=True, null_padding=true)