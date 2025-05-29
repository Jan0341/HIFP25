# Medicare Claims Data – Dataset Documentation

## 📌 Introduction

Hello,

This README file explains the structure and variables of the datasets contained in this folder. Please note that **no official documentation** was provided with these datasets. The following descriptions are based on careful interpretation and analysis.

---

## 📁 Dataset Overview

There are a total of **eight datasets** included:

### 🔬 Test Datasets:
- `Test_Beneficiarydata-1542969243754.csv`
- `Test_Inpatientdata-1542969243754.csv`
- `Test_Outpatientdata-1542969243754.csv`
- `Test-1542969243754.csv`

### 🧪 Training Datasets:
- `Train_Beneficiarydata-1542865627584.csv`
- `Train_Inpatientdata-1542865627584.csv`
- `Train_Outpatientdata-1542865627584.csv`
- `Train-1542865627584.csv`

---

## 1️⃣ Train_Beneficiarydata-1542865627584.csv

This dataset includes demographic and insurance information about the beneficiary. It also contains:
- 11 variables describing chronic conditions,
- 1 variable indicating kidney disease history,
- 4 variables related to insurance reimbursements and deductibles.

### 🔍 Variable Descriptions

| Column | Description |
|--------|-------------|
| `BeneID` | Unique ID of the beneficiary |
| `DOB` | Date of Birth |
| `DOD` | Date of Death |
| `Gender` | Gender of the beneficiary |
| `Race` | Race or ethnicity |
| `State` | State of residence |
| `Country` | Country code (ISO format) |
| `NoOfMonths_PartACov` | Number of months with Medicare Part A coverage |
| `NoOfMonths_PartBCov` | Number of months with Medicare Part B coverage |
| `Renal Disease Indicator` | Indicates if the beneficiary had a long history of kidney disease when enrolling |
| `ChronicCond_Alzheimer` | Chronic condition: Alzheimer's disease |
| `ChronicCond_Heartfailure` | Chronic condition: Heart failure |
| `ChronicCond_KidneyDisease` | Chronic condition: Kidney disease |
| `ChronicCond_Cancer` | Chronic condition: Cancer |
| `ChronicCond_ObstrPulmonary` | Chronic condition: Obstructive pulmonary disease |
| `ChronicCond_Depression` | Chronic condition: Depression |
| `ChronicCond_Diabetes` | Chronic condition: Diabetes |
| `ChronicCond_IschemicHeart` | Chronic condition: Ischemic heart disease |
| `ChronicCond_Osteoporasis` | Chronic condition: Osteoporosis |
| `ChronicCond_rheumatoidarthritis` | Chronic condition: Rheumatoid arthritis |
| `ChronicCond_stroke` | Chronic condition: Stroke |
| `IPAnnualReimbursementAmt` | Annual inpatient reimbursement amount covered by insurance |
| `IPAnnualDeductibleAmt` | Annual deductible for inpatient services (out-of-pocket by beneficiary) |
| `OPAnnualReimbursementAmt` | Annual outpatient reimbursement amount covered by insurance |
| `OPAnnualDeductibleAmt` | Annual deductible for outpatient services (out-of-pocket by beneficiary) |

---

## 2️⃣ Train_Inpatientdata-1542865627584.csv

This dataset is similar to the outpatient data but includes additional variables that describe hospitalization details, such as admission and discharge dates. It contains claims for **inpatient** (hospitalized) services.

### 🔍 Variable Descriptions

| Column | Description |
|--------|-------------|
| `BeneID` | Unique ID of the beneficiary |
| `ClaimID` | Unique ID of the claim |
| `ClaimStartDt` | Start date of the claim |
| `ClaimEndDt` | End date of the claim |
| `Provider` | Unique ID of the healthcare provider |
| `InscClaimAmtReimbursed` | Amount reimbursed by insurance for the claim |
| `AttendingPhysician` | ID of the attending physician |
| `OperatingPhysician` | ID of the operating physician |
| `OtherPhysician` | ID of other physicians involved |
| `AdmissionDt` | Date of hospital admission |
| `ClmAdmitDiagnosisCode` | Primary diagnosis code at the time of admission |
| `DeductibleAmtPaid` | Deductible (co-payment) paid by the beneficiary |
| `DischargeDt` | Date of discharge from the hospital |
| `DiagnosisGroupCode` | Diagnosis group code (may group multiple conditions) |
| `ClmDiagnosisCode_1-10` | Diagnosis codes (up to 10 per claim) |
| `ClmProcedureCode_1` | Medical procedure code for treatments performed |

---

## 3️⃣ Train_Outpatientdata-1542865627584.csv

This dataset is nearly identical in structure to the inpatient dataset but contains claims related to **outpatient** services (where the patient was not admitted). It does not include variables related to hospital stays (e.g., admission or discharge dates).

### 🔍 Variable Descriptions

| Column | Description |
|--------|-------------|
| `BeneID` | Unique ID of the beneficiary |
| `ClaimID` | Unique ID of the claim |
| `ClaimStartDt` | Start date of the claim |
| `ClaimEndDt` | End date of the claim |
| `Provider` | Unique ID of the healthcare provider |
| `InscClaimAmtReimbursed` | Amount reimbursed by insurance |
| `AttendingPhysician` | ID of the attending physician |
| `OperatingPhysician` | ID of the operating physician |
| `OtherPhysician` | ID of other physicians involved |
| `ClmDiagnosisCode_1` to `_10` | Diagnosis codes related to the outpatient claim |
| `ClmProcedureCode_1` to `_6` | Medical procedure codes associated with the outpatient visit |
| `DeductibleAmtPaid` | Deductible (co-payment) paid by the beneficiary |
| `ClmAdmitDiagnosisCode` | Group diagnosis code (optional/may be blank) |

---

## 4️⃣ Train-1542865627584.csv

This final dataset contains:

| Column | Description |
|--------|-------------|
| `ProviderID` | Unique ID of the provider |
| `PotentialFraud` | Indicates whether the provider is suspected of fraud (`Yes` / `No`) |




