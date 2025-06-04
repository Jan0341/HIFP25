# 1. Import Libraries
import pandas as pd
import numpy as np
import dask.dataframe as dd
import warnings

# 2. Utilities
from s3_utils import save_dask_to_s3

# 3. Settings - Dont stop executing the file due to a warning
warnings.filterwarnings("ignore")

# 4. Dictionary for the .csv files to load them into the Functions
dic_path={
    'train_in': "s3://medicare-fraud-data-25-05-2025/raw/Train_Inpatientdata-*.csv",
    'train_out': 's3://medicare-fraud-data-25-05-2025/raw/Train_Outpatientdata-*.csv',
    'train_bene': "s3://medicare-fraud-data-25-05-2025/raw/Train_Beneficiarydata-*.csv",
    'train_labels': "s3://medicare-fraud-data-25-05-2025/raw/Train-*.csv",
    'test_in' : "s3://medicare-fraud-data-25-05-2025/raw/Test_Inpatientdata-*.csv",
    'test_out': "s3://medicare-fraud-data-25-05-2025/raw/Test_Outpatientdata-*.csv",
    'test_bene': "s3://medicare-fraud-data-25-05-2025/raw/Test_Beneficiarydata-*.csv",
    'test_labels': "s3://medicare-fraud-data-25-05-2025/raw/Test-*.csv"
}

# 5. Defining the "Clean_inpatient"-Function, to clean and transform the OG-Datasets Train_Inpatient and Test_Inpatient
def clean_inpatient(path):
    # Dtypes for inpatient data
    inpatient_dtypes = {
        'ClaimID': 'object',
        'ClaimStartDt': 'object',
        'ClaimEndDt': 'object',
        'Provider': 'object',
        'InscClaimAmtReimbursed': 'float64',
        'AttendingPhysician': 'object',
        'OperatingPhysician': 'object',
        'OtherPhysician': 'object',
        'AdmissionDt': 'object',
        'ClmAdmitDiagnosisCode': 'object',
        'DeductibleAmtPaid': 'float64',
        'DischargeDt': 'object',
        'ClmDiagnosisCode_1': 'object',
        'ClmDiagnosisCode_2': 'object',
        'ClmDiagnosisCode_3': 'object',
        'ClmDiagnosisCode_4': 'object',
        'ClmDiagnosisCode_5': 'object',
        'ClmDiagnosisCode_6': 'object',
        'ClmDiagnosisCode_7': 'object',
        'ClmDiagnosisCode_8': 'object',
        'ClmDiagnosisCode_9': 'object',
        'ClmDiagnosisCode_10': 'object',
        'DiagnosisGroupCode': 'object'
    }
    date_columns = ['ClaimStartDt', 'ClaimEndDt', 'AdmissionDt', 'DischargeDt']
    # Read CSV via Dask
    df = dd.read_csv(
        path,
        dtype=inpatient_dtypes,
        parse_dates=date_columns,
        assume_missing=True
    )
    # Delete lines with missing provider or refund amount
    df = df.dropna(subset=['Provider', 'InscClaimAmtReimbursed'])
    # Copy for Edit
    df_copy = df.copy()
    # Date conversion
    df_copy["ClaimStartDt"] = dd.to_datetime(df_copy["ClaimStartDt"], errors="coerce")
    df_copy["ClaimEndDt"] = dd.to_datetime(df_copy["ClaimEndDt"], errors="coerce")
    df_copy["AdmissionDt"] = dd.to_datetime(df_copy["AdmissionDt"], errors="coerce")
    df_copy["DischargeDt"] = dd.to_datetime(df_copy["DischargeDt"], errors="coerce")
    # Make new columns
    df_copy["ClaimDuration"] = (df_copy["ClaimEndDt"] - df_copy["ClaimStartDt"]).dt.days
    df_copy["HospitalDuration"] = (df_copy["DischargeDt"] - df_copy["AdmissionDt"]).dt.days
    # Reorder columns
    cols = list(df_copy.columns)
    # Safely reorder ClaimDuration after ClaimEndDt
    if 'ClaimDuration' in cols:
        cols.remove('ClaimDuration')
        idx = cols.index('ClaimEndDt')
        cols.insert(idx + 1, 'ClaimDuration')
    # Move AdmissionDt, DischargeDt, HospitalDuration together after ClaimDuration
    for col in ['AdmissionDt', 'DischargeDt', 'HospitalDuration']:
        if col in cols:
            cols.remove(col)
    idx = cols.index('ClaimDuration')
    cols[idx + 1:idx + 1] = ['AdmissionDt', 'DischargeDt', 'HospitalDuration']
    # Apply new column order
    df_copy = df_copy[cols]
    # Return
    return df_copy


# 6. Defining the "clean_outpatient"-Function, to clean and transform the OG-Datasets Test_Inpatient and Train_Inpatient
def clean_outpatient(path):
    outpatient_dtypes = {
    'ClaimID': 'object',
    'ClaimStartDt': 'object',
    'ClaimEndDt': 'object',
    'Provider' : 'object',
    'InscClaimAmtReimbursed' : 'float64',
    'AttendingPhysician' :'object',
    'OperatingPhysician' :'object',
    'OtherPhysician' :'object',
    #'AdmissionDt'  :'object', because not outpatient dataset
    'ClmAdmitDiagnosisCode' :'object',
    'DeductibleAmtPaid' :'float64',
    #'DischargeDt' :'object', because not outpatient dataset
    'ClmAdmitDiagnosisCode': 'object',
    'ClmDiagnosisCode_1': 'object',
    'ClmDiagnosisCode_2': 'object',
    'ClmDiagnosisCode_3': 'object',
    'ClmDiagnosisCode_4': 'object',
    'ClmDiagnosisCode_5': 'object',
    'ClmDiagnosisCode_6': 'object',
    'ClmDiagnosisCode_7': 'object',
    'ClmDiagnosisCode_8': 'object',
    'ClmDiagnosisCode_9': 'object',
    'ClmDiagnosisCode_10': 'object',
    'DeductibleAmtPaid': 'float64',  # Keeping as float64 as inferred, even if int64 was expected by Dask
    #'DiagnosisGroupCode': 'object' because not outpatient dataset
    }
    date_columns = ['ClaimStartDt', 'ClaimEndDt']

    df = dd.read_csv(
    path,
    dtype=outpatient_dtypes,
    parse_dates=date_columns,
    assume_missing=True  # optional, falls NaNs in int-Spalten möglich sind
    )
# Copy of the Datasets
    df_copy = df.copy()
    # 1. Check/Transform if ClaimStartDt and ClaimEndDt are datetimes
    df_copy["ClaimStartDt"] = dd.to_datetime(df_copy["ClaimStartDt"], errors="coerce")
    df_copy["ClaimEndDt"] = dd.to_datetime(df_copy["ClaimEndDt"], errors="coerce")
    # 2. Make a new columns for the duration of the Claim
    df_copy["ClaimDuration"] = (df_copy["ClaimEndDt"] - df_copy["ClaimStartDt"]).dt.days
    # 3. NO AdmissionDt and DischargeDT in this Dataset

    return df_copy


# 7. Defining the "clean_bene"-Function, to clean and transform the OG-Datasets Test_Bene and Train_Bene
def clean_bene(path):
    df = dd.read_csv(path)
    df_copy=df.copy()
# 1. New Values for Gender 0 = Female, 1 = Male
    df_copy["Gender"] = df_copy["Gender"].replace(2, 0)
# 2. Drop columns PartAVCov and PartBCov
    df_copy = df_copy.drop(["NoOfMonths_PartACov", "NoOfMonths_PartBCov"], axis=1)
# 3. RenalDiseaseIndicator: "Y" → 1, rest → 0
    df_copy["RenalDiseaseIndicator"] = df_copy["RenalDiseaseIndicator"].replace("Y", 1)
    df_copy["RenalDiseaseIndicator"] = df_copy["RenalDiseaseIndicator"].fillna(0).astype("int64")
# 4. Changing values in Chronical Diseases from 2 => 0 
    chronic_cols = [
    "ChronicCond_Alzheimer",
    "ChronicCond_Heartfailure",
    "ChronicCond_KidneyDisease",
    "ChronicCond_Cancer",
    "ChronicCond_ObstrPulmonary",
    "ChronicCond_Depression",
    "ChronicCond_Diabetes",
    "ChronicCond_IschemicHeart",
    "ChronicCond_Osteoporasis",
    "ChronicCond_rheumatoidarthritis",
    "ChronicCond_stroke"
    ]
    df_copy[chronic_cols] = df_copy[chronic_cols].replace(2, 0)
# 5. Check/Transform Datetims for DOB and DOD
    df_copy["DOB"] = dd.to_datetime(df_copy["DOB"], errors='coerce')
    df_copy["DOD"] = dd.to_datetime(df_copy["DOD"], errors='coerce')
# 6. Setting NaN in DoD to the Date of the end of the survey 2009-12-01:
    default_dod = pd.to_datetime("2009-12-01")
    df_copy["DOD"] = df_copy["DOD"].fillna(default_dod)
# 7. Creating a column which contains the age of the beneficiary by using DoB and DoD
    df_copy["Bene_Age"] = ((df_copy["DOD"] - df_copy["DOB"]).dt.days // 365)
# 8. Creating a column which contains the information if the beneficiary is alive (1) or dead (0)
    df_copy["Bene_Alive"] = (df_copy["DOD"] == default_dod).astype("int64")
# 9. Reorder columns: Putting Bene_Age and Bene_Alive between DOD and Gender
    cols = list(df_copy.columns)
    for col in ["Bene_Age", "Bene_Alive"]:
        if col in cols:
            cols.remove(col)
    idx = cols.index("DOD")
    cols[idx + 1:idx + 1] = ["Bene_Age", "Bene_Alive"]
    df_copy = df_copy[cols]
    
    return df_copy

#8. Defining the Clean label Function
def clean_label(path):
    """
    Loads the label CSV as a Dask DataFrame and maps 'Yes' to 1, 'No' to 0 in 'PotentialFraud' column if it exists.
    """
    df = dd.read_csv(path).copy()

    # Strip whitespace from column names just in case
    df.columns = df.columns.str.strip()

    # Check and map only if 'PotentialFraud' column exists
    if "PotentialFraud" in df.columns:
        df["PotentialFraud"] = df["PotentialFraud"].map({"Yes": 1, "No": 0})
    else:
        print("⚠️ Warning: 'PotentialFraud' column not found — no mapping applied.")

    return df


# 8. Saving the Dataframes
# Saving_paths is the paths for the cleaned and transformed Datasets
saving_paths =  "s3://medicare-fraud-data-25-05-2025/clean/"

df_train_in=clean_inpatient(dic_path['train_in'])

df_train_out=clean_outpatient(dic_path['train_out'])

df_train_bene=clean_bene(dic_path['train_bene'])

df_train_labels=clean_label(dic_path['train_labels'])

df_test_in=clean_inpatient(dic_path['test_in'])

df_test_out=clean_outpatient(dic_path['test_out'])

df_test_bene=clean_bene(dic_path['test_bene'])

df_test_labels=clean_label(dic_path['test_labels'])

df_train_in.to_csv(saving_paths + "train_inpatient/*.csv", index=False)
df_train_out.to_csv(saving_paths + "train_outpatient/*.csv", index=False)
df_train_bene.to_csv(saving_paths + "train_beneficiary/*.csv", index=False)
df_train_labels.to_csv(saving_paths + "train_labels/*.csv", index=False)
df_test_in.to_csv(saving_paths + "test_inpatient/*.csv", index=False)
df_test_out.to_csv(saving_paths + "test_outpatient/*.csv", index=False)
df_test_bene.to_csv(saving_paths + "test_beneficiary/*.csv", index=False)
df_test_labels.to_csv(saving_paths + "test_labels/*.csv", index=False)


print("✅ All datasets cleaned and saved to S3 successfully!")




