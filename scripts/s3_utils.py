# s3_utils.py
# 1. Import Librarys
import dask.dataframe as dd
# 2. Defining Save Function
def save_dask_to_s3(df, path, file_format="csv", single_file=False, index=False):
    """
    Save a Dask DataFrame to S3 in CSV or Parquet format.
    
    Parameters:
        df (dask.DataFrame): The Dask DataFrame to save
        path (str): S3 path (e.g. s3://bucket/folder/)
        file_format (str): 'csv' or 'parquet'
        single_file (bool): Save as single file (only for small data)
        index (bool): Whether to save the index
    """
    if file_format == "csv":
        if single_file:
            df.compute().to_csv(path, index=index)
        else:
            df.to_csv(path + "part-*.csv", index=index)
    elif file_format == "parquet":
        df.to_parquet(path, write_index=index)
    else:
        raise ValueError("Unsupported file_format: choose 'csv' or 'parquet'")


"""How to Use It in Your Project:

from s3_utils import save_dask_to_s3, dd.read_csv

# Save
save_dask_to_s3(df_train_in, clean_path"train_inpatient/", file_format="csv")

"""
# 3. Defining Function for cleaning loaded Data
def load_clean_data():
    """
    Load all clean data from S3.
    """
    inpatient_dtypes = {
    'ClaimID': 'object',
    'ClaimStartDt': 'object',
    'ClaimEndDt': 'object',
    'Provider' : 'object',
    'InscClaimAmtReimbursed' : 'float64',
    'AttendingPhysician' :'object',
    'OperatingPhysician' :'object',
    'OtherPhysician' :'object',
    'AdmissionDt'  :'object',
    'ClmAdmitDiagnosisCode' :'object',
    'DeductibleAmtPaid' :'float64',
    'DischargeDt' :'object',
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
    'DiagnosisGroupCode': 'object'
    }
    outpatient_dtypes = {
    'ClaimID': 'object',
    'ClaimStartDt': 'object',
    'ClaimEndDt': 'object',
    'Provider' : 'object',
    'InscClaimAmtReimbursed' : 'float64',
    'AttendingPhysician' :'object',
    'OperatingPhysician' :'object',
    'OtherPhysician' :'object',
    #'AdmissionDt'  :'object',
    'ClmAdmitDiagnosisCode' :'object',
    'DeductibleAmtPaid' :'float64',
    #'DischargeDt' :'object',
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
    'DeductibleAmtPaid': 'float64',  
    #'DiagnosisGroupCode': 'object'
    }
    date_columns_in = ['ClaimStartDt', 'ClaimEndDt', 'AdmissionDt', 'DischargeDt']
    date_columns_out = ['ClaimStartDt', 'ClaimEndDt']
    clean_path = "s3://medicare-fraud-data-25-05-2025/clean/"

    df_train_in = dd.read_csv(clean_path+"train_inpatient/*.csv",parse_dates=date_columns_in, dtype=inpatient_dtypes)
    df_train_out = dd.read_csv(clean_path+"train_outpatient/*.csv",parse_dates=date_columns_out, dtype=outpatient_dtypes)
    df_test_in = dd.read_csv(clean_path+"test_inpatient/*.csv",parse_dates=date_columns_in, dtype=inpatient_dtypes)
    df_test_out = dd.read_csv(clean_path+"test_outpatient/*.csv",parse_dates=date_columns_out, dtype=outpatient_dtypes)
    df_train_bene = dd.read_csv(clean_path+"train_beneficiary/*.csv")
    df_test_bene = dd.read_csv(clean_path+"test_beneficiary/*.csv")
    df_train_labels = dd.read_csv(clean_path+"train_labels/*.csv")
    df_test_labels = dd.read_csv(clean_path+"test_labels/*.csv")
    
    return (df_train_in, df_train_out, df_test_in, df_test_out, df_train_bene, df_test_bene, df_train_labels, df_test_labels)