import dask.dataframe as dd
# 2. Define the function to load merged data
def load_merged_data():
    """
    Load all merged data from S3.
    """
    merged_dtypes = {
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
    'IPAnnualReimbursementAmt': 'float64',
    'OPAnnualReimbursementAmt': 'float64',
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
    'DiagnosisGroupCode': 'object',
    'IPAnnualDeductibleAmt': 'float64',
    'OPAnnualDeductibleAmt': 'float64',
    }
    #date_columns_in = ['ClaimStartDt', 'ClaimEndDt', 'AdmissionDt', 'DischargeDt']
    clean_path = "s3://medicare-fraud-data-25-05-2025/clean/"
    df_train = dd.read_csv(clean_path+"train_full/*.csv", dtype=merged_dtypes)
    df_test = dd.read_csv(clean_path+"test_full/*.csv", dtype=merged_dtypes)
    print("Data loaded successfully")
    
    return (df_train, df_test)

# 3. Call the function to load data
df_train, df_test = load_merged_data()


# dictionary to hold physician columns
physician_cols = [col for col in df_test.columns if "Physician" in col]
print(physician_cols)


# 1. Replacing NANs in all Physician Columns by Zero
cols_to_fill = ['AttendingPhysician', 'OperatingPhysician', 'OtherPhysician']
df_test[cols_to_fill] = df_test[cols_to_fill].fillna(0)
df_train[cols_to_fill] = df_train[cols_to_fill].fillna(0)


# 2. Sum of the Beneficiary Age for every Provider
prv_bene_age_sum = df_test.groupby("Provider")["Bene_Age"].sum().reset_index()
prv_bene_age_sum = prv_bene_age_sum.rename(columns={"Bene_Age": "Bene_Age_Sum"})


# 3. Number of Total Claims per Provider. The original Idea was to identify the Total Number of false Claims by a Provider. For that he subtract the number of fradulent claims from the number of total claims
prv_total_claims = df_test.groupby("Provider")["ClaimID"].count().reset_index()
prv_total_claims.columns = ["Provider", "TotalClaims"]


# 4. Define a function to compute total claims per provider for the attending physician
def prv_total_claims_for_physicians(df):
    """
    Compute total claims per provider for each physician type, and return one merged Dask DataFrame.
    """

    # Count total claims per provider-physician type
    att = df.groupby(["Provider", "AttendingPhysician"])["ClaimID"].count().reset_index()
    att = att.rename(columns={"ClaimID": "AttendingPhysician_TotalClaims"})

    #op = df.groupby(["Provider", "OperatingPhysician"])["ClaimID"].count().reset_index()
    #op = op.rename(columns={"ClaimID": "OperatingPhysician_TotalClaims"})
#
    #ot = df.groupby(["Provider", "OtherPhysician"])["ClaimID"].count().reset_index()
    #ot = ot.rename(columns={"ClaimID": "OtherPhysician_TotalClaims"})
#
    ## Now reduce these to provider-level totals by summing claims per provider
    att_sum = att.groupby("Provider")["AttendingPhysician_TotalClaims"].sum().reset_index()
    #op_sum = op.groupby("Provider")["OperatingPhysician_TotalClaims"].sum().reset_index()
    #ot_sum = ot.groupby("Provider")["OtherPhysician_TotalClaims"].sum().reset_index()

    # Merge safely
    #merged = att_sum.merge(op_sum, on="Provider", how="outer")
    #merged = merged.merge(ot_sum, on="Provider", how="outer")

    return att_sum

merged=prv_total_claims_for_physicians(df_test)

# 7. Prv_Physician_Count
def prv_physician_count(df, physician_col):
    """
    Count unique physicians for each provider.
    If multiple columns are provided, all unique physician IDs across them are counted.
    Works with Dask DataFrames.
    """
    if isinstance(physician_col, list):
        # Combine provider with all physician columns, then reshape and deduplicate
        dfs = []
        for col in physician_col:
            temp = df[["Provider", col]].rename(columns={col: "Physician"}).dropna()
            dfs.append(temp)
        
        combined = dd.concat(dfs)
        unique_counts = (
            combined.dropna()
            .drop_duplicates()
            .groupby("Provider")["Physician"]
            .nunique()
            .reset_index()
        )
        unique_counts = unique_counts.rename(columns={"Physician": "Prv_Physician_Count"})

    else:
        unique_counts = (
            df.groupby("Provider")[physician_col]
            .nunique()
            .reset_index()
            .rename(columns={physician_col: f"{physician_col}_Count"})
        )

    return unique_counts

prv_Attphysician_count = prv_physician_count(df_test, "AttendingPhysician")
prv_OPphysician_count = prv_physician_count(df_test, "OperatingPhysician")
prv_Otphysician_count = prv_physician_count(df_test, "OtherPhysician")
prv_Allphysician_count = prv_physician_count(df_test, ["AttendingPhysician", "OperatingPhysician", "OtherPhysician"])


# 10. Provider_Insurance_Clam_Reimbursement_Amt
def prv_insc_claim_reimb_amt(df):
    """
    Calculate the total insurance reimbursement amount per provider.
    """
    return df.groupby("Provider")["InscClaimAmtReimbursed"].sum().reset_index().rename(
        columns={"InscClaimAmtReimbursed": "Provider_Insurance_Claim_Reimbursement_Amt"}
    )
provider_insurance_reimbursement = prv_insc_claim_reimb_amt(df_test)
provider_insurance_reimbursement.head()


# 11. Provider_Total_Bene
def prv_total_bene(df):
    """
    Calculate the total number of unique beneficiaries per provider.
    """
    return df.groupby("Provider")["BeneID"].nunique().reset_index().rename(
        columns={"BeneID": "Provider_Total_Patients"}
    )
provider_total_patients = prv_total_bene(df_test)
provider_total_patients



# 12. Provider_Total_Chronic_Beneficiaries

def prv_total_chron_bene(df, chronic_cols):
    """
    Calculates the total number of beneficiaries per provider for each chronic condition.

    Parameters:
        df (Dask or Pandas DataFrame): Input beneficiary DataFrame
        chronic_cols (list of str): List of chronic condition columns (values should be 0 or 1)

    Returns:
        DataFrame with one row per provider and total counts of each chronic condition.
    """
    # Check if all columns exist
    missing = [col for col in chronic_cols if col not in df.columns]
    if missing:
        raise ValueError(f"The following columns are missing: {missing}")
    
    # Group and sum per provider
    agg_df = df.groupby("Provider")[chronic_cols].sum().reset_index()

    # Rename columns
    agg_df = agg_df.rename(columns={col: f"Provider_Total_{col}_Patients" for col in chronic_cols})

    return agg_df
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

provider_total_chronic_patients = prv_total_chron_bene(df_test, chronic_cols)
provider_total_chronic_patients


# 14. count of diagnosis for every Provider
import dask.dataframe as dd

def prv_diagnosis_count(df, diagnosis_cols):
    """
    Count non-null occurrences of the ClmDiagnosisCode 1-3 per provider.
    
    Parameters:
        df (Dask DataFrame): Input DataFrame containing diagnosis codes
        diagnosis_cols (list of str): List of diagnosis code columns
    
    Returns:
        Dask DataFrame with counts of each diagnosis column per provider
    """
    # Start with the first column's counts
    result = df.groupby("Provider")[diagnosis_cols[0]].count().reset_index().rename(
        columns={diagnosis_cols[0]: f"{diagnosis_cols[0]}_Count"}
    )
    
    # Iterate through remaining diagnosis columns and join counts
    for col in diagnosis_cols[1:]:
        temp = df.groupby("Provider")[col].count().reset_index().rename(
            columns={col: f"{col}_Count"}
        )
        result = result.merge(temp, on="Provider", how="outer")

    return result
diagnosis_cols = [
    "ClmAdmitDiagnosisCode",
    "ClmDiagnosisCode_1",
    "ClmDiagnosisCode_2",
    "ClmDiagnosisCode_3"
]
diagnosis_counts_per_provider = prv_diagnosis_count(df_test, diagnosis_cols)
diagnosis_counts_per_provider

# 18. Most frequent Claimcodes for every Provider
from functools import reduce
import dask.dataframe as dd

def prv_most_frequent_claim_codes(df, claim_code_cols):
    """
    Find the most frequent claim code for each provider across multiple columns.
    
    Parameters:
        df (Dask DataFrame): Input DataFrame containing claim codes
        claim_code_cols (list of str): List of claim code column names
    
    Returns:
        Dask DataFrame: Each row contains Provider and the most frequent code per claim column
    """
    results = []

    for col in claim_code_cols:
        # Count frequencies per Provider per code
        code_counts = (
            df.groupby(["Provider", col])
            .size()
            .reset_index()
            .rename(columns={0: "Count"})
        )

        # Sort within each partition, then drop duplicates to get most frequent
        most_frequent = (
            code_counts.map_partitions(lambda pdf: pdf.sort_values("Count", ascending=False))
            .drop_duplicates(subset="Provider")
            .rename(columns={col: f"{col}_Most_Frequent"})
            .drop(columns=["Count"])
        )

        results.append(most_frequent)

    # Merge all the most frequent codes per column
    final_result = reduce(lambda left, right: left.merge(right, on="Provider", how="outer"), results)

    return final_result

claim_code_cols = [
    "ClmAdmitDiagnosisCode",
    "ClmDiagnosisCode_1",
    "ClmDiagnosisCode_2",
    "ClmDiagnosisCode_3",
    
]
most_frequent_codes = prv_most_frequent_claim_codes(df_test, claim_code_cols)
most_frequent_codes

from functools import reduce

def prv_most_frequent_physicians(df, physician_cols):
    """
    Find the most frequent physician for each provider across multiple physician columns.
    
    Parameters:
        df (Dask DataFrame): Input DataFrame containing provider and physician columns
        physician_cols (list of str): List of physician column names
    
    Returns:
        Dask DataFrame: Each row contains Provider and the most frequent physician per column
    """
    results = []

    for col in physician_cols:
        # Count frequencies per Provider per Physician
        physician_counts = (
            df.groupby(["Provider", col])
            .size()
            .reset_index()
            .rename(columns={0: "Count"})
        )

        # Sort by frequency, then get most frequent physician per provider
        most_frequent = (
            physician_counts.map_partitions(lambda pdf: pdf.sort_values("Count", ascending=False))
            .drop_duplicates(subset="Provider")
            .rename(columns={col: f"{col}_Most_Frequent"})
            .drop(columns=["Count"])
        )

        results.append(most_frequent)

    # Merge all the most frequent physician columns on Provider
    final_df = reduce(lambda left, right: left.merge(right, on="Provider", how="outer"), results)

    return final_df
physician_cols = [
    "AttendingPhysician",
    "OperatingPhysician",
    "OtherPhysician"
]
most_frequent_physicians_df = prv_most_frequent_physicians(df_test, physician_cols)
most_frequent_physicians_df


# 16. bene deductible and claimcost amount
def calculate_bene_amount(df):
    """
    Return a Dask DataFrame with BeneID, AllocatedAmount (as-is), and summed Deductible & Reimbursed amounts.

    Parameters:
        df (Dask DataFrame): Input with reimbursement and deductible fields

    Returns:
        Dask DataFrame with columns: BeneID, AllocatedAmount, DeductibleAmtPaid (sum), InscClaimAmtReimbursed (sum)
    """
    

    # Calculate AllocatedAmount (not to be summed)
    df["AllocatedAmount"] = df["IPAnnualReimbursementAmt"] + df["OPAnnualReimbursementAmt"]

    # Get first AllocatedAmount per BeneID (assuming same for all rows of that BeneID)
    allocated = df[["BeneID", "AllocatedAmount"]].drop_duplicates(subset="BeneID")

    # Sum the other columns per BeneID
    summed = df.groupby("BeneID")[["DeductibleAmtPaid", "InscClaimAmtReimbursed"]].sum().reset_index()

    # Merge
    result = allocated.merge(summed, on="BeneID", how="left")

    return result

bene_amount_df = calculate_bene_amount(df_test)

bene_amount_avg_prv_df= dd.merge(
    df_test[["BeneID", "Provider"]],
    bene_amount_df,
    on="BeneID",
    how="left"
).groupby("Provider")["AllocatedAmount", 'DeductibleAmtPaid', "InscClaimAmtReimbursed"].mean().reset_index()
bene_amount_avg_prv_df = bene_amount_avg_prv_df.rename(
    columns={"AllocatedAmount": "Avg_alocated_Amount_Per_Provider", 'DeductibleAmtPaid': "Avg_Deductible_Amt_Paid_Per_Provider", "InscClaimAmtReimbursed": "Avg_InscClaimAmtReimbursed_Per_Provider" }
)
bene_amount_avg_prv_df['per_utils_remaining_amount'] = (bene_amount_avg_prv_df["Avg_InscClaimAmtReimbursed_Per_Provider"] - bene_amount_avg_prv_df["Avg_Deductible_Amt_Paid_Per_Provider"]) /(bene_amount_avg_prv_df["Avg_alocated_Amount_Per_Provider"] - bene_amount_avg_prv_df["Avg_Deductible_Amt_Paid_Per_Provider"])
bene_amount_avg_prv_df.head(5)