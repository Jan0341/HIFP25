# 1. Import Libraries
from s3_utils import save_dask_to_s3, load_clean_data
import dask.dataframe as dd

# 2. Loading clean Data from S3 
(df_train_in, df_train_out, df_test_in, df_test_out, df_train_bene, df_test_bene, df_train_labels, df_test_labels) = load_clean_data()

# 3. Merging Data
def merge_all_data(df_train_in, df_train_out, df_train_bene, df_train_labels):
    """
    Merge all data from inpatient, outpatient, beneficiary, and labels.
    
    Returns:
        Dask DataFrame: Merged DataFrame containing all relevant data.
    """
    
    
    # Step 1: Identify common columns between inpatient and outpatient
    common_cols = [col for col in df_train_in.columns if col in df_train_out.columns]
    print(f"✅ Common columns: {len(common_cols)}")
    
    # Step 2: Outer merge inpatient + outpatient on shared columns
    df_train_in_out = dd.merge(
        df_train_in,
        df_train_out,
        how='outer',
        on=common_cols
    )
    print("✅ Merged inpatient + outpatient shape (approx):")
    print(df_train_in_out.shape)
    
    # Step 3: Inner merge with beneficiary data on 'BeneID'
    df_train_in_out_bene = dd.merge(
        df_train_in_out,
        df_train_bene,
        how='inner',
        on='BeneID'
    )
    print("✅ Final merged shape with beneficiary info (approx):")
    print(df_train_in_out_bene.shape)
    
    # Merge with labels
    df_train_full = dd.merge(
        df_train_in_out_bene,
        df_train_labels,
        how='inner',
        on='Provider'  # assuming this is the key for labels
    )
    print("✅ Final merged shape with labels (approx):")
    print(df_train_full.shape)
    
    return df_train_full

# 4. Merging and Saving Data
df_train_full = merge_all_data(df_train_in, df_train_out, df_train_bene, df_train_labels)
df_test_full = merge_all_data(df_test_in, df_test_out, df_test_bene, df_test_labels)
print("✅ Merging completed. Now saving to S3...")
save_dask_to_s3(df_train_full,
                 "s3://medicare-fraud-data-25-05-2025/clean/train_full/")
print("✅ Train data saved successfully.")
save_dask_to_s3(df_test_full,
                 "s3://medicare-fraud-data-25-05-2025/clean/test_full/")
print("✅ Merging and saving completed successfully.")
                 