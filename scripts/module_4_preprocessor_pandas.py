# used with merged_ready files
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler

class MedicarePreprocessorPandas:
    def __init__(self):
        self.cat_cols = []
        self.num_cols = []
        self.target_col = "PotentialFraud"
        self.fill_values = {}
        self.scaler = None
        self.fitted_columns = []

    def fit(self, df: pd.DataFrame):
        self.cat_cols = [
            "Race_mode", "State_mode", "County_mode",
            "ClmAdmitDiagnosisCode_Most_Frequent",
            "ClmDiagnosisCode_1_Most_Frequent",
            "ClmDiagnosisCode_2_Most_Frequent",
            "ClmDiagnosisCode_3_Most_Frequent",
            "AttendingPhysician_Most_Frequent",
            "OperatingPhysician_Most_Frequent",
            "OtherPhysician_Most_Frequent"
        ]

        df = df.copy()

        # Create engineered features early
        df["avg_cost_per_claim"] = np.where(
            df["TotalClaims"] == 0,
            0.0,
            df["Provider_Insurance_Claim_Reimbursement_Amt"] / df["TotalClaims"]
        )
        df["perc_chronic_alz"] = np.where(
            df["Provider_Total_Patients"] == 0,
            0.0,
            df["Provider_Total_ChronicCond_Alzheimer_Patients"] / df["Provider_Total_Patients"]
        )

        self.num_cols = [
            col for col in df.select_dtypes(include=[np.number]).columns
            if col not in self.cat_cols and col != self.target_col
        ]

        # Save fill values
        self.fill_values = {col: df[col].mean() for col in self.num_cols}
        for col in self.cat_cols:
            self.fill_values[col] = "MISSING"

        # Fill missing and encode
        for col in self.cat_cols:
            df[col] = df[col].astype("category")
            if "MISSING" not in df[col].cat.categories:
                df[col] = df[col].cat.add_categories(["MISSING"])
            df[col] = df[col].fillna("MISSING")
            df[col] = df[col].cat.codes

        for col in self.num_cols:
            df[col] = df[col].fillna(self.fill_values[col])

        self.scaler = StandardScaler()
        self.scaler.fit(df[self.num_cols])

        self.fitted_columns = self.cat_cols + self.num_cols + ["Provider"]
        return self

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # Add missing columns from training
        for col in self.cat_cols + self.num_cols:
            if col not in df.columns:
                if col in self.cat_cols:
                    df[col] = "MISSING"
                else:
                    df[col] = self.fill_values.get(col, 0.0)

        # Feature engineering
        df["avg_cost_per_claim"] = np.where(
            df["TotalClaims"] == 0,
            0.0,
            df["Provider_Insurance_Claim_Reimbursement_Amt"] / df["TotalClaims"]
        )

        df["perc_chronic_alz"] = np.where(
            df["Provider_Total_Patients"] == 0,
            0.0,
            df["Provider_Total_ChronicCond_Alzheimer_Patients"] / df["Provider_Total_Patients"]
        )

        # Fill and encode categoricals
        for col in self.cat_cols:
            df[col] = df[col].astype("category")
            if "MISSING" not in df[col].cat.categories:
                df[col] = df[col].cat.add_categories(["MISSING"])
            df[col] = df[col].fillna("MISSING")
            df[col] = df[col].cat.codes

        for col in self.num_cols:
            df[col] = df[col].fillna(self.fill_values.get(col, 0.0))

        # Scale
        for col in self.num_cols:
            if col not in df.columns:
                df[col] = 0.0
        df[self.num_cols] = self.scaler.transform(df[self.num_cols])

        # Final columns
        final_cols = [col for col in self.fitted_columns if col in df.columns]
        df_out = df[final_cols].copy()

        if self.target_col in df.columns:
            df_out[self.target_col] = df[self.target_col]

        return df_out
