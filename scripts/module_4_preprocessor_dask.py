import dask.dataframe as dd
import pandas as pd
import numpy as np
from dask_ml.preprocessing import StandardScaler

class MedicarePreprocessor:
    def __init__(self, df: dd.DataFrame):
        self.df = df

        self.cat_cols = [
            #"Provider",
            "Race_mode",
            
            
            #"PotentialFraud",
            "ClmAdmitDiagnosisCode_Most_Frequent",
            
            "ClmDiagnosisCode_2_Most_Frequent",
            "ClmDiagnosisCode_3_Most_Frequent",
            "AttendingPhysician_Most_Frequent",
            "OperatingPhysician_Most_Frequent",
            "OtherPhysician_Most_Frequent"
        ]

        self.num_cols = [
            col
            for col, dtype in df.dtypes.items()
            if (dtype == "float64" and col != "PotentialFraud" and col not in self.cat_cols)
        ]

        self.target_col = "PotentialFraud"

    def drop_unused_columns(self, drop_cols: list = None):
        if drop_cols is None:
            drop_cols = [
                "State_mode",
                "County_mode",
                "ClmAdmitDiagnosisCode_Most_Frequent",
                "ClmDiagnosisCode_2_Most_Frequent",
                "ClmDiagnosisCode_3_Most_Frequent",
                "AttendingPhysician_Most_Frequent",
                'ClmDiagnosisCode_1_Most_Frequent'
            ]
        to_drop = [c for c in drop_cols if c in self.df.columns]
        if to_drop:
            self.df = self.df.drop(columns=to_drop)
        self.cat_cols = [c for c in self.cat_cols if c not in to_drop]
        return self

    def fill_missing(self):
        # 1) Compute all numeric means in one shot
        means = self.df[self.num_cols].mean().compute()  # pandas.Series of means
        means_dict = {col: float(means[col]) for col in self.num_cols}

        # 2) Cast cat columns to string, then fill all missing with "MISSING"
        for col in self.cat_cols:
            self.df[col] = self.df[col].astype("string")

        fill_dict = means_dict.copy()
        for col in self.cat_cols:
            fill_dict[col] = "MISSING"

        # 3) One fillna call for everything:
        self.df = self.df.fillna(fill_dict)
        return self

    def encode_categoricals(self):
        # 1) Cast all cat columns to category dtype
        for col in self.cat_cols:
            self.df[col] = self.df[col].astype("category")

        # 2) One global categorize/shuffle
        self.df = self.df.categorize(columns=self.cat_cols)

        # 3) One pass to convert each cat to codes
        def _apply_cat_codes(pdf):
            pdf2 = pdf.copy()
            for c in self.cat_cols:
                pdf2[c] = pdf2[c].cat.codes.astype("int64")
            return pdf2

        self.df = self.df.map_partitions(_apply_cat_codes)
        return self



    def scale_numeric_features(self):
        exclude = {self.target_col, "Provider"}
        to_scale = [c for c in self.num_cols if c not in exclude]

        if to_scale:
            scaler = StandardScaler()
            scaled_df = scaler.fit_transform(self.df[to_scale])
            self.df = dd.concat(
                [self.df.drop(columns=to_scale), scaled_df],
                axis=1
            )
        return self

    def get_processed_df(self) -> dd.DataFrame:
        self.df = self.df.persist()
        return self.df
