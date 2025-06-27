import os
import mlflow
import mlflow.sklearn
import pandas as pd
import numpy as np
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import (
    recall_score, precision_score, f1_score,
    accuracy_score, roc_auc_score, confusion_matrix,
    classification_report
)
import matplotlib.pyplot as plt
import seaborn as sns


def train_and_log_model_with_mlflow(df_train_processed, threshold=0.05, experiment_name="Medicare_Fraud_Detection"):
    """
    Train a model and log metrics, parameters, and artifacts to MLflow.

    Parameters:
    - df_train_processed (pd.DataFrame): Processed training data.
    - threshold (float): Probability threshold for classification.
    - experiment_name (str): MLflow experiment name.

    Returns:
    - model: Trained XGBoost model.
    """

    # Set experiment
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run():
        # Features and target
        X = df_train_processed.drop(columns=["PotentialFraud"])
        y = df_train_processed["PotentialFraud"]

        # Split the data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, stratify=y, test_size=0.2, random_state=42
        )

        # Categorical casting for XGBoost
        X_train['Provider'] = X_train['Provider'].astype('category')
        X_test['Provider'] = X_test['Provider'].astype('category')

        # Initialize and train model
        model = XGBClassifier(
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            use_label_encoder=False,
            eval_metric='logloss',
            enable_categorical=True
        )

        model.fit(X_train, y_train)

        # Log parameters
        mlflow.log_params({
            "model_type": "XGBClassifier",
            "n_estimators": 100,
            "max_depth": 6,
            "learning_rate": 0.1,
            "threshold": threshold
        })

        # Predict probabilities and apply threshold
        y_prob = model.predict_proba(X_test)[:, 1]
        y_pred_thresh = (y_prob >= threshold).astype(int)

        # Compute metrics
        metrics = {
            "recall": recall_score(y_test, y_pred_thresh),
            "precision": precision_score(y_test, y_pred_thresh),
            "f1": f1_score(y_test, y_pred_thresh),
            "accuracy": accuracy_score(y_test, y_pred_thresh),
            "roc_auc": roc_auc_score(y_test, y_prob)
        }

        # Log metrics
        mlflow.log_metrics(metrics)

        # Log model
        mlflow.sklearn.log_model(model, "model")

        # Save and log confusion matrix
        cm = confusion_matrix(y_test, y_pred_thresh)
        plt.figure(figsize=(6, 4))
        sns.heatmap(cm, annot=True, fmt="d", cmap="Blues")
        plt.title("Confusion Matrix")
        plt.xlabel("Predicted")
        plt.ylabel("Actual")
        cm_path = "confusion_matrix.png"
        plt.savefig(cm_path)
        mlflow.log_artifact(cm_path)
        plt.close()

        # Save and log classification report
        report = classification_report(y_test, y_pred_thresh, output_dict=True)
        report_df = pd.DataFrame(report).transpose()
        report_csv = "classification_report.csv"
        report_df.to_csv(report_csv)
        mlflow.log_artifact(report_csv)

        print(f"Model training and logging complete. Run ID: {mlflow.active_run().info.run_id}")

        return model
