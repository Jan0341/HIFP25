from fastapi import FastAPI, HTTPException
import pandas as pd
import json

app = FastAPI()

# 1. Lade Ergebnis-DataFrame (fraud_predictions_with_shap.csv)
results = pd.read_csv("/home/ec2-user/HIFP25/Frontend/fraud_predictions_with_shap.csv")

# 2. Lade Feature-Erklärungen
with open("/home/ec2-user/HIFP25/Frontend/feature_explanation.json", "r") as f:
    feature_explanations = json.load(f)

# 3. Feature-Erklärungen zu den Top 3 Features im DataFrame hinzufügen
def get_feature_explanations(row):
    explanations = []
    for feat in [row['Top1_Feature'], row['Top2_Feature'], row['Top3_Feature']]:
        explanation = feature_explanations.get(feat, "Keine Erklärung verfügbar")
        explanations.append(explanation)
    return explanations

results['Top_Feature_Explanations'] = results.apply(get_feature_explanations, axis=1)

# 4. API-Endpunkt, um Infos pro Provider zu liefern
@app.get("/provider/{provider_id}")
def get_provider_info(provider_id: str):
    row = results[results['Provider'] == provider_id]
    if row.empty:
        raise HTTPException(status_code=404, detail="Provider nicht gefunden")
    return {
        "Provider": provider_id,
        "Fraud_Probability": float(row["Fraud_Probability"].values[0]),
        "Fraud_Probability_Percent": float(row["Fraud_Probability_Percent"].values[0]),
        "Top_Features": [
            row["Top1_Feature"].values[0],
            row["Top2_Feature"].values[0],
            row["Top3_Feature"].values[0]
        ],
        "Top_Feature_Explanations": row["Top_Feature_Explanations"].values[0]
    }
