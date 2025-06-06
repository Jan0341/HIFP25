# FastApi.py
from fastapi import FastAPI
import pandas as pd
from pydantic import BaseModel
import random


app = FastAPI()

class FraudInput(BaseModel):
    Provider: str
    ClaimDuration_mean: float
    ChronicCond_Diabetes_mean: float

@app.post("/predict")
def predict(input_data: FraudInput):
    # Dummy-Vorhersage (Zufallswert oder fester Wert)
    dummy_value = round(random.uniform(5, 95), 2)  # z. B. 12.34 %
    return {"fraud_probability": dummy_value}