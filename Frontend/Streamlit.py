# app.py
import streamlit as st
import requests

st.set_page_config(page_title="Fraud Detection", layout="centered")
st.title("💡 Medical Fraud Prediction")

# Eingaben
provider = st.text_input("Provider ID", value="P123")
claim_duration = st.slider("Durchschnittliche Claim-Dauer", 0, 200, 50)
diabetes = st.selectbox("Viele Patienten mit Diabetes?", ["Ja", "Nein"])

if st.button("Vorhersage starten"):
    payload = {
        "Provider": provider,
        "ClaimDuration_mean": claim_duration,
        "ChronicCond_Diabetes_mean": 1.0 if diabetes == "Ja" else 0.0
    }

    try:
        response = requests.post("http://3.70.132.180:8000/predict", json=payload)
        result = response.json()
        st.success(f"📊 Betrugswahrscheinlichkeit: {result['fraud_probability']}%")
    except:
        st.error("❌ Verbindung zum Backend fehlgeschlagen")
