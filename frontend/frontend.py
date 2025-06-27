import streamlit as st
import pandas as pd
import json
import plotly.graph_objects as go
import plotly.express as px
from openai import OpenAI
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
import io
import time
from streamlit_extras.colored_header import colored_header
from streamlit_extras.let_it_rain import rain

# Set page config with fancy theme
st.set_page_config(
    page_title="HIFP25 – Health Insurance Fraud Detection",
    layout="centered",
    initial_sidebar_state="auto",
    page_icon="🕵️"
)

# Custom CSS for fancy decorations
st.markdown("""
<style>
    .main {
        background-color: #f8f9fa;
    }
    .stTextInput>div>div>input {
        border: 2px solid #4a4e69;
        border-radius: 5px;
    }
    .stButton>button {
        background-color: #4a4e69;
        color: white;
        border-radius: 5px;
        border: none;
        padding: 10px 24px;
    }
    .stButton>button:hover {
        background-color: #22223b;
        color: white;
    }
    .css-1aumxhk {
        background-color: #22223b;
    }
    .css-1v3fvcr {
        background-color: #f8f9fa;
    }
    .css-1q8dd3e {
        background-color: #4a4e69;
    }
</style>
""", unsafe_allow_html=True)

# Login function
USER_CREDENTIALS = {
    "admin": "geheim123",
    "user1": "passwort456"
}

st.sidebar.header("🔒 Login")
username = st.sidebar.text_input("Username")
password = st.sidebar.text_input("Password", type="password")

if username in USER_CREDENTIALS and password == USER_CREDENTIALS[username]:
    st.success(f"✨ Welcome, {username}!")
else:
    st.warning("Please log in to access the dashboard.")
    st.stop()

# Load data
df = pd.read_csv("allmerged_result.csv")
with open("feature_explanation.json") as f:
    feat_dict = json.load(f)

# ---------------- Filter and Comparison Section ---------------- #
colored_header(
    label="HIFP25 – Fraud Prediction Dashboard",
    description="Detecting suspicious healthcare provider activities",
    color_name="blue-70",
)


# Define relevant features (in dollars or percents)
compare_features = {
    "Avg_allocated_Amount_Per_Provider": "Annualy planned budget",
    "Avg_InscClaimAmtReimbursed_Per_Provider" : "Annually Amount of Money for the provider",
    "Avg_Deductible_Amt_Paid_Per_Provider" : "Annualy Money paid by patients",
    #"perc_allocated_used" : "Percentage of Money used from the budget"
                    }

# Sidebar slider for fraud probability filter
st.sidebar.markdown("### 🎯 Filter by Fraud Probability")
min_fraud, max_fraud = float(df["Fraud_Probability_Percent"].min()), float(df["Fraud_Probability_Percent"].max())
fraud_range = st.sidebar.slider("Fraud Probability Range (%)", 0.0, 100.0, (min_fraud, max_fraud), step=1.0)

# Filter data
filtered_df = df[(df["Fraud_Probability_Percent"] >= fraud_range[0]) & (df["Fraud_Probability_Percent"] <= fraud_range[1])]

# Dropdown to select one provider from filtered list
provider_select = st.selectbox("Compare Dollar-Based Features of a Provider", options=filtered_df["Provider"].unique())

if provider_select:
    row = df[df["Provider"] == provider_select].iloc[0]

    # Extract and format values for the bar plot
    bar_data = {
        "Feature": [],
        "Value": [],
        "Formatted": []
    }

    for feat, name in compare_features.items():
        val = row[feat]
        bar_data["Feature"].append(name)
        bar_data["Value"].append(val)
        bar_data["Formatted"].append(f"${val:,.2f}" if "perc" not in feat else f"{val*100:.2f}%")

    bar_df = pd.DataFrame(bar_data)

    # Animate bar plot on change
    with st.container():
        fig = px.bar(
            bar_df,
            x="Feature",
            y="Value",
            text="Formatted",
            title=f" Feature Comparison for Provider in Dollars {provider_select}",
            color="Feature",
            color_discrete_sequence=px.colors.qualitative.Vivid,
        )
        fig.update_traces(textposition="outside")
        fig.update_layout(
            showlegend=False,
            yaxis_title="Value",
            xaxis_title="Feature",
            transition={"duration": 500},
            margin=dict(t=100)  # smooth animation
        )
        st.plotly_chart(fig, use_container_width=True)
# ---------------- Main Dashboard Section ---------------- #
# OpenAI client
client = OpenAI(api_key="Put-Api-Key-Here-please")

# Explanation generation with OpenAI (cached)
@st.cache_data(show_spinner="Generating Report...")
def generate_llm_explanation(feature_name, actual_value, existing_description):
    prompt = f"""
    Explain to a professional audience the impact of the feature '{feature_name}' on the prediction of potential healthcare fraud.
    - Actual_Value: {actual_value}
    - Description: {existing_description}
    Provide a clear and concise explanation of how this feature influences the model.
    """.strip()
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=[
                {"role": "system", "content": "You are an expert in machine learning explainability."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=200,
            temperature=0.7,
        )
        if response.choices and response.choices[0].message and response.choices[0].message.content:
            return response.choices[0].message.content.strip()
        else:
            return "(No content returned by OpenAI.)"
    except Exception as e:
        return f"(Error generating explanation: {e})"

# Provider ID input with fancy decoration
if provider_select:
    row = df[df["Provider"] == provider_select].iloc[0]

    # ------------------ Gauge Above ------------------ #
    fraud_prob = row["Fraud_Probability_Percent"]
    gauge_placeholder = st.empty()
    
    for percent in range(0, int(fraud_prob) + 1, 2):
        fig_gauge = go.Figure(go.Indicator(
            mode="gauge+number",
            value=percent,
            title={'text': "Fraud Probability (%)"},
            gauge={
                'axis': {'range': [0, 100]},
                'bar': {'color': "red" if percent > 50 else "green"},
                'steps': [
                    {'range': [0, 50], 'color': "#E0F7E9"},
                    {'range': [50, 100], 'color': "#FBE9E7"}
                ],
                'threshold': {
                    'line': {'color': "black", 'width': 4},
                    'thickness': 0.75,
                    'value': percent
                }
            },
            domain={'x': [0, 1], 'y': [0, 1]}
        ))
        fig_gauge.update_layout(
            margin=dict(t=80),
            height=300
        )
        gauge_placeholder.plotly_chart(fig_gauge, use_container_width=True)
        time.sleep(0.05)
    
        # Final gauge state
        fig_gauge.update_traces(value=fraud_prob)
        gauge_placeholder.plotly_chart(fig_gauge, use_container_width=True)

        # ------------------ Bar Plot Below ------------------ #
        #st.markdown("### 💵 Dollar-Based Feature Breakdown")
        #bar_data = {
        #    "Feature": [],
        #    "Value": [],
        #    "Formatted": []
        #}

        #for feat, name in compare_features.items():
        #    val = row[feat]
        #    bar_data["Feature"].append(name)
        #    bar_data["Value"].append(val)
        #    bar_data["Formatted"].append(f"${val:,.2f}" if "perc" not in feat else f"{val*100:.2f}%")

        #bar_df = pd.DataFrame(bar_data)
        #fig = px.bar(
        #    bar_df,
        #    x="Feature",
        #    y="Value",
        #    text="Formatted",
        #    title=f" Dollar-Based Feature Comparison for Provider {provider_select}",
        #    color="Feature",
        #    height=600,  # Bigger bar plot
        #    color_discrete_sequence=px.colors.qualitative.Vivid,
        #)
        #fig.update_traces(textposition="outside")
        #fig.update_layout(
        #    showlegend=False,
        #    yaxis_title="Value",
        #    xaxis_title="Feature",
        #    transition={"duration": 500},
        #)
        #st.plotly_chart(fig, use_container_width=True)

    # ------------------ Report Button ------------------ #
    if st.button("📝 Generate Explanation Report for Selected Provider"):
        # Now reuse your previous LLM explanation block with:
        provider_input = provider_select
        row = df[df["Provider"] == provider_input].iloc[0]
        # And generate explanation and PDF as before
        ...

        
        # Generate report with explanations
        actual_features = [
            ("Top1_Feature", "Top1_ActualValue"),
            ("Top2_Feature", "Top2_ActualValue"),
            ("Top3_Feature", "Top3_ActualValue"),
        ]
        
        report_parts = []
        for i, (feat_col, val_col) in enumerate(actual_features, 1):
            feat_name = row[feat_col]
            actual_value = row[val_col]
            existing_desc = feat_dict.get(feat_name, "No description available.")
            
            # Format dollar values if it's a reimbursement amount
            display_value = f"${actual_value:,.2f}" if "Reimbursement_Amt" in feat_name else f"{actual_value:.2f}"
            
            explanation = generate_llm_explanation(feat_name, actual_value, existing_desc)
            paragraph = f"**{i}. {feat_name} (Value: {display_value})**\n\n{explanation}\n"
            report_parts.append(paragraph)
        
        full_report = "\n---\n".join(report_parts)
        
        # Display report in an expandable section
        with st.expander("📊 Individual SHAP declaration for this provider", expanded=True):
            st.markdown(full_report)
        
        # PDF generation function
        def generate_pdf(text, title="SHAP Report"):
            buffer = io.BytesIO()
            c = canvas.Canvas(buffer, pagesize=A4)
            width, height = A4
            
            # Set colors and fonts
            c.setFillColorRGB(0.2, 0.2, 0.4)  # Dark blue
            c.setStrokeColorRGB(0.2, 0.2, 0.4)
            c.setFont("Helvetica-Bold", 16)
            
            # Draw header with rectangle
            c.rect(30, height - 60, width - 60, 40, fill=1)
            c.setFillColorRGB(1, 1, 1)  # White text
            c.drawCentredString(width/2, height - 45, title)
            c.setFillColorRGB(0, 0, 0)  # Black for content
            
            # Content
            c.setFont("Helvetica", 10)
            y = height - 100
            for line in text.split("\n"):
                if y < 50:  # New page if we're at the bottom
                    c.showPage()
                    c.setFont("Helvetica", 10)
                    y = height - 50
                c.drawString(50, y, line)
                y -= 15
            
            # Add footer
            c.setFont("Helvetica", 8)
            c.drawString(50, 30, f"Generated by HIFP25 Fraud Detection System for Provider {provider_input}")
            
            c.save()
            buffer.seek(0)
            return buffer
        
        # Download button with fancy styling
        pdf_buffer = generate_pdf(full_report, title=f"SHAP Report for Provider {provider_input}")
        st.download_button(
            label="📥 Download Report as PDF",
            data=pdf_buffer,
            file_name=f"shap_report_{provider_input}.pdf",
            mime="application/pdf",
            help="Click to download a detailed PDF report"
        )
        

# Add some decorative elements
st.sidebar.markdown("---")
st.sidebar.markdown("### 🛡️ Fraud Detection Metrics")
st.sidebar.metric("Total Providers Analyzed", len(df))
st.sidebar.metric("High Risk Providers", len(df[df["Fraud_Probability_Percent"] > 70]))
st.sidebar.markdown("---")
st.sidebar.info("ℹ️ This dashboard helps identify potential healthcare fraud using advanced machine learning models.")