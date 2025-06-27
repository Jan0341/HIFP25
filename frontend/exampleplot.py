import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import json
import time
import io
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from openai import OpenAI
from streamlit_extras.colored_header import colored_header


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

# Define relevant features (in dollars or percents)
compare_features = {
    "Avg_allocated_Amount_Per_Provider": "Annualy planned budget",
    "Avg_Deductible_Amt_Paid_Per_Provider" : "Annualy Money paid by patients",
    "Avg_InscClaimAmtReimbursed_Per_Provider" : "Annually Amount of Money for the provider",   
    "perc_allocated_used" : "Percentage of Money used from the budget"
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

    import streamlit as st
import pandas as pd
import plotly.express as px

# 🔐 Dummy Login (optional)

# 📂 Load your actual data here
df = pd.read_csv("merged_result_final.csv")  # Ensure this file exists in your working directory

# 🧾 Map original feature names to user-friendly labels
compare_features = {
    "Avg_allocated_Amount_Per_Provider": "Annually Planned Budget",
    "Avg_Deductible_Amt_Paid_Per_Provider": "Annually Paid by Patients",
    "Avg_InscClaimAmtReimbursed_Per_Provider": "Annually Paid to Provider",
    "perc_allocated_used": "Percentage of Budget Used"
}

# ✳️ Feature groups
dollar_features = [
    "Avg_allocated_Amount_Per_Provider",
    "Avg_Deductible_Amt_Paid_Per_Provider",
    "Avg_InscClaimAmtReimbursed_Per_Provider"
]
percent_features = ["perc_allocated_used"]

# 👤 Provider selector
provider_select = st.selectbox("Select a Provider", df["Provider"].unique())
row = df[df["Provider"] == provider_select].iloc[0]

# 📊 Dollar Features DataFrame
dollar_data = {
    "Feature": [],
    "Value": [],
    "Formatted": []
}
for feat in dollar_features:
    val = row[feat]
    label = compare_features.get(feat, feat)
    dollar_data["Feature"].append(label)
    dollar_data["Value"].append(val)
    dollar_data["Formatted"].append(f"${val:,.2f}")
df_dollars = pd.DataFrame(dollar_data)

# 📈 Percent Features DataFrame
percent_data = {
    "Feature": [],
    "Value": [],
    "Formatted": []
}
for feat in percent_features:
    val = row[feat]
    label = compare_features.get(feat, feat)
    percent_data["Feature"].append(label)
    percent_data["Value"].append(val)
    percent_data["Formatted"].append(f"{val:.2f}%")
df_percent = pd.DataFrame(percent_data)

# 💵 Dollar Feature Bar Plot
fig_dollar = px.bar(
    df_dollars,
    x="Feature",
    y="Value",
    text="Formatted",
    title=f"💵 Dollar-Based Feature Comparison for Provider {provider_select}",
    color="Feature",
    color_discrete_sequence=px.colors.qualitative.Vivid,
)

fig_dollar.update_traces(
    textposition="outside",
    marker_line_width=1.5
)

fig_dollar.update_layout(
    height=600,
    font=dict(size=18, color="black"),
    showlegend=False,
    yaxis_title="Amount ($)",
    xaxis_title="Feature",
    plot_bgcolor="white",
)

fig_dollar.update_xaxes(showgrid=False)
fig_dollar.update_yaxes(showgrid=False)

st.plotly_chart(fig_dollar, use_container_width=True)

# 📊 Percent Feature Bar Plot
fig_percent = px.bar(
    df_percent,
    x="Feature",
    y="Value",
    text="Formatted",
    title=f"📊 Percentage-Based Feature for Provider {provider_select}",
    color="Feature",
    color_discrete_sequence=["#636EFA"]
)

fig_percent.update_traces(
    textposition="outside",
    marker_line_width=1.5
)

fig_percent.update_layout(
    height=500,
    font=dict(size=18, color="black"),
    showlegend=False,
    yaxis_title="Percentage (%)",
    xaxis_title="Feature",
    plot_bgcolor="white",
)

fig_percent.update_xaxes(showgrid=False)
fig_percent.update_yaxes(showgrid=False)

st.plotly_chart(fig_percent, use_container_width=True)

# ---------------- Main Dashboard Section ---------------- #
# Fancy title with colored header
colored_header(
    label="HIFP25 – Fraud Prediction Dashboard",
    description="Detecting suspicious healthcare provider activities",
    color_name="blue-70",
)

# OpenAI client

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
provider_input = st.text_input("🔍 Enter a provider ID:", "")

if provider_input:
    if provider_input in df["Provider"].values:
        row = df[df["Provider"] == provider_input].iloc[0]
        
        # Create columns for layout
        col1, col2 = st.columns([1, 1])
        
        with col1:
            # Top 3 Features Bar Plot
            features = [
                row["Top1_Feature"],
                row["Top2_Feature"],
                row["Top3_Feature"]
            ]
            values = [
                row["Top1_ActualValue"],
                row["Top2_ActualValue"],
                row["Top3_ActualValue"]
            ]
            
            # Format dollar values if it's a reimbursement amount
            formatted_values = []
            for feat, val in zip(features, values):
                if "Reimbursement_Amt" in feat:
                    formatted_values.append(f"${val:,.2f}")
                else:
                    formatted_values.append(f"{val:.2f}")
            
            # Create bar plot
            fig_bar = px.bar(
                x=features,
                y=values,
                text=formatted_values,
                labels={'x': 'Feature', 'y': 'Value'},
                title="Top 3 Influential Features",
                color=features,
                color_discrete_sequence=px.colors.qualitative.Pastel
            )
            fig_bar.update_traces(textposition='outside')
            fig_bar.update_layout(showlegend=False)
            st.plotly_chart(fig_bar, use_container_width=True)
        
        with col2:
            # Fraud Probability Gauge with animation
            fraud_prob = row["Fraud_Probability_Percent"]
            
            # Create animated gauge
            placeholder = st.empty()
            
            # Animation loop
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
                    margin=dict(l=0, r=0, t=30, b=0),
                    height=300
                )
                placeholder.plotly_chart(fig_gauge, use_container_width=True)
                time.sleep(0.05)  # Adjust speed of animation
            
            # Final gauge with exact value
            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number",
                value=fraud_prob,
                title={'text': "Fraud Probability (%)"},
                gauge={
                    'axis': {'range': [0, 100]},
                    'bar': {'color': "red" if fraud_prob > 50 else "green"},
                    'steps': [
                        {'range': [0, 50], 'color': "#E0F7E9"},
                        {'range': [50, 100], 'color': "#FBE9E7"}
                    ],
                    'threshold': {
                        'line': {'color': "black", 'width': 4},
                        'thickness': 0.75,
                        'value': fraud_prob
                    }
                },
                domain={'x': [0, 1], 'y': [0, 1]}
            ))
            fig_gauge.update_layout(
                margin=dict(l=0, r=0, t=30, b=0),
                height=300
            )
            placeholder.plotly_chart(fig_gauge, use_container_width=True)
        
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
        
    else:
        st.error("❌ Provider ID not found. Please check the ID and try again.")

# Add some decorative elements
st.sidebar.markdown("---")
st.sidebar.markdown("### 🛡️ Fraud Detection Metrics")
st.sidebar.metric("Total Providers Analyzed", len(df))
st.sidebar.metric("High Risk Providers", len(df[df["Fraud_Probability_Percent"] > 70]))
st.sidebar.markdown("---")
st.sidebar.info("ℹ️ This dashboard helps identify potential healthcare fraud using advanced machine learning models.")
