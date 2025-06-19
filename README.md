# HIFP25 - Health Insurance Fraud Project 2025
This project was developed by [Jan0341](https://github.com/Jan0341) and [Hassan-Merai](https://github.com/Hassan-Merai/).

It is a machine learning pipeline designed and deployed on AWS, using an EC2 t3.large instance and an S3 bucket for storage.

The EC2 instance was responsible for executing all processing steps, including:

```mermaid
graph TD
    A[Data Cleaning] --> B[Feature Engineering]
    B --> C[Preprocessing]
    C --> D[Model Training]
    D --> E[Front-End (Streamlit)]
```

The main goal of the project was to build a predictive model capable of identifying potential health insurance fraud in the United States, particularly fraud committed by healthcare providers.

The entire project was implemented in Python, primarily using Dask and Pandas for data handling.
We used XGBoost as the main machine learning algorithm and Apache Airflow for workflow automation.
