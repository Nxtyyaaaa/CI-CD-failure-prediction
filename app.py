import streamlit as st
import joblib
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# load model
model = joblib.load("model.pkl")

st.title("🚀 CI/CD Failure Prediction Dashboard")

# --- INPUT ---
st.sidebar.header("Input Features")

failure_streak = st.sidebar.slider("Failure Streak", 0, 10, 1)
hour = st.sidebar.slider("Hour", 0, 23, 12)
day = st.sidebar.slider("Day of Week", 0, 6, 3)

data = pd.DataFrame([{
    "failure_streak": failure_streak,
    "hour": hour,
    "day_of_week": day
}])

# --- PREDICTION ---
st.subheader("🔍 Prediction Result")

if st.button("Predict"):
    pred = model.predict(data)[0]
    prob = model.predict_proba(data)[0][1]

    st.metric("Failure Probability", round(prob, 2))

    # risk classification
    if prob > 0.8:
        st.error("🚨 HIGH RISK of Failure")
    elif prob > 0.5:
        st.warning("⚠️ MEDIUM RISK")
    else:
        st.success("✅ LOW RISK")

    st.write("Prediction:", "Failure" if pred else "Success")

# --- PR CURVE ---
st.subheader("📈 Model Performance (Precision-Recall Curve)")

try:
    precision = np.load("precision.npy")
    recall = np.load("recall.npy")

    fig, ax = plt.subplots()
    ax.plot(recall, precision)
    ax.set_xlabel("Recall")
    ax.set_ylabel("Precision")
    ax.set_title("Precision-Recall Curve")

    st.pyplot(fig)

except:
    st.info("Run train_model.py to generate PR curve data")