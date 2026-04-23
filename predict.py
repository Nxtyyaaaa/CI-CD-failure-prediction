import joblib
import pandas as pd

model = joblib.load("model.pkl")

data = pd.DataFrame([{
    "failure_streak": 2,
    "hour": 14,
    "day_of_week": 3
}])

prediction = model.predict(data)
prob = model.predict_proba(data)

print("Prediction:", prediction[0])
print("Failure Probability:", prob[0][1])

threshold = 0.5

prediction = model.predict(data)[0]
prob = model.predict_proba(data)[0][1]

print("Raw Probability:", round(prob, 4))

if prob > 0.8:
    risk = "HIGH RISK"
elif prob > 0.5:
    risk = "MEDIUM RISK"
else:
    risk = "LOW RISK"

print("Prediction:", "Failure" if prediction else "Success")
print("Risk Level:", risk)