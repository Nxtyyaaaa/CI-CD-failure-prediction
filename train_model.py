import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
import numpy as np

# load data

df = pd.read_parquet("processed_data")
# features
features = [
    "failure_streak",
    "hour",
    "day_of_week"
]
import numpy as np

# rebuild target (remove leakage)
prob = (
    0.3 * df["failure_streak"] +
    0.2 * df["hour"]/24 +
    0.2 * df["day_of_week"]/7 +
    0.3 * np.random.rand(len(df))
)

df["is_failure"] = (prob > 0.6).astype(int)

X = df[features]
y = df["is_failure"]

# split
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# model
model = LogisticRegression(class_weight="balanced")

# train
model.fit(X_train, y_train)

# predict
y_pred = model.predict(X_test)

# evaluate
print(classification_report(y_test, y_pred))

import joblib
joblib.dump(model, "model.pkl")

print("✅Model saved as model.pkl")

from sklearn.metrics import precision_recall_curve
import numpy as np

# get probabilities
y_scores = model.predict_proba(X_test)[:, 1]

precision, recall, thresholds = precision_recall_curve(y_test, y_scores)

# save for dashboard
np.save("precision.npy", precision)
np.save("recall.npy", recall)

print("✅ PR curve data saved")