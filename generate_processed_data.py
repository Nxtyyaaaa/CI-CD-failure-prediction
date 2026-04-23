import pandas as pd
import numpy as np

# simulate dataset size
n = 500

np.random.seed(42)

df = pd.DataFrame({
    "error_flag": np.random.randint(0, 2, n),
    "failure_rate": np.random.rand(n),
    "recent_failure_rate": np.random.rand(n),
    "failure_streak": np.random.randint(0, 5, n),
    "hour": np.random.randint(0, 24, n),
    "day_of_week": np.random.randint(0, 7, n),
})

# simulate target (imbalanced)
df["is_failure"] = (
    (df["error_flag"] == 1) &
    (df["recent_failure_rate"] > 0.6)
).astype(int)

# save as parquet
df.to_parquet("processed_data.parquet", index=False)

print("✅ processed_data created successfully")