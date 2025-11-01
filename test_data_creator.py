import pandas as pd
from pathlib import Path

# === Cấu hình ===
input_file = "producer/data/p000018.psv"   # file gốc
output_file = f"test_{Path(input_file).name}"  

df = pd.read_csv(input_file, sep="|", keep_default_na=False, na_values=[])
cols_to_drop = [col for col in df.columns if "sepsislabel" in col.lower()]
df = df.drop(columns=cols_to_drop, errors="ignore")


df.to_csv(output_file, sep="|", index=False)

print(f" Done! Saved to: {output_file}")
print(f"Removed columns: {cols_to_drop}")
