import pandas as pd
import os
from datetime import datetime

# List of CSV files you want to process
csv_files = [
    "freepalestine_videos.csv",
    "standwithisrael_videos_.csv"
]

for filename in csv_files:
    if os.path.exists(filename):
        print(f" Processing {filename}...")

        # Load CSV
        df = pd.read_csv(filename)

        # Convert Unix timestamp to human-readable format
        df["create_date"] = pd.to_datetime(df["create_time"], unit='s')

        # Save new file
        new_filename = filename.replace(".csv", "_readable.csv")
        df.to_csv(new_filename, index=False, encoding="utf-8-sig")
        print(f" Saved to {new_filename}\n")
    else:
        print(f" File not found: {filename}")
