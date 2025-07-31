import pandas as pd

# Load the CSV files into DataFrames
file1 = 'path/to/original.csv'
file2 = 'path/to/duplicate.csv'

df1 = pd.read_csv(file1)
df2 = pd.read_csv(file2)

# Compare DataFrames
comparison = df1.compare(df2)

if comparison.empty:
    print("The data in both CSV files are the same.")
else:
    print("The data in the CSV files are different.")
    print(comparison)