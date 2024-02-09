import pandas as pd

class DataFrameAnalyzer:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    def analyze(self):
        df = self._obj  # The DataFrame to analyze
        
        # Identify all unique data types in the DataFrame
        all_dtypes = list(df.dtypes.unique())

        # Shape of the DataFrame
        shape_info = pd.Series(df.shape, index=["rows", "columns"], name="Shape")

        # Initialize dtype counts series with zeros for all identified dtypes
        dtype_counts = pd.Series(0, index=all_dtypes, name="Dtype Counts")
        dtype_counts.update(df.dtypes.value_counts())

        # Deep memory usage (in bytes)
        deep_memory_usage = df.memory_usage(deep=True).sum()
        deep_mem_info = pd.Series([deep_memory_usage], index=["Deep Memory Usage (Bytes)"], name="Memory")

        # Shallow memory usage (in bytes)
        shallow_memory_usage = df.memory_usage(deep=False).sum()
        shallow_mem_info = pd.Series([shallow_memory_usage], index=["Shallow Memory Usage (Bytes)"], name="Memory")

        # Count of nulls per column
        null_counts = df.isnull().sum().rename("Null Counts")

        # Unique values per column
        unique_values = df.nunique().rename("Unique Values")

        # Summary statistics for numeric columns
        numeric_stats = df.describe().transpose()

        # Combining all information
        summary_df = pd.concat([shape_info, dtype_counts, deep_mem_info, shallow_mem_info, null_counts, unique_values], axis=0)
        summary_df = pd.concat([summary_df, numeric_stats], axis=0, sort=False)

        return summary_df

# Register the custom accessor on pandas DataFrame with the name "df_kit"
pd.api.extensions.register_dataframe_accessor("df_kit")(DataFrameAnalyzer)
