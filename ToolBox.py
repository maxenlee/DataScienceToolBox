import pandas as pd

class DataFrameAnalyzer:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    def analyze(self):
        df = self._obj  # The DataFrame to analyze

        # Identify all unique data types in this DataFrame
        all_dtypes = list(df.dtypes.unique())

        # Shape of the DataFrame
        shape_info = pd.Series(df.shape, index=["rows", "columns"], name="Shape")

        # Initialize dtype counts series with zeros for all identified dtypes
        dtype_counts = pd.Series(0, index=all_dtypes, name="Dtype Counts")
        # Update counts with actual values
        dtype_counts.update(df.dtypes.value_counts())

        # Memory usage (in bytes), deep and shallow
        total_memory_deep = df.memory_usage(deep=True).sum()
        total_memory_shallow = df.memory_usage(deep=False).sum()
        mem_info_deep = pd.Series([total_memory_deep], index=["Deep Memo"], name="Memory")
        mem_info_shallow = pd.Series([total_memory_shallow], index=["Shallow Mem"], name="Memory")

        # Count of nulls in total
        total_nulls = pd.Series({'Nulls': df.isnull().sum().sum()})

         # Sum of Unique Values Across All Columns
        total_unique_values = pd.Series({'Unique': sum(df.nunique())})

        # Combining all information into a single DataFrame for analysis
        summary_stats = pd.concat([shape_info, dtype_counts, mem_info_deep, mem_info_shallow, total_nulls, total_unique_values])

        return summary_stats

# Register the custom accessor on pandas DataFrame with the name "df_kit"
pd.api.extensions.register_dataframe_accessor("df_kit")(DataFrameAnalyzer)
