import pandas as pd

class DataFrameAnalyzer:
    def __init__(self, pandas_obj):
        self._obj = pandas_obj

    def analyze(self):
        # Check the type of the _obj attribute to determine the analysis approach
        if isinstance(self._obj, pd.DataFrame):
            return self._analyze_dataframe(self._obj)
        elif isinstance(self._obj, dict):
            # Assuming all values in the dictionary are DataFrames
            return {key: self._analyze_dataframe(df) for key, df in self._obj.items()}
        elif isinstance(self._obj, list):
            # Assuming the list contains DataFrames
            return [self._analyze_dataframe(df) for df in self._obj]
        else:
            raise TypeError("Unsupported type. The analyzer supports pandas DataFrames, dictionaries of DataFrames, and lists of DataFrames.")

    def _analyze_dataframe(self, df):
        # Analysis logic for a single DataFrame
        analysis_results = {}

        # Identify all unique data types in the DataFrame
        all_dtypes = list(df.dtypes.unique())

        # Shape of the DataFrame
        shape_info = pd.Series(df.shape, index=["rows", "columns"], name="Shape")

        # Initialize dtype counts series with zeros for all identified dtypes
        dtype_counts = pd.Series(0, index=all_dtypes, name="Dtype Counts")
        # Update counts with actual values
        dtype_counts.update(df.dtypes.value_counts())

        # Memory usage (in bytes)
        total_memory = df.memory_usage(deep=True).sum()
        mem_info = pd.Series([total_memory], index=["Memory Usage (Bytes)"], name="Memory")

        # Combine all information into a single DataFrame for analysis
        summary_df = pd.concat([shape_info, dtype_counts, mem_info])

        return summary_df

# Register the custom accessor on pandas DataFrame with the name "df_kit"
pd.api.extensions.register_dataframe_accessor("df_kit")(DataFrameAnalyzer)
