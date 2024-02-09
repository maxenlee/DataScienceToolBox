import pandas as pd

class DataFrameAnalyzer:
    def __init__(self):
        """
        Initialize the DataFrameAnalyzer instance.
        Currently, no initialization parameters are needed, but this method can be expanded
        in the future to accept parameters for configuration.
        """
        pass

    def analyzer(self,dict_df):
        # Initialize a dictionary to store the analysis results
        analysis_results = {}

        # Identify all unique data types across all data frames
        all_dtypes = set()
        for df in dict_df.values():
            all_dtypes.update(df.dtypes.unique())

        # Convert set of dtypes to a list for consistent ordering
        all_dtypes = list(all_dtypes)

        for name, df in dict_df.items():
            # Shape of the DataFrame
            shape_info = pd.Series(df.shape, index=["rows", "columns"], name="Shape")

            # Initialize dtype counts series with zeros for all identified dtypes
            dtype_counts = pd.Series(0, index=all_dtypes, name="Dtype Counts")
            # Update counts with actual values
            dtype_counts.update(df.dtypes.value_counts())

            # Memory usage (in bytes)
            total_memory = df.memory_usage().sum()
            mem_info = pd.Series([total_memory], index=["Memory Usage (Bytes)"], name="Memory")

            # Combine all information into a DataFrame for the current DataFrame
            summary_df = pd.concat([shape_info, dtype_counts, mem_info])

            # Store the summary DataFrame in the results dictionary
            analysis_results[name] = summary_df

        # Optional: Convert the results dictionary to a multi-index DataFrame for a cleaner presentation
        summary_df_all = pd.concat(analysis_results, axis=1)

        return summary_df_all
