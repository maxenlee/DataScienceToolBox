import pandas as pd

class DataFrameAnalyzer:
    """
    A custom DataFrame analyzer accessor for pandas DataFrames.

    This class provides a method to extract and summarize key statistics and 
    information about a DataFrame, which can be particularly useful for initial 
    data exploration and analysis. It includes details about the shape of the DataFrame, 
    data types present, memory usage, null value counts, and the sum of unique values across all columns.

    Attributes:
    - _obj (pd.DataFrame): The pandas DataFrame instance to be analyzed.

    Methods:
    - analyze(): Returns a summary of key statistics and information about the DataFrame.
    """

    def __init__(self, pandas_obj):
        """
        Initializes the DataFrameAnalyzer with a pandas DataFrame object.

        Parameters:
        - pandas_obj (pd.DataFrame): The pandas DataFrame to be analyzed.
        """
        self._obj = pandas_obj

    def analyze(self):
        """
        Analyzes the DataFrame and returns a summary of key statistics and information.

        The analysis includes the following details:
        - Shape of the DataFrame, indicating the number of rows and columns.
        - Data types counts, showing how many columns there are for each data type present.
        - Deep and shallow memory usage of the DataFrame, in bytes.
        - Total count of null values across all columns.
        - Sum of unique values across all columns.

        Returns:
        pd.Series: A pandas Series object containing the summarized statistics and information
        of the DataFrame, with each statistic as an item in the Series.
        """
        df = self._obj  # The DataFrame to analyze

        # Shape of the DataFrame
        shape_info = pd.Series(df.shape, index=["rows", "columns"], name="Shape")

        # Data types counts
        all_dtypes = list(df.dtypes.unique())
        dtype_counts = pd.Series(0, index=all_dtypes, name="Dtype Counts").update(df.dtypes.value_counts())

        

        # Total nulls
        total_nulls = pd.Series({'Total Null Counts': df.isnull().sum().sum()})

        # Total unique values
        total_unique_values = pd.Series({'Total Unique Values': sum(df.nunique())})

        # Memory usage
        total_memory_deep = df.memory_usage(deep=True).sum()
        total_memory_shallow = df.memory_usage(deep=False).sum()
        mem_info_deep = pd.Series([total_memory_deep], index=["Deep(Bytes)"], name="Memory")
        mem_info_shallow = pd.Series([total_memory_shallow], index=["Shallow (Bytes)"], name="Memory")

        # Combining all information into a single summary
        summary_stats = pd.concat([shape_info, dtype_counts, total_nulls, total_unique_values, mem_info_deep, mem_info_shallow])

        return summary_stats

# Register the custom accessor on pandas DataFrame with the name "df_kit"
pd.api.extensions.register_dataframe_accessor("df_kit")(DataFrameAnalyzer)
