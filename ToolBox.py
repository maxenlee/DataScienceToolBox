import pandas as pd

from google.cloud import bigquery
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
    
## Yo, Maxen ==>>>>  Were you wanting to do something like this?

        # Analysis logic for a single DataFrame
        analysis_results = {}
        
        # Shape of the DataFrame
        analysis_results["shape_info"] = pd.Series(df.shape, index=["rows", "columns"])
        
        # Identify all unique data types in the DataFrame
        analysis_results["dtype_counts"] = df.dtypes.value_counts()
        
        # Total nulls
        analysis_results["total_nulls"] = pd.Series({'Nulls': df.isnull().sum().sum()})

        # Memory usage (in bytes)
        analysis_results["Deep(Bytes)"] = df.memory_usage(deep=True).sum()
        analysis_results["Shallow (Bytes)"] = df.memory_usage(deep=False).sum()

        # Total unique values
        analysis_results["total_unique_values"] = pd.Series({'Unique': sum(df.nunique())})

        # Total size
        analysis_results["total_size"] = pd.Series({'Size': df.size})
        
        # Combine all information into a single DataFrame for analysis
        return  pd.concat(analysis_results.values())


# Register the custom accessor on pandas DataFrame with the name "df_kit"
pd.api.extensions.register_dataframe_accessor("df_kit")(DataFrameAnalyzer)





# Ensure that the BigQuery client is initialized
client = bigquery.Client(project='your-project-id')

def run_query(sample_query, client=client, dry_run=False):
    """
    Run or estimate the cost of a BigQuery query.

    This function takes a SQL query as input and uses the BigQuery client to either
    run the query and return its results or estimate the cost of running the query if
    dry_run is set to True.

    Parameters:
    - sample_query (str): A SQL query string to run or estimate the cost for.
    - client (bigquery.Client): Optional. A BigQuery client object to perform the query.
                                If not provided, a default client will be used.
    - dry_run (bool): Optional. If set to True, the function will estimate the cost
                      of the query instead of running it.

    Prints:
    - If dry_run is True, prints the estimated bytes that the query will process and the estimated cost.

    Returns:
    - If dry_run is True, returns the estimated cost (float) in USD.
    - If dry_run is False, returns the query results as a pandas DataFrame.
    """
    
    job_config = bigquery.QueryJobConfig(dry_run=dry_run, use_query_cache=True)

    # Perform the dry run or actual query based on the dry_run flag
    query_job = client.query(sample_query, job_config=job_config)

    if dry_run:
        # The query will not be executed, no rows will be returned.
        # Instead, you can get the estimated bytes processed for the query.
        total_bytes_processed = query_job.total_bytes_processed
        print(f"This query will process {total_bytes_processed} bytes.")

        # Cost per TB (as of last update, please check the latest rates)
        cost_per_tb = 5.0  # You may update this value based on the current BigQuery pricing

        # Convert bytes to TB (1 TB = 10^12 bytes)
        bytes_in_tb = 10**12

        # Calculate the estimated cost
        estimated_cost = (total_bytes_processed / bytes_in_tb) * cost_per_tb

        print(f"Estimated cost of the query: ${estimated_cost:.2f}")

        return estimated_cost
    else:
        # Wait for the query to finish
        results = query_job.result()
        # Convert the results to a pandas DataFrame
        df = results.to_dataframe()

        return df

# Usage:
# Assuming 'sample_query' is defined
# To estimate the cost:
# estimated_cost = run_query(sample_query, dry_run=True)

# To actually run the query and get results:
# query_results = run_query(sample_query)



if __name__ == "__main__":
    print('')
    # Code to execute when the module is run as a script
    # For example, test your run_query function here





