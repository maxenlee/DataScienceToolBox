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

from google.cloud import bigquery

class QueryStr(str):
    """
    A subclass of str designed to hold SQL queries specifically formatted for
    Google BigQuery, with added functionality to execute or estimate the cost of
    the query within a Google Cloud project.

    Attributes:
    Inherits all attributes from str.

    Methods:
    BigQuery(dry_run=False, project_id=None): Executes or estimates the cost of the BigQuery query.
    """

    def __new__(cls, *args, **kwargs):
        # This method is overridden to ensure instances are created properly.
        return super(QueryStr, cls).__new__(cls, *args, **kwargs)

    def __repr__(self):
        # Provides a more descriptive representation of the instance.
        return "<Formatted BigQuery String>"

    def BigQuery(self, dry_run=False, project_id=None):
        """
        Executes or estimates the cost of the BigQuery query.

        Parameters:
        dry_run (bool): If True, estimates the cost of the query without executing it. Defaults to False.
        project_id (str): The Google Cloud project ID to run the query against. If not specified,
                          attempts to use 'userdata.get('project_id')'. Ensure 'userdata' is defined.

        Returns:
        float: Estimated cost in USD if dry_run is True.
        pandas.DataFrame: Query results as a DataFrame if dry_run is False.
        """
        if project_id is None:
            try:
                project_id = userdata.get('project_id')  # Ensure 'userdata' is defined and accessible.
            except NameError:
                raise ValueError("project_id is required if userdata is not defined or does not contain 'project_id'.")

        client = bigquery.Client(project=project_id)
        job_config = bigquery.QueryJobConfig(dry_run=dry_run, use_query_cache=True)
        query_job = client.query(self, job_config=job_config)

        if dry_run:
            total_bytes_processed = query_job.total_bytes_processed
            print(f"This query will process {total_bytes_processed} bytes.")
            
            cost_per_tb = 5.0  # Update this based on current BigQuery pricing
            bytes_in_tb = 10**12
            estimated_cost = (total_bytes_processed / bytes_in_tb) * cost_per_tb
            print(f"Estimated cost of the query: ${estimated_cost:.2f}")
            
            return estimated_cost
        else:
            results = query_job.result()
            return results.to_dataframe()




if __name__ == "__main__":
    print('')
    # Code to execute when the module is run as a script
    # For example, test your run_query function here





