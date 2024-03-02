import pandas as pd
from google.cloud import bigquery


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
    
        # Analysis logic for a single DataFrame
        analysis_results = {}
        
        # Shape of the DataFrame
        analysis_results["shape_info"] = pd.Series(df.shape, index=["rows", "columns"])
        
        # Identify all unique data types in the DataFrame
        analysis_results["dtype_counts"] = df.dtypes.value_counts()
        
        # Total nulls
        analysis_results["total_nulls"] = pd.Series({'Nulls': df.isnull().sum().sum()})

        # Memory usage (in bytes)
        analysis_results["Deep(Bytes)"] = pd.Series({"Deep(Bytes)":df.memory_usage(deep=True).sum()})
        analysis_results["Shallow (Bytes)"] = pd.Series({"Shallow (Bytes)":df.memory_usage(deep=False).sum()})

        # Total unique values
        analysis_results["total_unique_values"] = pd.Series({'Unique': sum(df.nunique())})

        # Total size
        analysis_results["total_size"] = pd.Series({'Size': df.size})
        
        # Combine all information into a single DataFrame for analysis
        return  pd.concat(analysis_results.values())


# Register the custom accessor on pandas DataFrame with the name "df_kit"
pd.api.extensions.register_dataframe_accessor("df_kit")(DataFrameAnalyzer)



# ToolBox.py


from IPython import get_ipython
from google.cloud import bigquery as bq
from IPython.core.magic import register_cell_magic
from IPython.display import display
import pandas as pd
import shlex  # For safely splitting the argument line
from google.api_core.exceptions import GoogleAPIError, BadRequest, Forbidden, NotFound, Conflict, InternalServerError, ServiceUnavailable
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('BigQueryMagic')

# Global configuration dictionary for BigQuery settings
bigquery_config = {
    'source': 'default-source',
    'project_id': 'default-project-id',
}

def configure_bigquery(source=None, project_id=None):
    """
    Update the BigQuery configuration.
    
    Parameters:
    - source (str): The default dataset source to use in queries.
    - project_id (str): The Google Cloud project ID for billing and access.
    """
    global bigquery_config
    if source:
        bigquery_config['source'] = source
    if project_id:
        bigquery_config['project_id'] = project_id

@register_cell_magic
def bigquery(line, cell):
    """
    Execute a BigQuery SQL query and optionally store the results in a pandas DataFrame.
    
    Usage:
    %%bigquery [options] [dataframe_var_name]
    <SQL query>
    
    Options:
    - dry: Perform a dry run to estimate query costs.
    - --source=<source>: Specify a dataset source to override the global configuration.
    - --project_id=<project_id>: Specify a Google Cloud project ID to override the global configuration.
    
    Parameters:
    - line (str): The options line where options include 'dry', 'dataframe_var_name',
                  and flags for 'source' and 'project_id'.
    - cell (str): The SQL query to be executed.
    """
    args = shlex.split(line)
    dry_run = 'dry' in args
    dataframe_var_name = None
    local_source = bigquery_config.get('source')
    local_project_id = bigquery_config.get('project_id')

    # Process optional arguments
    if dry_run:
        args.remove('dry')
    for arg in args:
        if arg.startswith('--source='):
            local_source = arg.split('=')[1]
        elif arg.startswith('--project_id='):
            local_project_id = arg.split('=')[1]
        else:
            dataframe_var_name = arg

    try:
        client = bq.Client(project=local_project_id)
        job_config = bq.QueryJobConfig(dry_run=dry_run, use_query_cache=not dry_run)
        
        formatted_query = cell.format(source=local_source)
        query_job = client.query(formatted_query, job_config=job_config)
        
        if dry_run:
            bytes_processed = query_job.total_bytes_processed
            logger.info(f"Estimated bytes to be processed: {bytes_processed} bytes.")
            cost_per_tb = 5  # Assume $5 per TB as the cost
            estimated_cost = (bytes_processed / (1024**4)) * cost_per_tb
            logger.info(f"Estimated cost of the query: ${estimated_cost:.2f} USD")
        else:
            results = query_job.result()
            dataframe = results.to_dataframe()
            if dataframe_var_name:
                ipython = get_ipython()
                ipython.user_ns[dataframe_var_name] = dataframe
                logger.info(f"Query results stored in DataFrame '{dataframe_var_name}'.")
            else:
                display(dataframe)
    except GoogleAPIError as e:
        if isinstance(e, BadRequest):
            logger.error(f"BadRequest (400): {str(e)} - Check your SQL syntax.")
        elif isinstance(e, Forbidden):
            logger.error(f"Forbidden (403): {str(e)} - You might not have the necessary permissions for the resource.")
        elif isinstance(e, NotFound):
            logger.error(f"NotFound (404): {str(e)} - The specified resource was not found.")
        elif isinstance(e, Conflict):
            logger.error(f"Conflict (409): {str(e)} - A conflict occurred with the existing resource.")
        elif isinstance(e, InternalServerError):
            logger.error(f"InternalServerError (500): {str(e)} - BigQuery encountered an internal error.")
        elif isinstance(e, ServiceUnavailable):
            logger.error(f"ServiceUnavailable (503): {str(e)} - BigQuery service is temporarily unavailable. Try again later.")
        else:
            logger.error(f"GoogleAPIError: {str(e)} - An API error occurred.")
    except Exception as e:
        logger.exception("An unexpected error occurred")


if __name__ == "__main__":
    print('')
    # Code to execute when the module is run as a script
    # For example, test your run_query function here





