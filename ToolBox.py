
# Ensure that the BigQuery client is initialized
client = bigquery.Client(project='your-project-id')
from google.cloud import bigquery as bq
from IPython.core.magic import register_cell_magic
from IPython.display import display
from google.cloud import bigquery
import pandas as pd
from google.cloud import bigquery as bq
from IPython.core.magic import register_cell_magic
from IPython.display import display
import shlex  # For safely splitting the argument line



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






# Necessary imports for defining cell magic
from IPython.core.magic import register_cell_magic
from google.cloud import bigquery as bq
from IPython.display import display

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
    # Use the global configuration for source and project_id
    source = bigquery_config.get('source')
    project_id = bigquery_config.get('project_id')
    dry_run = 'dry' in line.split()  # Check if 'dry' is in the command line arguments
    args_dict = {k: v for k, v in (arg.split('=') for arg in line.split() if '=' in arg)}
    gb = (1024**3)
    tb = (1024**4)
    
    # Optionally override source and project_id if provided in the magic command line
    source = args_dict.get('source', source)
    project_id = args_dict.get('project_id', project_id)

    formatted_query = cell.format(source=source)  # Apply string formatting here
    client = bq.Client(project=project_id)
    

    if dry_run:
      
        job_config = bq.QueryJobConfig(maximumBytesBilled = gb,dry_run=dry_run, use_query_cache=not dry_run)
        query_job = client.query(formatted_query, job_config=job_config)  # Use the formatted query
        
        bytes_processed = query_job.total_bytes_processed
        print(f"Estimated bytes to be processed: {bytes_processed} bytes.")

        # Assume $5 per TB for cost calculations; adjust as needed
        cost_per_tb = 5  
        estimated_cost = (bytes_processed / tb) * cost_per_tb
        print(f"Estimated cost of the query: ${estimated_cost:.2f} USD")
    else:
        try:
            job_config = bq.QueryJobConfig(dry_run=dry_run, use_query_cache=not dry_run)
            query_job = client.query(formatted_query, job_config=job_config)  # Use the formatted query
            results = query_job.result()  # Waits for the query to finish
            display(results.to_dataframe())
        except Exception as e:
            print(f"An error occurred: {e}")


