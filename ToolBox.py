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


from google.cloud import bigquery as bq
from IPython.core.magic import register_cell_magic
from IPython import get_ipython
from IPython.display import display
import pandas as pd
import shlex  # For safely splitting the argument line

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
    
    # Parse the line for arguments
    args = shlex.split(line)
    dry_run = 'dry' in args
    if dry_run:
        args.remove('dry')
    
    # The DataFrame variable name could be provided after 'dry' or as the first argument
    dataframe_var_name = args[0] if args else None
    
    # Parse for potential keyword arguments
    args_dict = {k: v for k, v in (arg.split('=') for arg in args if '=' in arg)}
    
    # Optionally override source and project_id if provided in the magic command line
    source = args_dict.get('source', source)
    project_id = args_dict.get('project_id', project_id)
    
    client = bq.Client(project=project_id)
    job_config = bq.QueryJobConfig(dry_run=dry_run, use_query_cache=not dry_run)
    
    # Apply formatting with the configured source
    formatted_query = cell.format(source=source)
    query_job = client.query(formatted_query, job_config=job_config)
    
    if dry_run:
        bytes_processed = query_job.total_bytes_processed
        print(f"Estimated bytes to be processed: {bytes_processed} bytes.")
        cost_per_tb = 5  # Update this based on the current rate as necessary
        estimated_cost = (bytes_processed / (1024**4)) * cost_per_tb
        print(f"Estimated cost of the query: ${estimated_cost:.2f} USD")
        
        if dataframe_var_name:
            # Create an empty DataFrame for structural purposes during dry runs
            globals()[dataframe_var_name] = pd.DataFrame()
            print(f"Empty DataFrame '{dataframe_var_name}' created for dry run.")
    else:
        try:
            results = query_job.result()
            dataframe = results.to_dataframe()
            if dataframe_var_name:
                # Assign the results to a DataFrame with the specified name
                globals()[dataframe_var_name] = dataframe
                print(f"Query results stored in DataFrame '{dataframe_var_name}'.")
            else:
                except Exception as e:
                    print(f"An error occurred: {e}")
                
            






@register_cell_magic
def bigquery(line, cell):
    args = line.split()
    dry_run = 'dry' in args
    dataframe_var_name = None
    
    # Assuming project_id and source are defined globally or fetched from configuration
    global project_id, source
    
    if dry_run:
        args.remove('dry')
    if args:
        dataframe_var_name = args[0]  # Assuming the first argument is the DataFrame name if present

    client = bq.Client(project=project_id)
    job_config = bq.QueryJobConfig(dry_run=dry_run, use_query_cache=not dry_run)
    
    formatted_query = cell.format(source=source)
    query_job = client.query(formatted_query, job_config=job_config)
    
    if dry_run:
        bytes_processed = query_job.total_bytes_processed
        print(f"Estimated bytes to be processed: {bytes_processed} bytes.")
        cost_per_tb = 5  # Assume $5 per TB as the cost
        estimated_cost = (bytes_processed / (1024**4)) * cost_per_tb
        print(f"Estimated cost of the query: ${estimated_cost:.2f} USD")
    else:
        try:
            results = query_job.result()
            dataframe = results.to_dataframe()
            if dataframe_var_name:
                # Assign the results to a DataFrame with the specified name
                ipython = get_ipython()
                ipython.user_ns[dataframe_var_name] = dataframe
                print(f"Query results stored in DataFrame '{dataframe_var_name}'.")
            else:
                # If no DataFrame name provided, display results
                display(dataframe)
        except Exception as e:
            print(f"An error occurred: {e}")

if __name__ == "__main__":
    print('')
    # Code to execute when the module is run as a script
    # For example, test your run_query function here





