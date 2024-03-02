
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
import json
from google.cloud import bigquery as bq
from IPython.core.magic import register_cell_magic
from IPython.display import display, JSON
import pandas as pd
import shlex
from google.api_core.exceptions import GoogleAPIError
import logging
from ipywidgets import widgets
import os


# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('BigQueryMagic')


bigquery_config = {
    'source': 'default-source',
    'project_id': 'default-project-id',
}

def configure_bigquery(source=None, project_id=None):

    global bigquery_config
    if source:
        bigquery_config['source'] = source
    if project_id:
        bigquery_config['project_id'] = project_id

@register_cell_magic
def bigquery(line, cell):

    args = shlex.split(line)
    dry_run = 'dry' in args
    dataframe_var_name = None
    output_file = None
    params = {}
    
    # Extract and remove known arguments
    args = [arg for arg in args if not process_known_args(arg)]

    # Remaining args processing
    if args:
        if not dry_run:  # Assume first arg is the DataFrame name if not a dry run
            dataframe_var_name = args[0]
    
    try:
        client = bq.Client(project=bigquery_config['project_id'])
        job_config = bq.QueryJobConfig(dry_run=dry_run, use_query_cache=not dry_run, query_parameters=params)
        
        formatted_query = cell.format(source=bigquery_config['source'])
        query_job = client.query(formatted_query, job_config=job_config)
        
        if dry_run:
            handle_dry_run(query_job)
        else:
            handle_query_execution(query_job, dataframe_var_name, output_file)
    except GoogleAPIError as e:
        logger.error(f"GoogleAPIError: {str(e)}")
    except Exception as e:
        logger.exception("An unexpected error occurred")

def process_known_args(arg):
    global params, output_file
    if arg.startswith('--source='):
        bigquery_config['source'] = arg.split('=')[1]
        return True
    elif arg.startswith('--project_id='):
        bigquery_config['project_id'] = arg.split('=')[1]
        return True
    elif arg.startswith('--params='):
        params_str = arg.split('=')[1]
        params = json.loads(params_str)
        return True
    elif arg.startswith('--output_file='):
        output_file = arg.split('=')[1]
        return True
    elif arg == 'dry':
        return True
    return False

def handle_query_execution(query_job, dataframe_var_name, output_file):
    results = query_job.result()
    # Define the default path as /content if only a filename is provided
    default_path = "/content/"
    
    if output_file:
        # If output_file doesn't contain a path, prepend the default_path
        if not os.path.dirname(output_file):
            output_file = os.path.join(default_path, output_file)
        
        # Ensure the directory exists, create if it doesn't
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Save the query results to the specified file
        results.to_dataframe().to_csv(output_file)
        logger.info(f"Query results stored in {output_file}")
    elif dataframe_var_name:
        dataframe = results.to_dataframe()
        ipython = get_ipython()
        ipython.user_ns[dataframe_var_name] = dataframe
        logger.info(f"Query results stored in DataFrame '{dataframe_var_name}'.")
    else:
        display(results.to_dataframe())



if __name__ == "__main__":
    print('')
    # Code to execute when the module is run as a script
    # For example, test your run_query function here


