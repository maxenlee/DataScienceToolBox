How to Import to Python
```
! curl -s -O 'https://raw.githubusercontent.com/maxenlee/DataScienceToolBox/main/ToolBox.py'
from ToolBox import DataFrameAnalyzer
```
i.e. For the Analyze Function
```
#if DataFrame is in a dictionary
dict_df["df_000"].df_kit.analyze()
```
```
df_summ = pd.DataFrame()
for i in dict_df:
  df_summ[i] = dict_df[i].df_kit.analyze()
df_summ.transpose()
```



# ToolBox
Toolbox for development

Tools under development

Data Science
  
Data Collection
 - ETL
   -![Feature Selection](https://miro.medium.com/v2/resize:fit:720/format:webp/1*tzfWABEHK9-4SOaSl1mdRA.png)


   
 -High-Level Overview of DataSet(dictionary of data sets)
    - Count rows in df
    - Count Columns in df
    - Count Nulls
    - Memory Size
    - 
  - get from storage in databases
  - web scraping collection
    
  
Data Cleaning & Regularization
  - Drop column threshold
    -If null in data > 50%
    -If null is Target, drop df.
  - Stats
    -count data types
    -count nulls
  - count 

EDA
  - classify the target
  - Find Categorical
      -get dummies
  - Find Numerical
     -   

Data processing:
  - train_test_split to dictionary
  - model fitter
  - predictor

Data Visualization:
  - Looping through visualizations
  - side by side visualizations
  - Viz library
  - 

Recursion 


 
