#Histogram Plotter


import pandas as pd
import matplotlib.pyplot as plt

# Set up subplots for multiple histograms
num_columns = len(df_trimmed.columns)
num_rows = num_columns // 3 + (1 if num_columns % 3 != 0 else 0)  # 3 histograms per row

fig, axes = plt.subplots(num_rows, 3, figsize=(15, num_rows*3))
axes = axes.flatten()

# Plot histograms for each column
for i, col in enumerate(df_trimmed.columns):
    ax = axes[i]
    df_trimmed[col].plot(kind='hist', ax=ax, bins=20, color='skyblue', edgecolor='black')
    ax.set_title(col)
    ax.grid(True)
    
# Hide empty subplots
for j in range(i+1, len(axes)):
    axes[j].axis('off')

plt.tight_layout()
plt.show()

# import seaborn as sns
# import matplotlib.pyplot as plt
def Pair_Plotter(DataFrame):
  '''
  Iterates over the entire dataframe 1 column at a time.

  '''
  for i in list(DataFrame.columns):
    # Create a PairGrid object
    g = sns.PairGrid(data=trans_sample,
                      y_vars=[i])

    # Map a scatter plot to the upper triangle
    g.map_upper(sns.scatterplot)

    # Hide the lower triangle
    g.map_lower(plt.plot, visible=False)

    # Show the plot
    plt.show()


