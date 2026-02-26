# descriptive visualization of the cleaned data
from helpers.visualization import plot_energy_timeseries
from pathlib import Path
import pandas as pd

file_path=Path(__file__).parents[1] / 'data/processed/ger_merged.csv'
save_path=Path(__file__).parents[1] / 'results/figures/ger_descr_plot.png'
data_ger = pd.read_csv(file_path, index_col=0 )

# make plot for Germany
ger_descr_plot = plot_energy_timeseries(data_ger, title='Time Series Data Germany', save_path=save_path)

# and for Spain 
file_path=Path(__file__).parents[1] / 'data/processed/es_merged.csv'
save_path=Path(__file__).parents[1] / 'results/figures/es_descr_plot.png'
data_es = pd.read_csv(file_path, index_col=0)

# make plot for Germany
es_descr_plot = plot_energy_timeseries(data_es, title='Time Series Data Spain', save_path=save_path)