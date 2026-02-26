import pandas as pd
from helpers.timetransitions import fix_dst_transitions
from helpers.price_lags import add_lagged_price_features
from pathlib import Path

####### Germany  ####### 

file_path=Path(__file__).parents[1] / 'raw/entsoe_germany_2015_2025.csv'
#rename columns
data_ger = pd.read_csv(file_path)

data_ger.rename(columns={'Forecasted Load_mw': 'load_forecast_mw', 'Solar_mw': 'solar_forecast_mw', 
                         'Wind Offshore_mw': 'offshore_forecast_mw', 'Wind Onshore_mw': 'onshore_forecast_mw' }, inplace=True)

# sum up on- and offshore generation
data_ger['wind_aggr_mw'] = data_ger['offshore_forecast_mw'].add(data_ger['onshore_forecast_mw'], 
                                                                fill_value=0) # fill in a 0 for NAs in onshore_forecast_mw

# if there are NAs in both wind cols, set value in aggregated col to NA as well
data_ger.loc[data_ger['offshore_forecast_mw'].isna() & data_ger['onshore_forecast_mw'].isna(), 'wind_aggr_mw'] = pd.NA

# remove superfluous cols
data_ger = data_ger.drop(['offshore_forecast_mw', 'onshore_forecast_mw'], axis=1)

# set index col to datetime format so I can work with it as time series
data_ger['Unnamed: 0'] = pd.to_datetime(data_ger['Unnamed: 0'], utc=True)
data_ger = data_ger.set_index('Unnamed: 0')
data_ger.index.name = 'datetime'  #

# remedy issues from switches to/from daylight saving time
data_ger = fix_dst_transitions(data_ger)

# add lagged price data to df and export to csv
data_ger_lagged = add_lagged_price_features(data_ger)
data_ger_lagged.to_csv('data/processed/data_ger_lagged.csv')


####### Spain  ####### 
file_path=Path(__file__).parents[1] / 'raw/entsoe_spain_2015_2025.csv'
#rename columns
data_es = pd.read_csv(file_path)

data_es.rename(columns={'Forecasted Load_mw': 'load_forecast_mw', 
                        'Solar_mw': 'solar_forecast_mw', 'Wind Onshore_mw': 'wind_aggr_mw'}, inplace=True)

# set index col to datetime format
data_es['Unnamed: 0'] = pd.to_datetime(data_es['Unnamed: 0'], utc=True)
data_es = data_es.set_index('Unnamed: 0')
data_es.index.name = 'datetime'  #

# remedy issues from switches to/from daylight saving time
data_es = fix_dst_transitions(data_es)

# and finally, add lagged data, remove 2014 data and save
data_es_lagged = add_lagged_price_features(df=data_es)
data_es_lagged = data_es_lagged.loc['2015-01-01':]
data_es_lagged.to_csv('data/processed/data_es_lagged.csv')



####### Commodity data  ####### 
from helpers.stock_data import clean_stock_data, impute_weekends, clean_ttf_data

paths = [Path(__file__).parents[1] / 'raw/CO_2_allowances_2015_2025.csv', Path(__file__).parents[1] / 'raw/oil_2015_2025.csv']
path_ttf = Path(__file__).parents[1] / 'raw/ttf_gas_2017_2025.csv' # separate solution needed because data is from different source
name_ttf = 'data/processed/gas_clean.csv'
names = ['data/processed/co2_allowances_clean.csv', 'data/processed/oil_clean.csv']

for file_path, name in zip(paths, names):
    data = pd.read_csv(file_path, sep=';')
    data = clean_stock_data(data)
    data = impute_weekends(data)
    data.to_csv(name)

# separate ttf clean-up
data = pd.read_csv(path_ttf)
data = clean_ttf_data(data)
data = impute_weekends(data)
data.to_csv(name_ttf)

##### merge commodity data with the data for Germany/Spain
# load the CSVs we've written
co2 = pd.read_csv('data/processed/co2_allowances_clean.csv', index_col=0, parse_dates=True)
oil = pd.read_csv('data/processed/oil_clean.csv', index_col=0, parse_dates=True)
gas = pd.read_csv('data/processed/gas_clean.csv', index_col=0, parse_dates=True)
ger = pd.read_csv('data/processed/data_ger_lagged.csv', index_col=0, parse_dates=True)
es = pd.read_csv('data/processed/data_es_lagged.csv', index_col=0, parse_dates=True)

#perform merge

co2['date'] = pd.to_datetime(co2.index).normalize()
oil['date'] = pd.to_datetime(oil.index).normalize()
gas['date'] = pd.to_datetime(gas.index).normalize()
# ensure column identifiability
co2 = co2.rename(columns=lambda c: f'co2_{c}' if c != 'date' else c)
oil = oil.rename(columns=lambda c: f'oil_{c}' if c != 'date' else c)
gas = gas.rename(columns=lambda c: f'gas_{c}' if c != 'date' else c)

def merge_commodities(entsoe_df):
    entsoe_df = entsoe_df.copy()
    entsoe_df.index = pd.to_datetime(entsoe_df.index).tz_localize(None) # remove UTC
    original_index = entsoe_df.index
    entsoe_df['date'] = entsoe_df.index.normalize()

    for commodity_df in [co2, oil, gas]:
        entsoe_df = entsoe_df.merge(commodity_df, on='date', how='left')

    entsoe_df = entsoe_df.drop(columns=['date', 'co2_last_course_eur', 'oil_last_course_eur', 'gas_last_course_eur'])
    entsoe_df.index = original_index
    return entsoe_df

es_merged = merge_commodities(es)
ger_merged = merge_commodities(ger)


#and remove all rows with missing values
# for Germany: around 1100 due to missing load forecasts
# for Spain: around 150 due to missing RES forecasts
es_merged = es_merged.dropna()
es_merged = es_merged.loc['01-01-2018':]
ger_merged = ger_merged.dropna()
ger_merged = ger_merged.loc['01-01-2018':]

ger_merged.to_csv('data/processed/ger_merged.csv')
es_merged.to_csv('data/processed/es_merged.csv')


####### Validation: compare merged output against raw ENTSO-E source data #######
import numpy as np

from helpers.validation import validate_merged 


validate_merged(ger_merged, 'data/raw/entsoe_germany_2015_2025.csv', 'Germany (ger_merged)')
validate_merged(es_merged, 'data/raw/entsoe_spain_2015_2025.csv', 'Spain (es_merged)')