import pandas as pd
from entsoe import EntsoePandasClient
from dotenv import load_dotenv
from pathlib import Path
import os

# set up client
load_dotenv(dotenv_path=Path(__file__).parents[2] / 'config/.env')
API_KEY = os.getenv('API_KEY')
client = EntsoePandasClient(api_key=API_KEY)

# periods need to be split because of bidding zone change in 2018
periods = [
    ('20141201', '20181001', 'DE_AT_LU'), # artefact
    ('20181001', '20251001', 'DE_LU'),
]

results = {
    'day_ahead_prices': [],
    'load_forecast':[],
    'wind_solar_forecast':[],
}

for start_str, end_str, country_code in periods:
    start = pd.Timestamp(start_str, tz= 'Europe/Berlin')
    end = pd.Timestamp(end_str, tz='Europe/Berlin')
    print(f'Lade Daten für {country_code}: {start_str} – {end_str} ...')

    results['day_ahead_prices'].append(
        client.query_day_ahead_prices(country_code, start=start, end=end)
    )
    results['load_forecast'].append(
        client.query_load_forecast(country_code, start= start, end=end)
    )
    results['wind_solar_forecast'].append(
        client.query_wind_and_solar_forecast(country_code, start=start, end=end, psr_type=None)
    )
# combine periods
day_ahead_prices = pd.concat(results['day_ahead_prices']).sort_index()
load_forecast = pd.concat(results['load_forecast']).sort_index()
wind_solar_forecast = pd.concat(results['wind_solar_forecast']).sort_index()

# resample to hourly
day_ahead_prices= day_ahead_prices.resample('h').mean()
load_forecast = load_forecast.resample('h').mean()
wind_solar_forecast = wind_solar_forecast.resample('h').mean()

# build combined DataFrame
df = pd.DataFrame({'day_ahead_price_eur_mwh': day_ahead_prices})
df = df.join(load_forecast.add_suffix('_mw'))
df = df.join(wind_solar_forecast.add_suffix( '_mw'))

print(df.head())
print(df.columns.tolist())
print(df.shape)

# save file 
df.to_csv('data/raw/entsoe_germany_2015_2025.csv')