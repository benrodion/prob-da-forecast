import pandas as pd
def add_lagged_price_features(df):
    """
    helper to create the lagged price data needed for the expert model.
    """
    df = df.copy()
    price = df['day_ahead_price_eur_mwh']
    
    # same hour, d-1 / d-2 / d-7
    df['price_d_1_h_eur_mwh'] = price.shift(freq=pd.Timedelta('1 day')) 
    df['price_d_2_h_eur_mwh'] = price.shift(freq=pd.Timedelta('2 days'))
    df['price_d_7_h_eur_mwh'] = price.shift(freq=pd.Timedelta('7 days'))
    
    # price for last hour of the previous day
    df['price_d_1_24_eur_mwh'] = price.shift(1).where(df.index.hour == 0).ffill()
    
    #min/max of previous day
    daily_min = price.resample('D').min()
    daily_max = price.resample('D').max()
    
    # forward fill the min/max values
    df['price_d_1_min_eur_mwh'] = daily_min.shift(1).reindex(df.index, method='ffill')
    df['price_d_1_max_eur_mwh'] = daily_max.shift(1).reindex(df.index, method='ffill')
    
    return df