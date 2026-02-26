import pandas as pd

def clean_stock_data(df):
    """
    Takes stock data (oil, co2 allowances, gas) as inputs.
    Converts the "Datum" column to datetime, sets it as index, casts the price data as float and
    drops unneccessary columns
    """

    df = df.copy()
    
    df['date'] = pd.to_datetime(df['Datum'])
    df = df.set_index('date')
    
    
    df['first_course_eur'] = df['Erster'].str.replace(',', '.').astype(float)
    df['last_course_eur'] = df['Schlusskurs'].str.replace(',', '.').astype(float)
    
    
    df = df.drop(columns=['Hoch', 'Tief', 'Schlusskurs', 'Stuecke', 'Volumen', 'Datum', 'Erster'])
    
    return df

def clean_ttf_data(df):
    df = df.copy()
    df['date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y')
    df = df.set_index('date')
    df['first_course_eur'] = df['Open'].astype(float)
    df['last_course_eur'] = df['Price'].astype(float)
    df = df.drop(columns=['Date', 'Open', 'High', 'Low', 'Vol.', 'Change %', 'Price'], errors='ignore')  
    return df


def impute_weekends(df):
    """
    Fill in missing data for weekends & holidays.
    """

    #create full date range
    full_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq='D')
    df = df.reindex(full_range)

    for date in df.index:
        if df.loc[date].isna().all():  #also holidays on weekdays
            candidate = date - pd.Timedelta(days=1)
            while candidate in df.index:
                if not df.loc[candidate].isna().all():
                    # impute
                    df.loc[date, 'first_course_eur'] = df.loc[candidate, 'last_course_eur']
                    df.loc[date, 'last_course_eur'] = df.loc[candidate, 'last_course_eur']
                    break
                candidate -= pd.Timedelta(days=1)

    return df