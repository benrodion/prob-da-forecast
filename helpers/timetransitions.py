import pandas as pd
def fix_dst_transitions(df):
    """
    Helper that reduce duplicated values from switch to daylight saving time to one value.
    Imputes missing value from switch from daylight saving time as mean of the surrounding 2 values

    """
    df = df.copy().sort_index()
    
    #remove duplicates (swtich to daylight saving time)
    df = df[~df.index.duplicated(keep='first')]
    
    # impute missing values (switch to spring time)
    full_range = pd.date_range(
        start=df.index.min(),
        end=df.index.max(),
        freq=pd.infer_freq(df.index), 
        tz=df.index.tz  
    )
    
    df = df.reindex(full_range).interpolate(method='linear', limit=1)
    
    return df