import pandas as pd
def validate_merged(merged_df, entsoe_path, label):
    """
    Checks if after the cleaning, the price and forecast data still matches the raw entsoe data
    """
    print(f"\n{'='*55}")
    print(f"VALIDATION: {label}")
    print(f"{'='*55}")

    entsoe = pd.read_csv(entsoe_path, index_col=0, parse_dates=True)
    entsoe.index = pd.to_datetime(entsoe.index, utc=True).tz_convert('UTC').tz_localize(None)
    entsoe = entsoe.rename(columns={
        'day_ahead_price_eur_mwh': 'price_e',
        'Forecasted Load_mw': 'load_e',
        'Solar_mw': 'solar_e',
        'Wind Onshore_mw': 'wind_on_e',
        'Wind Offshore_mw': 'wind_off_e',
    })

    m = merged_df.rename(columns={
        'day_ahead_price_eur_mwh': 'price_m',
        'load_forecast_mw': 'load_m',
        'solar_forecast_mw': 'solar_m',
        'wind_aggr_mw': 'wind_m',
    })

    df = entsoe.join(m[['price_m','load_m','solar_m','wind_m']], how='inner')

    if 'wind_off_e' in df.columns:
        df['wind_e'] = df['wind_on_e'].fillna(0) + df['wind_off_e'].fillna(0)
    else:
        df['wind_e'] = df['wind_on_e']

    pairs = [
        ('price_e', 'price_m','Day-Ahead Price [€/MWh]'),
        ('load_e','load_m', 'Load Forecast [MW]'),
        ('solar_e', 'solar_m', 'Solar [MW]'),
        ('wind_e', 'wind_m', 'Wind (aggr.)[MW]'),
    ]

    all_ok = True
    for col_e, col_m, name in pairs:
        if col_e not in df or col_m not in df:
            print(f"  {name}: SKIP (column missing)")
            continue
        valid = df[col_e].notna() & df[col_m].notna()
        diff= (df[col_e] - df[col_m])[valid]
        mae = diff.abs().mean()
        pct = (diff.abs() < 0.01).mean() * 100
        n_bad = (diff.abs() >= 0.01).sum()
        status = "OK" if mae < 0.01 else f"MISMATCH ({n_bad} rows)"
        print(f"  {name}  MAE={mae:.4f}  exact={pct:.1f}%  {status}")
        if mae >= 0.01:
            all_ok = False
            print(f" Biggest issues:")
            print(diff.abs().nlargest(5).to_string(header=False))

    print(f"\n  Overlap: {df.index.min().date()} and {df.index.max().date()}  ({len(df)} rows)")
    print(f"  Result:  {'ALL CHECKS PASSED!' if all_ok else 'ISSUES FOUND! – see above'}")
