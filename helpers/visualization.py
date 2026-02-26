import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.transforms import blended_transform_factory


def plot_energy_timeseries(df, title, save_path=None):
    df = df.copy()
    df.index = pd.to_datetime(df.index)  # handling of DatetimeIndex regardless of loading

    # Convert MW to GWh and combine solar + wind into RES
    df["load_gw"] = df["load_forecast_mw"]/ 1000
    df["res_gw"]= (df["solar_forecast_mw"] + df["wind_aggr_mw"])/1000

    # Period boundaries
    train_start = pd.Timestamp("2018-01-01")
    train_end = pd.Timestamp("2019-12-31 23:00")
    val_start= pd.Timestamp("2020-01-01")
    val_end = pd.Timestamp("2020-12-31 23:00")
    qra_start = pd.Timestamp("2021-01-01")
    qra_end = pd.Timestamp("2021-07-02 23:00")
    test_start = pd.Timestamp("2021-07-03")
    test_end = df.index.max()

    def bracket(ax, x0, x1, y_frac, label):
        trans = blended_transform_factory(ax.transData, ax.transAxes)
        mid = (mdates.date2num(x0) + mdates.date2num(x1)) / 2
        ax.annotate("", xy=(mdates.date2num(x1), y_frac), xytext=(mdates.date2num(x0), y_frac),
                    xycoords=trans, textcoords=trans,  arrowprops=dict(arrowstyle="<->", color="black", lw=0.8))
        ax.text(mid, y_frac + 0.04, label, ha="center", va="bottom",transform=trans, fontsize=8.5)

    fig, axes = plt.subplots(5, 1, figsize=(14, 14), sharex=True)
    fig.subplots_adjust(hspace=0.08, top=0.93, bottom=0.10, left=0.10, right=0.97)

    vlines = [val_start, qra_start, test_start]

    axes[0].plot(df.index, df["day_ahead_price_eur_mwh"], color="#7fb3b3", linewidth=0.4)
    axes[0].set_ylabel("DA Price\n(EUR/MWh)", fontsize=9)

    axes[1].fill_between(df.index, df["load_gw"], color="#4a6fa5", alpha=0.85, linewidth=0)
    axes[1].plot(df.index, df["load_gw"], color="#4a6fa5", linewidth=0.3)
    axes[1].set_ylabel("DA Load\nForecast (GWh)", fontsize=9)

    axes[2].fill_between(df.index, df["res_gw"], color="#52a788", alpha=0.85, linewidth=0)
    axes[2].plot(df.index, df["res_gw"], color="#52a788", linewidth=0.3)
    axes[2].set_ylabel("DA RES\nForecast (GWh)", fontsize=9)

    axes[3].plot(df.index, df["co2_first_course_eur"], color="#c47c2b", linewidth=0.8)
    axes[3].set_ylabel("EUA Price\n(EUR/tCO₂)", fontsize=9)

    axes[4].plot(df.index, df["oil_first_course_eur"], color="#8b2e4e", linewidth=0.8, label="Brent Oil Price (EUR/bbl.)")
    axes[4].plot(df.index, df["gas_first_course_eur"], color="#e07b39", linewidth=0.8, label="TTF Gas Price (EUR/MWh)")
    axes[4].set_ylabel("Fuel prices", fontsize=9)
    axes[4].legend(loc="upper center", bbox_to_anchor=(0.5, -0.25), ncol=2, fontsize=9, frameon=False)

    for ax in axes:
        for v in vlines:
            ax.axvline(v, color="black", linestyle="--", linewidth=0.9, alpha=0.8)
        ax.grid(True, linestyle=":", alpha=0.4, linewidth=0.5)
        ax.tick_params(labelsize=8)

    axes[-1].xaxis.set_major_locator(mdates.YearLocator())
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    axes[-1].set_xlabel("Time", fontsize=10)
    axes[-1].set_xlim(df.index.min(), df.index.max())

    bracket(axes[0], train_start, train_end, 1.05, "Training")
    bracket(axes[0],val_start,val_end, 1.05, "Validation")
    bracket(axes[0], qra_start, qra_end, 1.05, "QRA\ncalibration")
    bracket(axes[0], test_start, test_end, 1.05, "Out-of-sample testing")
    bracket(axes[0], train_start, val_end, 1.18, "Hyper-parameter tuning")

    plt.suptitle(title, fontsize=10, y=0.98)

    if save_path:
        fig.savefig(save_path, dpi=150, bbox_inches="tight")

    return fig


