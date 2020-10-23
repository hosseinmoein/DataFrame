import numpy as np
import pandas as pd
from pandas.core.nanops import nanmean as pd_nanmean
from statsmodels.tsa.seasonal import DecomposeResult
from statsmodels.tsa.filters._utils import _maybe_get_pandas_wrapper_freq
import statsmodels.api as sm

# -----------------------------------------------------------------------------

def decompose(df, period=365, lo_frac=0.6, lo_delta=0.01):
    """Create a seasonal-trend (with Loess, aka "STL") decomposition of
    observed time series data. This implementation is modeled after the
    ``statsmodels.tsa.seasonal_decompose`` method but substitutes a Lowess
    regression for a convolution in its trend estimation.
    This is an additive model, Y[t] = T[t] + S[t] + e[t]
    For more details on lo_frac and lo_delta, see:
    `statsmodels.nonparametric.smoothers_lowess.lowess()`
    Args:
        df (pandas.Dataframe):
            Time series of observed counts. This DataFrame must be continuous
            (no gaps or missing data), and include a ``pandas.DatetimeIndex``.
        period (int, optional):
            Most significant periodicity in the observed time series, in units
            of 1 observation. Ex: to accomodate strong annual periodicity
            within years of daily observations, ``period=365``.
        lo_frac (float, optional):
            Fraction of data to use in fitting Lowess regression.
        lo_delta (float, optional):
            Fractional distance within which to use linear-interpolation
            instead of weighted regression. Using non-zero ``lo_delta``
            significantly decreases computation time.
    Returns:
        `statsmodels.tsa.seasonal.DecomposeResult`: An object with DataFrame
        attributes for the seasonal, trend, and residual components, as well
        as the average seasonal cycle.
    """

    # use some existing pieces of statsmodels
    lowess = sm.nonparametric.lowess
    _pandas_wrapper, _ = _maybe_get_pandas_wrapper_freq(df)

    # get plain np array
    observed = np.asanyarray(df).squeeze()

    # calc trend, remove from observation
    trend = lowess(observed, # Y
                   [x for x in range(len(observed))],  # X
                   frac=lo_frac,
                   delta=lo_delta * len(observed),
                   return_sorted=False)
    detrended = observed - trend

    # period must not be larger than size of series to avoid introducing NaNs
    period = min(period, len(observed))

    # calc one-period seasonality, remove tiled array from detrended
    period_averages = np.array([pd_nanmean(detrended[i::period])
                                for i in range(period)])
    # 0-center the period avgs
    period_averages -= np.mean(period_averages)
    seasonal = np.tile(period_averages,
                       len(observed) // period + 1)[:len(observed)]
    resid = detrended - seasonal

    # convert the arrays back to appropriate dataframes, stuff them back into
    #  the statsmodel object
    results = list(map(_pandas_wrapper, [seasonal, trend, resid, observed]))
    dr = DecomposeResult(seasonal=results[0],
                         trend=results[1],
                         resid=results[2],
                         observed=results[3],
                         period_averages=period_averages)
    return dr

# -----------------------------------------------------------------------------

def forecast(stl, fc_func, steps=10, seasonal=False, **fc_func_kwargs):
    """Forecast the given decomposition ``stl`` forward by ``steps`` steps
    using the forecasting function ``fc_func``, optionally including the
    calculated seasonality.
    This is an additive model, Y[t] = T[t] + S[t] + e[t]
    Args:
        stl (a modified statsmodels.tsa.seasonal.DecomposeResult):
            STL decomposition of observed time series created using the
            ``stldecompose.decompose()`` method.
        fc_func (function):
            Function which takes an array of observations and returns a single
            valued forecast for the next point.
        steps (int, optional):
            Number of forward steps to include in the forecast
        seasonal (bool, optional):
            Include seasonal component in forecast
        fc_func_kwargs:
            keyword arguments

        All remaining arguments are passed to the forecasting function
        ``fc_func``
    Returns:
        forecast_frame (pd.Dataframe):
            A ``pandas.Dataframe`` containing forecast values and a
            DatetimeIndex matching the observed index.
    """

    # container for forecast values
    forecast_array = np.array([])

    # forecast trend
    # unpack precalculated trend array stl frame
    trend_array = stl.trend

    # iteratively forecast trend ("seasonally adjusted") component
    # note: this loop can be slow
    for step in range(steps):
        # make this prediction on all available data
        pred = fc_func(np.append(trend_array, forecast_array), **fc_func_kwargs)
        # add this prediction to current array
        forecast_array = np.append(forecast_array, pred)
    col_name = fc_func.__name__

    # forecast start and index are determined by observed data
    observed_timedelta = stl.observed.index[-1] - stl.observed.index[-2]
    forecast_idx_start = stl.observed.index[-1] + observed_timedelta
    forecast_idx = pd.date_range(
        start=forecast_idx_start,
        periods=steps,
        freq=pd.tseries.frequencies.to_offset(observed_timedelta))

    # (optionally) forecast seasonal & combine
    if seasonal:
        # track index and value of max correlation
        seasonal_idx = 0
        max_correlation = -np.inf
        # loop over indexes=length of period avgs
        detrended_array = np.asanyarray(stl.observed - stl.trend).squeeze()
        for idx, x in enumerate(stl.period_averages):
            # work slices backward from end of detrended observations
            if idx == 0:
                # slicing w/ [x:-0] doesn't work
                detrended_slice = detrended_array[-len(stl.period_averages):]
            else:
                detrended_slice = \
                    detrended_array[-(len(stl.period_averages) + idx):-idx]

            # calculate corr b/w period_avgs and detrend_slice
            this_correlation = np.correlate(detrended_slice,
                                            stl.period_averages)[0]
            if this_correlation > max_correlation:
                # update idx and max correlation
                max_correlation = this_correlation
                seasonal_idx = idx

        # roll seasonal signal to matching phase
        rolled_period_averages = np.roll(stl.period_averages, -seasonal_idx)
        # tile as many time as needed to reach "steps", then truncate
        tiled_averages = \
            np.tile(rolled_period_averages,
                    (steps // len(stl.period_averages) + 1))[:steps]
        # add seasonal values to previous forecast
        forecast_array += tiled_averages
        col_name += '+seasonal'

    # combine data array with index into named dataframe
    forecast_frame = pd.DataFrame(data=forecast_array, index=forecast_idx)
    forecast_frame.columns = [col_name]
    return forecast_frame

# -----------------------------------------------------------------------------

# example one-step-ahead forecasting functions
# implementations based on https://www.otexts.org/fpp

def naive(data, **kwargs):
    """The naive forecast for the next point is the value of the previous
    point. In most forecasting situations, the naive model is a good baseline
    due to it's simplicity and low computational overhead.
    Args:
        data (np.array): Observed data, presumed to be ordered in time.
    Returns:
        float: a single-valued forecast for the next value in the series.
    """

    forecast = data[-1]
    return forecast

# -----------------------------------------------------------------------------

def seasonal_naive(data, n=7, **kwargs):
    """The seasonal naive forecast for the next point is the value observed
    ``n`` points prior in the series.
    The seasonal parameter (``n``) does not have units of time, but rather
    units of one observation. For example, to account for weekly cycles within
    daily observations, ``n=7``.
    Args:
        data (np.array): Observed data, presumed to be ordered in time.
        n (int): period of data seasonality
    Returns:
        float: a single-valued forecast for the next value in the series.
    """

    forecast = data[-n]
    return forecast

# -----------------------------------------------------------------------------

def mean(data, n=3, **kwargs):
    """The mean forecast for the next point is the mean value of the previous
    ``n`` points in the series.
    Args:
        data (np.array): Observed data, presumed to be ordered in time.
        n (int): period over which to calculate the mean
    Returns:
        float: a single-valued forecast for the next value in the series.
    """

    # don't start averaging until we've seen n points
    if len(data[-n:]) < n:
        forecast = np.nan
    else:
        # nb: we'll keep the forecast as a float
        forecast = np.mean(data[-n:])
    return forecast

# -----------------------------------------------------------------------------

def drift(data, n=3, **kwargs):
    """The drift forecast for the next point is a linear extrapolation from
    the previous ``n`` points in the series.
    Args:
        data (np.array): Observed data, presumed to be ordered in time.
        n (int): period over which to calculate linear model for extrapolation
    Returns:
        float: a single-valued forecast for the next value in the series.
    """

    yi = data[-n]
    yf = data[-1]
    slope = (yf - yi) / (n - 1)
    forecast = yf + slope
    return forecast

# -----------------------------------------------------------------------------

