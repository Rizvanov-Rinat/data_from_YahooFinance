import pandas as pd
import numpy as np
from yahooquery import Ticker
# import yfinance as yf
import datetime as dt


def parse(tickers):

    def to_dict(x):
        if isinstance(x, dict):
            return x
        else:
            return dict()

    summary_detail_keys = ['previousClose', 'marketCap', 'currency']
    financial_data_keys = ['totalRevenue', 'totalCash', 'totalDebt', 'ebitda', 'freeCashflow', 'operatingCashflow',
                           'grossProfits', 'revenueGrowth']
    key_stats_keys = ['enterpriseValue', 'sharesOutstanding', 'floatShares', 'lastFiscalYearEnd', 'profitMargins']
    keys = summary_detail_keys + financial_data_keys + key_stats_keys + ['growthFromFeb']

    counter = 0
    empty_row = pd.DataFrame({key: [np.nan] for key in keys})
    result = pd.DataFrame()
    for ticker in tickers:

        if ticker is np.nan:
            result = result.append(empty_row, ignore_index=True)
            continue

        try:
            ticker_info = Ticker(ticker)
        except TypeError:
            result = result.append(empty_row, ignore_index=True)
            continue

        summary_detail = to_dict(ticker_info.summary_detail.get(ticker))
        financial_data = to_dict(ticker_info.financial_data.get(ticker))
        key_stats = to_dict(ticker_info.key_stats.get(ticker))

        row = dict()
        row.update({key: summary_detail.get(key) for key in summary_detail_keys})
        row.update({key: financial_data.get(key) for key in financial_data_keys})
        row.update({key: key_stats.get(key) for key in key_stats_keys})

        try:
            price = ticker_info.history(start=dt.date(2020, 2, 4), end=dt.date(2020, 2, 7)).close[0]
            # price = yf.download(ticker, start=dt.date(2020, 2, 4), end=dt.date(2020, 2, 7), progress=False).Close[0]
            row['growthFromFeb'] = row['previousClose'] / price - 1
        except:
            row['growthFromFeb'] = np.nan

        row_df = pd.DataFrame(row, index=[0])
        result = result.append(row_df)

        counter += 1
        if counter % 100 == 0:
            print(f'{counter} tickers downloaded')

    result.reset_index(drop=True, inplace=True)

    return result


def normalization_data(parsed_df, raw_data):

    def devide(x):
        try:
            return float(x) / 1000000
        except TypeError:
            return x

    not_dividing = {'previousClose', 'currency', 'lastFiscalYearEnd', 'revenueGrowth', 'profitMargins', 'growthFromFeb'}
    dividing = set(parsed_df.columns.to_list()) - not_dividing
    currencies = ['GBp', 'ZAc', 'ILA']

    for column in parsed_df.columns:
        if column in dividing:
            raw_data[column] = parsed_df[column].apply(devide)
        else:
            raw_data[column] = parsed_df[column]

    raw_data.loc[raw_data.currency.isin(currencies), 'previousClose'] /= 100

    # for column in (dividing + ['previousClose']):
    #     raw_data.loc[raw_data.currency.isin(currencies), column] /= 100

    return raw_data
