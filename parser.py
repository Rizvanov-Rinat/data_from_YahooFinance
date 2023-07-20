import numpy as np
import pandas as pd
from yahooquery import Ticker


def to_dict(x):
    if isinstance(x, dict):
        return x
    else:
        return dict()


def get_52_week_change(ticker_info):
    try:
        historical_prices = ticker_info.history(period="1y", interval="1d")
        current_close = historical_prices.iloc[-1].close
        year_old_close = historical_prices.iloc[0].close
        if len(historical_prices) < 240:
            return None
        else:
            return (current_close - year_old_close) / year_old_close
    except (IndexError, ValueError):
        return None


def get_yahooquery_data(ticker):
    ticker_info = Ticker(ticker)
    summary_detail = to_dict(ticker_info.summary_detail.get(ticker))
    financial_data = to_dict(ticker_info.financial_data.get(ticker))
    key_stats = to_dict(ticker_info.key_stats.get(ticker))
    if not isinstance(key_stats.get('52WeekChange'), float):
        key_stats['52WeekChange'] = get_52_week_change(ticker_info)
    return summary_detail, financial_data, key_stats


def parse(tickers):
    summary_detail_keys = ['previousClose', 'marketCap', 'currency']
    financial_data_keys = ['financialCurrency', 'totalRevenue', 'totalCash', 'totalDebt', 'ebitda',
                           'freeCashflow', 'operatingCashflow', 'grossProfits', 'revenueGrowth']
    key_stats_keys = ['enterpriseValue', 'sharesOutstanding', 'floatShares', 'lastFiscalYearEnd',
                      'profitMargins', '52WeekChange']
    keys = summary_detail_keys + financial_data_keys + key_stats_keys

    counter = 0
    empty_row = pd.DataFrame({key: [np.nan] for key in keys})
    result = pd.DataFrame()

    for ticker in tickers:
        if ticker is np.nan:
            result = pd.concat([result, empty_row]).reset_index(drop=True)
            continue
        try:
            summary_detail, financial_data, key_stats = get_yahooquery_data(ticker)
        except TypeError:
            result = pd.concat([result, empty_row]).reset_index(drop=True)
            continue

        row = dict()
        row.update({key: summary_detail.get(key) for key in summary_detail_keys})
        row.update({key: financial_data.get(key) for key in financial_data_keys})
        row.update({key: key_stats.get(key) for key in key_stats_keys})

        row_df = pd.DataFrame(row, index=[0])
        result = pd.concat([result, row_df]).reset_index(drop=True)

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

    not_dividing = {'previousClose', 'currency', 'financialCurrency', 'lastFiscalYearEnd',
                    'revenueGrowth', 'profitMargins', '52WeekChange'}
    dividing = set(parsed_df.columns.to_list()) - not_dividing
    currencies = ['GBp', 'ZAc', 'ILA']

    for column in parsed_df.columns:
        if column in dividing:
            raw_data[column] = parsed_df[column].apply(devide)
        else:
            raw_data[column] = parsed_df[column]

    raw_data.loc[raw_data.currency.isin(currencies), 'previousClose'] /= 100

    return raw_data
