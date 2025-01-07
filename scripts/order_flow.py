import pandas as pd
import numpy as np
import argparse
import os
import glob

def calculate_bid_order_flow(df, level):
    price_col = f'bid_px_0{level}'
    qty_col = f'bid_sz_0{level}'
    price_delta = df[price_col] - df[price_col].shift(1)
    qty_delta = df[qty_col] - df[qty_col].shift(1)
    conditions = [
        (price_delta > 0),   # price_delta > 0
        (price_delta < 0),   # price_delta < 0
        (price_delta == 0)
    ]
    # Define the corresponding values for each condition
    values = [
        df[qty_col],            # if price_delta > 0, OF = quantity
        -df[qty_col].shift(1),           # if price_delta < 0, OF = -quantity
        qty_delta
    ]
    # Apply the conditions and values to create the "OF" column
    order_flow = np.select(conditions, values, default=qty_delta)
    order_flow[0] = 0
    return order_flow

def calculate_ask_order_flow(df, level):
    price_col = f'ask_px_0{level}'
    qty_col = f'ask_sz_0{level}'
    price_delta = df[price_col] - df[price_col].shift(1)
    qty_delta = df[qty_col] - df[qty_col].shift(1)
    conditions = [
        (price_delta > 0),   # price_delta > 0
        (price_delta < 0),   # price_delta < 0
        (price_delta == 0)
    ]
    # Define the corresponding values for each condition
    values = [
        -df[qty_col],            # if price_delta > 0, OF = quantity
        df[qty_col].shift(1),           # if price_delta < 0, OF = -quantity
        -qty_delta
    ]
    # Apply the conditions and values to create the "OF" column
    order_flow = np.select(conditions, values, default=qty_delta)
    order_flow[0] = 0
    return order_flow

def calculate_level_ofi(df, level_num):
    # Calculate order flow imbalance for a given level
    bid_order_flow = calculate_bid_order_flow(df, level_num)
    ask_order_flow = calculate_ask_order_flow(df, level_num)
    df[f'of_diffs_{level_num}'] = bid_order_flow - ask_order_flow
    df[f'order_volume_{level_num}'] = df[f'bid_sz_0{level_num}'] + df[f'ask_sz_0{level_num}']
    df.set_index('ts_event', inplace=True)
    minute_aggregated = df.resample('min')[f'of_diffs_{level_num}', f'order_volume_{level_num}'].sum().reset_index()
    df.reset_index(inplace=True)
    df = df.drop([f'of_diffs_{level_num}', f'order_volume_{level_num}'], axis=1)
    return minute_aggregated

def calculate_event_counts(df):
    # Set the 'ts_event' column as the index
    df.reset_index(inplace=True)
    df.set_index('ts_event', inplace=True)
    # Resample the data by minute and count total number of order book updates
    minute_aggregated = df.resample('min').count().reset_index()['index']
    df.reset_index(inplace=True)
    return minute_aggregated

def calculate_returns(df):
    # Calculate log return as defined in equation (5)
    temp_df = df.set_index('ts_event').sort_index()
    temp_df['mid_price'] = (temp_df['bid_px_00'] + temp_df['ask_px_00']) / 2
    mid_prices = (
        temp_df.resample('min')['mid_price']
        .agg(start='first', end='last')
        .reset_index()
    )
    log_returns = np.log(mid_prices['end'] / mid_prices['start'])
    return mid_prices['end'] - mid_prices['start'], log_returns

def calculate_normalized_ofi(book, max_levels):
    '''
    Calculate OFIs with appropriate normalization as defined in paper (at minute aggregation).
    Arguments:
        - book: DataFrame reprsenting MBP-10 order book
        - max_levels: number of levels to consider (up to 10)
    '''
    ofi_levels = []
    ofi_levels = calculate_level_ofi(book, 0)
    for i in range(1, max_levels):
        level_i = calculate_level_ofi(book, i)
        ofi_levels = pd.merge(ofi_levels, level_i, on='ts_event', how='inner')
    ofi_levels['event_count'] = calculate_event_counts(book)
    ofi_levels = ofi_levels[ofi_levels['event_count'] > 0]
    average_volume = (1 / max_levels) * ofi_levels[[f'order_volume_{i}' for i in range(max_levels)]].sum(axis=1)
    QM = 0.5 * (1 / ofi_levels['event_count']) * average_volume
    # normalized_ofis = ofi_levels[[f'of_diffs_{i}' for i in range(max_levels)]].to_numpy() / QM.to_numpy()[:,None]
    ofi_levels.reset_index(inplace=True)
    print(ofi_levels.columns)
    normalized_ofis = ofi_levels[['ts_event']]
    for i in range(max_levels):
        normalized_ofis.loc[:, f'ofi_{i}'] = ofi_levels[f'of_diffs_{i}'] / QM
    return normalized_ofis

def load_and_preprocess_book(stock_id):
    # load dataframes for 5 days
    dfs = []
    directory_path = f"data/raw/{stock_id}"
    pattern = os.path.join(directory_path, "xnas-itch-????????.mbp-10.csv.zst")
    matching_files = glob.glob(pattern)

    dfs = []
    for file_path in sorted(matching_files):
        # file_path contains the full path to the file
        day_df = pd.read_csv(file_path, compression="zstd")
        dfs += [day_df]
    book = pd.concat(dfs)

    # preprocess dataframe
    book['ts_event'] = pd.to_datetime(book['ts_event'])
    book = book.set_index('ts_event')
    book = book.between_time("09:30", "16:00")
    book = book.reset_index()

    return book

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--stock_id', type=str, required=True)
    parser.add_argument('--output_path', type=str, required=True)
    parser.add_argument('--max_levels', type=int, default=5)
    args = parser.parse_args()

    book = load_and_preprocess_book(args.stock_id)
    minute_ofi_df = calculate_normalized_ofi(book, args.max_levels)
    mid_price_delta, log_returns = calculate_returns(book)
    minute_ofi_df['log_return'] = log_returns
    minute_ofi_df['mid_price_delta'] = mid_price_delta
    minute_ofi_df = minute_ofi_df.dropna()
    minute_ofi_df.to_csv(f'{args.output_path}/{args.stock_id}.csv', index=False)

if __name__ == "__main__":
    main()