import pandas as pd

def get_json_for_get_df_range():
    df = pd.read_csv("./test/data/AAVE_USD.csv", delimiter=',')
    df.drop(["Unnamed: 0"], axis=1, inplace=True)
    df['timestamp'] = df['timestamp'].astype('string') # convert object type to string type
    df['timestamp'] = pd.to_datetime(df['timestamp'], format="%d/%m/%Y %H:%M") # format datetime
    df.rename(columns = {"timestamp":"index"}, inplace = True)
    json_df = {'result': {'BTC_USDT': {'status': 'ok', 'info':df.to_json()}}}

    return json_df
