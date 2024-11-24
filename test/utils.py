import pandas as pd
import os


def write_file(filename, string):
    with open(filename, 'w') as f:
        f.write(string)
    return filename


def get_json_for_get_df_range():
    df = pd.read_csv("./test/data/AAVE_USD.csv", delimiter=',')
    df.drop(["Unnamed: 0"], axis=1, inplace=True)
    df['timestamp'] = df['timestamp'].astype('string')  # convert object type to string type
    df['timestamp'] = pd.to_datetime(df['timestamp'], format="%d/%m/%Y %H:%M")  # format datetime
    df.rename(columns={"timestamp": "index"}, inplace=True)
    json_df = {'result': {'BTC_USDT': {'status': 'ok', 'info': df.to_json()}}}

    return json_df


def detect_environment():
    if os.environ.get("GITHUB_ACTIONS") == "true":
        return "github"
    elif os.environ.get("CI") == "true":
        return "gitlab"
    elif os.environ.get("TRAVIS") == "true":
        return "travis"
    elif os.environ.get("JENKINS_URL"):
        return "jenkins"
    else:
        return "local"
