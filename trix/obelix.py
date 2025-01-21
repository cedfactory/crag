import pandas as pd
from sklearn.cluster import KMeans

datafile = "scored_results.csv"
df = pd.read_csv(datafile)

#SYMBOL,TIMEFRAME,MA_TYPE,HIGH_OFFSET,LOW_OFFSET,ZEMA_LEN_BUY,ZEMA_LEN_SELL,SSL_ATR_PERIOD

parameters_columns = ["HIGH_OFFSET", "LOW_OFFSET", "ZEMA_LEN_BUY", "ZEMA_LEN_SELL", "SSL_ATR_PERIOD"]
eval_column = "WIN RATE [%]" # "TOTAL RETURN [%]" # "SHARPE RATIO"

pairs = df["SYMBOL"].unique()
print(pairs)

g_result = {}
for pair in pairs:
    dict_max_scores = {}
    dict_centroids = {}
    for timeframe in ["1m", "5m", "15m", "1h"]:
        for ma_type in ["ZLEMA", "TEMA", "DEMA", "ALMA", "HMA"]:
            key_current = ma_type + "_" + timeframe
            print("{} key_current : {}".format(pair, key_current))

            df_current = df[(df["TIMEFRAME"] == timeframe) &
                            (df["MA_TYPE"] == ma_type) &
                            (df["SYMBOL"] == pair)]
            #print(df_current)
            if df_current.empty:
                print('DataFrame is empty!')
                continue

            max_rank = df_current[eval_column].max()
            #df_current["weighted_rank"] = df_current["weighted_rank"].apply(lambda x: (max_rank - x))

            # filter interest
            #df_current = df_current[df_current[eval_column] > max_rank - .2]
            #df_current = df_current[df_current[eval_column] > 1.] # sharp ratio & total return
            #df_current[eval_column].clip(upper=20000, inplace=True) # total return
            if df_current.empty:
                print('DataFrame is empty!')
                continue
        
            #
            # Analysis
            #

            # best score
            row_max = df_current.loc[df_current[eval_column].idxmax()]
            print("Best score: {}".format(row_max[eval_column]))
            for p in parameters_columns:
                print("  {} : {}".format(p, row_max[p]))

            # store to display later
            dict_max_scores[key_current] = row_max

            columns = parameters_columns.copy()
            columns.extend([eval_column])
            df_current = df_current[columns]

            df_current = df_current.dropna(subset=[eval_column])
            df_current[eval_column] = df_current[eval_column].apply(lambda x: int(float(x)))
            df_repeated = df_current.loc[df_current.index.repeat(df_current[eval_column])].reset_index(drop=True)

            # columns as coordinates
            coords = df_repeated[parameters_columns]

            kmeans = KMeans(n_clusters=1)
            kmeans.fit(coords)
            centroids = kmeans.cluster_centers_
            print("KMeans :", centroids)

            # store to display later
            dict_centroids[key_current] = centroids[0]

            g_result[pair] = {"max": dict_max_scores, "kmeans": dict_centroids}


with open('_obelix_synthesis.xml', 'w') as file:
    file.write("<obelix>\n")
    for pair in g_result:
        file.write("<symbol name=\"{}\">\n".format(pair))
        for timeframe in ["1m", "5m", "15m", "1h"]:
            for ma_type in ["ZLEMA", "TEMA", "DEMA", "ALMA", "HMA"]:
                file.write("<config ma_type=\"{}\" timeframe=\"{}\">".format(ma_type, timeframe))
                key_current = ma_type + "_" + timeframe
                if "max" in g_result[pair] and key_current in g_result[pair]['max']:
                    max_values = g_result[pair]['max'][key_current]
                    #file.write("<max trix_length=\"{}\" trix_signal_length=\"{}\" long_ma_length=\"{}\" />".format(
                    #    max_values[0], max_values[1], max_values[2]))
                    file.write("<max ")
                    file.write("value=\"{}\" ".format(max_values[eval_column]))
                    for i, parameter in enumerate(parameters_columns):
                        file.write("{}=\"{}\" ".format(parameter, max_values[parameter]))
                    file.write("/>")
                if "kmeans" in g_result[pair] and key_current in g_result[pair]['kmeans']:
                    kmeans_values = g_result[pair]['kmeans'][key_current]
                    file.write("<kmeans ")
                    for i, parameter in enumerate(parameters_columns):
                        file.write("{}=\"{}\" ".format(parameter, kmeans_values[i]))
                    file.write("/>")
                file.write("</config>\n")
        file.write("</symbol>\n")
    file.write("</obelix>\n")

# read the file
import xml.etree.ElementTree as ET
g_configs = {}
tree = ET.parse('_obelix_synthesis.xml')
root = tree.getroot()
for symbol in root.findall('symbol'):
    symbol_name = symbol.get('name')
    g_configs[symbol_name] = []  # Chaque symbol aura une liste de configurations

    # Parcourir chaque configuration
    for config in symbol.findall('config'):
        config_data = {
            'ma_type': config.get('ma_type'),
            'timeframe': config.get('timeframe'),
            'max': {},
            'kmeans': {}
        }

        # Extraire les sous-éléments max et kmeans
        for sub_config in config:
            tmp = {}
            for i, parameter in enumerate(parameters_columns):
                tmp[parameter] = float(sub_config.get(parameter))
            config_data[sub_config.tag] = tmp

        # Ajouter cette configuration à la liste du symbole
        g_configs[symbol_name].append(config_data)

print("done")
