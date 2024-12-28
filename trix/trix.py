import pandas as pd
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn import cluster
from sklearn.preprocessing import StandardScaler

#datafile = "top_1_per_pair_timeframe.csv"
#datafile = "top_5_per_pair_timeframe.csv"
datafile = "intermediate_dataframe.csv"
df = pd.read_csv(datafile)

#df["Col1"] = df.apply(lambda row: row["Col2"] if row["Col2"] > 25 else row["Col1"], axis=1)
#df["timeframe"] = df.apply(lambda row: int(row["timeframe"][0]), axis=1)
#df["trix_signal_type"] = df.apply(lambda row: 0 if row["trix_signal_type"][0] == "s" else 1, axis=1)

pairs = df["pair"].unique()
print(pairs)

g_result = {}
for pair in pairs:
    dict_max_scores = {}
    dict_centroids = {}
    for timeframe in ["1h", "2h", "4h"]:
        for trix_signal_type in ["sma", "ema"]:
            key_current = trix_signal_type + "_" + timeframe

            df_current = df[(df["timeframe"] == timeframe) &
                            (df["trix_signal_type"] == trix_signal_type) &
                            (df["pair"] == pair)]
            #print(df_current)
            if df_current.empty:
                print('DataFrame is empty!')
                continue

            max_rank = df_current["weighted_score"].max()
            #df_current["weighted_rank"] = df_current["weighted_rank"].apply(lambda x: (max_rank - x))

            df_current = df_current[df_current["weighted_score"] > max_rank - .2]

            fig = plt.figure()
            ax = fig.add_subplot(111, projection='3d')

            scatter = ax.scatter(df_current['trix_length'], df_current['trix_signal_length'], df_current['long_ma_length'],
                                 marker='^', c=df_current["weighted_score"],cmap="coolwarm")

            plt.colorbar(scatter, label="weighted_score")

            plt.title("{} ({} {})".format(pair, trix_signal_type, timeframe))

            ax.set_xlabel('trix_length')
            ax.set_ylabel('trix_signal_length')
            ax.set_zlabel('long_ma_length')
            
            filename = "{}_{}_{}.png".format(pair.replace("/", "_"), trix_signal_type, timeframe)
            plt.savefig(filename, dpi=300, bbox_inches='tight')
            #print("filename : ", filename)
            print("=> {} / {} / {}".format(pair, timeframe, trix_signal_type))

            # Afficher le graphe
            #plt.show()
            plt.close(fig)

            #
            # Analysis
            #

            # best score
            row_max = df_current.loc[df_current["weighted_score"].idxmax()]
            print("Best score: {} (trix_length = {} / trix_signal_length = {} / long_ma_length = {})".format(
                row_max["weighted_score"], row_max["trix_length"], row_max["trix_signal_length"], row_max["long_ma_length"]))
            #row_min = df_current.loc[df_current["weighted_rank"].idxmin()]

            # store to display later
            dict_max_scores[key_current] = [row_max["trix_length"], row_max["trix_signal_length"], row_max["long_ma_length"]]

            columns_to_keep = ["trix_length", "trix_signal_length", "long_ma_length", "weighted_score"]
            df_current = df_current[columns_to_keep]
            df_current["weighted_score"] = df_current["weighted_score"].apply(lambda x: int(10* x))
            df_repeated = df_current.loc[df_current.index.repeat(df_current["weighted_score"])].reset_index(drop=True)

            # columns as coordinates
            coords = df_repeated[["trix_length", "trix_signal_length", "long_ma_length"]]

            kmeans = KMeans(n_clusters=1)
            kmeans.fit(coords)
            centroids = kmeans.cluster_centers_
            print("KMeans :", centroids)

            # store to display later
            dict_centroids[key_current] = [
                "{:.2f}".format(centroids[0][0]),
                "{:.2f}".format(centroids[0][1]),
                "{:.2f}".format(centroids[0][2])]

            g_result[pair] = {"max": dict_max_scores, "kmeans": dict_centroids}

    #
    # Output
    #
    pair_renamed = pair.replace("/", "_")
    with open('_{}.html'.format(pair_renamed), 'w') as file:
        file.write("<html><body>\n")
        file.write("<h1>{}</h1>".format(pair))
        for tf in ["1h", "2h", "4h"]:
            file.write("<center>")
            file.write("<h2>{}</h2>".format(tf))
            file.write("<table>")
            file1 = "{}_{}_{}.png".format(pair_renamed, "sma", tf)
            file2 = "{}_{}_{}.png".format(pair_renamed, "ema", tf)
            file.write("<tr>")
            if "sma_"+tf in dict_max_scores and "sma_"+tf in dict_centroids:
                file.write("<td><img width=200 src=\"{}\"/><small><p>max = {}<p>centroid = {}</small></td>".format(file1, dict_max_scores["sma_"+tf], dict_centroids["sma_"+tf]))
            if "ema_"+tf in dict_max_scores and "ema_"+tf in dict_centroids:
                file.write("<td><img width=200 src=\"{}\"/><small><p>max = {}<p>centroid = {}</small></td>".format(file2, dict_max_scores["ema_"+tf], dict_centroids["ema_"+tf]))
            file.write("</tr>")
            file.write("</table>")
            file.write("</center>")
        file.write("</body></html>\n")

with open('_trix_synthesis.xml', 'w') as file:
    file.write("<trix>\n")
    for pair in g_result:
        file.write("<symbol name=\"{}\">\n".format(pair))
        for timeframe in ["1h", "2h", "4h"]:
            for trix_signal_type in ["sma", "ema"]:
                file.write("<config trix_signal_type=\"{}\" timeframe=\"{}\">".format(trix_signal_type, timeframe))
                key_current = trix_signal_type + "_" + timeframe
                if "max" in g_result[pair] and key_current in g_result[pair]['max']:
                    max_values = g_result[pair]['max'][key_current]
                    file.write("<max trix_length=\"{}\" trix_signal_length=\"{}\" long_ma_length=\"{}\" />".format(
                        max_values[0], max_values[1], max_values[2]))
                if "kmeans" in g_result[pair] and key_current in g_result[pair]['kmeans']:
                    kmeans_values = g_result[pair]['kmeans'][key_current]
                    file.write("<kmeans trix_length=\"{}\" trix_signal_length=\"{}\" long_ma_length=\"{}\" />".format(
                        kmeans_values[0], kmeans_values[1], kmeans_values[2]))
                file.write("</config>\n")
        file.write("</symbol>\n")
    file.write("</trix>\n")

# read the file
import xml.etree.ElementTree as ET
g_configs = {}
tree = ET.parse('_trix_synthesis.xml')
root = tree.getroot()
for symbol in root.findall('symbol'):
    symbol_name = symbol.get('name')
    g_configs[symbol_name] = []  # Chaque symbol aura une liste de configurations

    # Parcourir chaque configuration
    for config in symbol.findall('config'):
        config_data = {
            'trix_signal_type': config.get('trix_signal_type'),
            'timeframe': config.get('timeframe'),
            'max': {},
            'kmeans': {}
        }

        # Extraire les sous-éléments max et kmeans
        for sub_config in config:
            config_data[sub_config.tag] = {
                'trix_length': float(sub_config.get('trix_length')),
                'trix_signal_length': float(sub_config.get('trix_signal_length')),
                'long_ma_length': float(sub_config.get('long_ma_length'))
            }

        # Ajouter cette configuration à la liste du symbole
        g_configs[symbol_name].append(config_data)
