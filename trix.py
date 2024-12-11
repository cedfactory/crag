import pandas as pd
import matplotlib.pyplot as plt

#datafile = "top_1_per_pair_timeframe.csv"
#datafile = "top_5_per_pair_timeframe.csv"
datafile = "intermediate_dataframe.csv"
df = pd.read_csv(datafile)

#df["Col1"] = df.apply(lambda row: row["Col2"] if row["Col2"] > 25 else row["Col1"], axis=1)
#df["timeframe"] = df.apply(lambda row: int(row["timeframe"][0]), axis=1)
#df["trix_signal_type"] = df.apply(lambda row: 0 if row["trix_signal_type"][0] == "s" else 1, axis=1)

pairs = df["pair"].unique()
print(pairs)

for timeframe in ["1h", "2h", "4h"]:
    for trix_signal_type in ["sma", "ema"]:

        for pair in pairs:
            df_current = df[(df["timeframe"] == timeframe) &
                            (df["trix_signal_type"] == trix_signal_type) &
                            (df["pair"] == pair)]
            #print(df_current)

            fig = plt.figure()
            ax = fig.add_subplot(111, projection='3d')

            scatter = ax.scatter(df_current['trix_length'], df_current['trix_signal_length'], df_current['long_ma_length'],
                                 marker='^', c=df_current["weighted_rank"],cmap="coolwarm")

            plt.colorbar(scatter, label="weighted_rank")

            plt.title("{} ({} {})".format(pair, trix_signal_type, timeframe))

            ax.set_xlabel('trix_length')
            ax.set_ylabel('trix_signal_length')
            ax.set_zlabel('long_ma_length')
            
            filename = "{}_{}_{}.png".format(pair.replace("/", "_"), trix_signal_type, timeframe)
            plt.savefig(filename, dpi=300, bbox_inches='tight')
            print("filename : ", filename)

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
                    file.write("<tr><td><img width=220 src=\"{}\"/></td><td><img width=220 src=\"{}\"/></td></tr>".format(file1, file2))
                    file.write("</table>")
                    file.write("</center>")
                file.write("</body></html>\n")

            # Afficher le graphe
            #plt.show()
            plt.close(fig)
            