# 5.5 Affichage des courbes de comparaison
from pyspark.sql import SparkSession
import plotly.express as px
import plotly.graph_objs as go
import pyspark.sql.functions as F

# Création session Spark
spark = SparkSession \
    .builder \
    .master("local") \
    .appName("atl-map-reduce-analysis") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

# Lecture du fichier parquet réalisé dans le main.py et stockage dans un dataframe spark
df = spark.read.option("header", "true").option(
    "recursiveFileLookup", "true").parquet("Archive/window_average_df.parquet")

df.show()

# Filtrer par critère 'age' et compter les occurrences par groupe d'âge
age_count_df = df.filter(F.col("criterion") == "age") \
                 .groupBy("variable") \
                 .agg(F.count("variable").alias("count"))

# Afficher le DataFrame PySpark résultant
age_count_df.show()


# Convertir le dataframe spark en df pandas pour pouvoir l'utiliser dans plotly
age_count_df_pandas = age_count_df.toPandas()

# Création du graphique
bar_age = go.Bar(
    x=age_count_df_pandas['variable'],
    y=age_count_df_pandas['count']
)

# Création des légendes
layout_age = go.Layout(
    title='Nombre d\'occurrences par groupe d\'âge',
    xaxis=dict(title='Groupe d\'âge'),
    yaxis=dict(title='Nombre d\'occurrences')
)

fig_age = go.Figure(data=[bar_age], layout=layout_age)

# Filtrer sur le critère sexe pour que la variable
sexe_h_df = df.filter((F.col("criterion") == "sexe")
                      & (F.col("variable") == "M"))

sexe_h_df.show()

sexe_h_df_pandas = sexe_h_df.toPandas()

# Création des traces pour les courbes d'évolution
trace_index = go.Scatter(
    x=sexe_h_df_pandas['timestamp'],
    y=sexe_h_df_pandas['index'],
    mode='lines+markers',
    name='Index'
)

trace_moving_avg = go.Scatter(
    x=sexe_h_df_pandas['timestamp'],
    y=sexe_h_df_pandas['moving_avg'],
    mode='lines+markers',
    name='Moving Average'
)

# Configuration du layout
layout_sexe = go.Layout(
    title='Évolution de l\'index et du moving_avg pour le sexe "H"',
    xaxis=dict(title='Timestamp'),
    yaxis=dict(title='Valeur')
)

# Création de la figure
fig_sexe = go.Figure(data=[trace_index, trace_moving_avg], layout=layout_sexe)

# Affichage des graphiques dans le web browser (1 onglet par graph)
fig_age.show()
fig_sexe.show()
