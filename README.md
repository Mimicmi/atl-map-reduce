# Réponses aux questions

# 5 - 2 :

En faisant un "union_df.printSchema()"
Le résultat est le suivant :
|-- timestamp: date (nullable = true)
|-- user_id: integer (nullable = true)
|-- application: string (nullable = true)
|-- time_spent: integer (nullable = true)
|-- times_opened: integer (nullable = true)
|-- notifications_received: integer (nullable = true)
|-- times_opened_after_notification: integer (nullable = true)
|-- age: string (nullable = true)
|-- sexe: string (nullable = true)

L'âge est donc en string par défaut.

Oui il est possible de changer le type de l'âge en le castant pendant la création de la colonne comme ceci :

age_sexe_df = split(union_df["age_sexe"], "-")
union_df = union_df.withColumn("age", age_sexe_df.getItem(0).cast("integer"))
union_df = union_df.withColumn("sexe", age_sexe_df.getItem(1).cast("string"))

Cela va nous créer la nouvelle colonne avec les valeurs récupérés du split

# Harmoniser les données :

On regarde les valeurs différentes en faisant :

distinctValuesDF = union_df.select("sexe").distinct().show()
On obtient : F, M, H, m, f
On va garder uniquement: F, M

Je fais alors :

union_df = union_df.withColumn("sexe",
when(union_df.sexe == "m", "M")
.when(union_df.sexe == "f", "F")
.when(union_df.sexe == "H", "M")
.otherwise(union_df.sexe)
)

distinctValuesDF = union_df.select("sexe").distinct().show()

Alors je n'aurai plus que "F, M" comme valeurs

# Faire les aggréagations sur date / sexe / âge / application :

union_df_agg = union_df.groupBy("timestamp", "sexe", "age", "application").agg(
mean("time_spent").alias("mean-time-spent"),
mean("times_opened").alias("mean-times-openend"),
mean("notifications_received").alias("mean-notifications-received"),
mean("times_opened_after_notification").alias(
"mean-times-opened-after-notifications")
)

union_df_agg.show()

# Quels sont les types des nouvelles colonnes ?

union_df_agg.printSchema()

root
|-- timestamp: date (nullable = true)
|-- sexe: string (nullable = true)
|-- age: integer (nullable = true)
|-- application: string (nullable = true)
|-- mean-time-spent: double (nullable = true)
|-- mean-times-openend: double (nullable = true)
|-- mean-notifications-received: double (nullable = true)
|-- mean-times-opened-after-notifications: double (nullable = true)
