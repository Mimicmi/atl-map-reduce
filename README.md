# Projet - Initialisation

Cloner le projet
Installer les dépendances :
pip install -r requirements.txt

## Les fichiers

3 fichiers : main.py, refacto.py, test\_.py

main.py prend les étapes jusqu'à la refactorisation
refacto.py est la refactorisation de main.py
test_py sont les tests unitaires liés à refacto.py

## Executer les fichiers

python main.py
python refacto.py
pytest

# ATTENTION

## Bon déroulement de l'exécution du projet

Pour que l'execution puisse marcher, il est important que le dossier "Archive"
contenant tous les csv soient au root du projet. Ce dernier fait partie du .gitignore

# **5 Etapes**

## **5.1 Lecture des données**

**Quelle est la différence entre l’utilisation d’un schéma figé lors de la lecture des données et l’utilisation de l’option inferSchema de spark ?**

-   Schéma figé: Rapide, précis mais inflexible (définition manuelle requise).
-   inferSchema: Flexible, s'adapte aux changements (analyse des données plus lente, inférence parfois imprécise).

\***\*Quels sont les avantages et les inconvénients de chaque approche ?\*\***

**Schéma figé**

Avantages

-   Performances : la définition explicite du schéma peut améliorer considérablement les performances, car Spark n'a pas besoin d'analyser l'intégralité de l'ensemble de données pour déduire le schéma.
-   Qualité des données : on contrôle la définition du schéma pour qu'il représente avec précision les données.

Les inconvénients

-   Plus de temps et de code : demande plus de code pour définir le schéma, ce qui peut être long et compliqué pour des ensembles de données complexes ou lorsque le schéma évolue dans le temps.

**inferSchema**
Avantages

-   Simplicité : Facile à utiliser, notamment pour une exploration rapide des données ou lorsque le schéma n'est pas connu à l'avance
-   Moins de code : pas besoin de spécifier manuellement le schéma, ce qui réduit la quantité de code à écrire

Les inconvénients

-   Frais généraux de performances : Spark doit analyser l'ensemble de données entier pour déduire le schéma, ce qui peut être coûteux en termes de calcul lorsqu’il y a beaucoup de données.
-   Qualité des données : l’inferSchema déduit des schémas incorrects si les données comportent des valeurs manquantes ou incohérentes.
-   Type Inference : l'inférence peut ne pas toujours identifier correctement les types de données des colonnes, ce qui entraîne des incompatibilités potentielles de types de données.

**Est-ce que l’utilisation d’un schéma figé accélère l’exécution de la lecture des données ? Pourquoi ?**

Oui la définition d’un schéma figé peut améliorer considérablement les performances, car Spark n'a pas besoin d'analyser les données pour définir un schéma.

**5.2 Nettoyage des données**

En faisant un "union_df.printSchema()"

Le résultat est le suivant :

```
|-- timestamp: date (nullable = true)

|-- user\_id: integer (nullable = true)

|-- application: string (nullable = true)

|-- time\_spent: integer (nullable = true)

|-- times\_opened: integer (nullable = true)

|-- notifications\_received: integer (nullable = true)

|-- times\_opened\_after\_notification: integer (nullable = true)

|-- age: string (nullable = true)

|-- sexe: string (nullable = true)
```

**Quel est le type de la colonne âge ? Est-il possible de le changer en un autre type ?**
Oui il est possible de changer le type de l'âge en le castant pendant la création de la colonne comme ceci :

```
age\_sexe\_df = split(union\_df\["age\_sexe"\], "-")

union\_df = union\_df.withColumn("age", age\_sexe\_df.getItem(0).cast("integer"))

union\_df = union\_df.withColumn("sexe", age\_sexe\_df.getItem(1).cast("string"))

Cela va nous créer la nouvelle colonne avec les valeurs récupérés du split
```

**\# Harmoniser les données :**

On regarde les valeurs différentes en faisant :

```
distinctValuesDF = union\_df.select("sexe").distinct().show()

On obtient : F, M, H, m, f

On va garder uniquement: F, M

Je fais alors :

union\_df = union\_df.withColumn("sexe",

when(union\_df.sexe == "m", "M")

.when(union\_df.sexe == "f", "F")

.when(union\_df.sexe == "H", "M")

.otherwise(union\_df.sexe)

)

distinctValuesDF = union\_df.select("sexe").distinct().show()
```

Alors je n'aurai plus que "F, M" comme valeurs

Faire les aggréagations sur date / sexe / âge / application :

```
union\_df\_agg = union\_df.groupBy("timestamp", "sexe", "age", "application").agg(

   mean("time\_spent").alias("mean-time-spent"),

   mean("times\_opened").alias("mean-times-openend"),

   mean("notifications\_received").alias("mean-notifications-received"),

   mean("times\_opened\_after\_notification").alias(

       "mean-times-opened-after-notifications")

)

union\_df\_agg.show()
```

**Quels sont les types des nouvelles colonnes ?**

```
union\_df\_agg.printSchema()

root

|-- timestamp: date (nullable = true)

|-- sexe: string (nullable = true)

|-- age: integer (nullable = true)

|-- application: string (nullable = true)

|-- mean-time-spent: double (nullable = true)

|-- mean-times-openend: double (nullable = true)

|-- mean-notifications-received: double (nullable = true)

|-- mean-times-opened-after-notifications: double (nullable = true)

Les nouvelles colonnes sont automatiquements détectées comme des doubles vu que des “mean” ont été fait.
```

**Quel type de jointure utiliser ? Quelle est la différence entre les types de jointures ?**

On utilise la jointure gauche (Left Join), ça permet de conserver toutes les lignes de la table principale et d'ajouter les données de catégorie.

De plus, nous avons fait un broadcast sur le petit dataframe pour les bon practice et gagner en performance.

```
\# Adding new path for new csv applications\_categories & new schema

PATH\_3 = "Archive/applications\_categories.csv"
```

```
schema2 = StructType(\[

   StructField("application", StringType()),

   StructField("category", StringType()),

\])

\# Creating new dataframes from applications\_categories.csv

df\_3 = spark.read.format("csv").schema(

   schema2).option("header", True).load(PATH\_3)

\# Join union\_df\_agg with the new csv with broadcast of newer csv as it's a little dataframe

new\_union = union\_df\_agg.join(broadcast(df\_3), on="application", how="left")

new\_union.show()
```

**5.3.1 Comparaison par tranche d’âge**

Étant en France, nous allons utiliser le timezone Europe/Paris pour définir un jour. **Comment peut-on le faire en pyspark ? Peut-on configurer la session spark afin de le rendre plus facile ?**

On peut dès le début dans notre schéma mettre la colonne “timestamp” au format “TimestampType” puis utiliser la fonction “from\\\_utc\\\_timestamp” pour lui mettre la bonne Timezone puis utiliser la fonction “to\\\_date” pour mettre au formée “yyyy-MM-dd”

```
schema = StructType([

   StructField("timestamp", TimestampType()),
```

```
union\_agg = union\_agg.withColumn(

   "timestamp", col("timestamp").cast("timestamp"))
```

```
union\_agg = union\_agg.withColumn(

   "timestamp", from\_utc\_timestamp(union\_agg.timestamp, "Europe/Paris"))

union\_agg = union\_agg.withColumn(

   "timestamp", to\_date("timestamp", "yyyy-MM-dd"))
```

Nous avions déjà fait en amont le schema qui avait le timestamp au format DateType() ce qui nous a permi déjà de simplifier la démarche et sans avoir des étapes à transformer le string en timestamp puis de faire le from_utc_timestamp

**5.3.4 Calcul de l’indice**

**Quels types de transformation pouvons-nous utiliser ?**

```
\# Filter only the range of criterion's age of 15-25

windowSpec = Window.partitionBy("criterion", "variable").orderBy(

   "timestamp")

window\_df = combined\_df.withColumn("index", lag("value").over(windowSpec))

window\_df.show()
```

Nous avons utilisé window qui va nous permettre de partitionner sur les “criterion” et “variable”, en l’état actuel les valeurs qui sont dans “variable” sont très distinctes par leurs valeurs (“15-25 ans”, “nomApplication”, “H”), nous rajoutons “criterions” pour la lecture et permettre que si un nouveau critère est donné qui ressemble aux variables de pouvoir fonctionner.

La transformation lag() va être appliquée pour pouvoir accéder à la valeur précédente pour pouvoir faire des comparaisons sur la colonne “value”. Le lag peut-être uniquement utilisé sur un window.

**Faire la moyenne glissante sur 5 valeurs pour éliminer le bruit + ajout nouvelle colonne smoothed_index**

**5.4 Stockage du résultat**

**Vérifions la spark UI. Est-ce qu’il y a des calculs qui ont été faits plusieurs fois ?**

Oui, les calculs de certains dataframes ont été faits plusieurs fois.

**Pouvons-nous optimiser ? Que fait la fonction cache() ? et que fait la fonction persist() ?**

Il est possible d’optimiser avec .cache() et .persist()

.cache() permet d’enregistrer le dataframe en mémoire pour optimiser les répétitions d’appel de ce Dataframe.

.persist() permet de spécifier différents niveaux de stockage, incluant la mémoire et le disque.

**Pourquoi utilisons-nous le format parquet au lieu du csv ?**

Car le CSV n’est pas le plus optimisé. Créer un CSV à chaque exécution remplirait le stockage de la machine rapidement contrairement au format parquet qui permet d’avoir un stockage des donnés plus optimisé. De plus, créer un script pour clear les CSV demanderait de la puissance en plus.

Pour faire simple, utiliser le format parquet est simplement plus optimisé.

**5.6 Être plus professionnel**

Maintenant que tout le code marche bien et que nous avons des résultats satisfaisants, revenons à la qualité de notre code.

**Est-ce qu’il est très bien écrit ? Est-ce qu’il est modulaire ?**

En l’état le code n’est pas bien écrit, c’est 1 seul script qui s'exécute.

Il n’est pas du tout modulaire car il n’a aucune fonction.

Une fois fait, notre code contient une fonction générale appelée “main”, puis des fonctions secondaires qui représentent les différentes fonctionnalités de notre code.

**Est-ce qu’il contient des tests unitaires ?**

Non, il ne contient pas de tests unitaires par défaut.

Le code a ensuite été refactoré pour pouvoir supporter les tests unitaires et permettre une meilleur scalabilité.
