# Importation de SparkSession, le point d'entrée principal pour utiliser Spark
from pyspark.sql import SparkSession

# Création d'une session Spark nommée "CheckParquet"
# C'est nécessaire pour lire et manipuler des fichiers Parquet
spark = SparkSession.builder.appName("CheckParquet").getOrCreate()

# ==============================================================
# LECTURE DES TICKETS
# ==============================================================

# On lit tous les fichiers Parquet situés dans le dossier tickets_parquet
# Le '*' permet de lire tous les fichiers présents dans ce dossier
df_tickets = spark.read.parquet("output/tickets_parquet/*")

# Affichage d'un titre pour le terminal
print("=== Tickets envoyés ===")

# Affiche le contenu des tickets avec toutes les colonnes
# truncate=False permet de ne pas tronquer le texte dans les colonnes
df_tickets.show(truncate=False)


# ==============================================================
# LECTURE DES STATISTIQUES
# ==============================================================

# On lit tous les fichiers Parquet situés dans le dossier stats_parquet
df_stats = spark.read.parquet("output/stats_parquet/*")

# Affichage d'un titre pour le terminal
print("=== Statistiques ===")

# Affiche le contenu des statistiques avec toutes les colonnes
df_stats.show(truncate=False)