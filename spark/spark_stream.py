# SparkSession = point d'entrée principal de Spark
from pyspark.sql import SparkSession

from kafka import KafkaConsumer

import time 

# Fonctions utiles pour manipuler les colonnes
from pyspark.sql.functions import col, from_json, when, count

# Types pour définir la structure du JSON
from pyspark.sql.types import StructType, StringType

# ===============================
# 0️ ATTENTE QUE LE TOPIC EXISTE
# ===============================

# Attente que le topic client_tickets existe
while True:
    try:
        consumer = KafkaConsumer(
            'client_tickets',
            bootstrap_servers='redpanda-0:9092'
        )
        print("Topic 'client_tickets' disponible !")
        consumer.close()
        break
    except Exception:
        print("Attente que le topic 'client_tickets' soit disponible...")
        time.sleep(2)

# ===============================
# 1️ CREATION DE LA SESSION SPARK
# ===============================

# Ici on configure Spark
# - appName : nom de l'application
# - executor.memory : mémoire utilisée
# - shuffle.partitions : nombre de partitions pour les calculs
## - spark.jars.packages : package externe nécessaire pour lire Kafka
spark = SparkSession.builder \
    .appName("ClientTicketsStreaming") \
    .config("spark.executor.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "1") \
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
    .getOrCreate()

# Réduit les logs pour éviter trop de messages inutiles
spark.sparkContext.setLogLevel("WARN")


# ===============================
# 2️ LECTURE DU TOPIC KAFKA (Redpanda)
# ===============================

# Spark va ouvrir une connexion continue vers Redpanda
# Il va lire les messages du topic "client_tickets"

df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda-0:9092")\
    .option("subscribe", "client_tickets") \
    .option("startingOffsets", "earliest") \
    .load()

# info :
# Kafka envoie les données sous forme de bytes
# Spark voit deux colonnes principales :
# - key
# - value (qui contient ton JSON)


# ===============================
# 3️ CONVERSION DU VALUE EN STRING
# ===============================

# On transforme la colonne "value" (bytes) en texte lisible
df_string = df_raw.selectExpr("CAST(value AS STRING)")


# ===============================
# 4️ DEFINITION DU SCHEMA DU JSON
# ===============================

# On explique à Spark à quoi ressemble notre JSON
# Sinon Spark ne sait pas comment organiser les colonnes

schema = StructType() \
    .add("ticket_id", StringType()) \
    .add("client_id", StringType()) \
    .add("creation_time", StringType()) \
    .add("demande", StringType()) \
    .add("type_demande", StringType()) \
    .add("priorite", StringType())


# ===============================
# 5️ PARSING DU JSON
# ===============================

# from_json :
# - prend la colonne texte
# - applique le schema
# - transforme le JSON en colonnes Spark

df_parsed = df_string \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Maintenant df_parsed contient :
# ticket_id | client_id | creation_time | demande | type_demande | priorite


# ===============================
# 6️ TRANSFORMATION ET ANALYSE DES DONNEES (DEUXIEME PARTIE)
# ===============================

# TRANSFORMATION
# On ajoute une nouvelle colonne appelée "equipe_support"
# Cette colonne est calculée automatiquement selon le type de demande

df_transformed = df_parsed.withColumn(
    "equipe_support",  # nom de la nouvelle colonne
    when(col("type_demande") == "Facturation", "Equipe Facturation")
        # si le type est "Facturation" → on assigne l'équipe Facturation

    .when(col("type_demande") == "Réclamation", "Equipe Réclamation")
        # si le type est "Réclamation" → on assigne l'équipe Réclamation

    .when(col("type_demande") == "Support Technique", "Equipe Technique")
        # si le type est "Support Technique" → on assigne l'équipe Technique

    .otherwise("Equipe Générale")
        # pour tous les autres cas → on assigne l'équipe Générale
)

# À cette etape :
# df_transformed contient toutes les colonnes d’origine
# + une nouvelle colonne "equipe_support"


# AGRÉGATION (ANALYSE)
# On calcule le nombre total de tickets pour chaque type de demande

df_aggregated = df_transformed.groupBy("type_demande") \
    .agg(
        count("ticket_id").alias("nb_tickets")
        # on compte combien de ticket_id existent pour chaque type
    )

# À cette etape :
# df_aggregated contient un tableau résumé :
# type_demande | nb_tickets


# ===============================
# 7️ EXPORT DES RESULTATS EN FICHIER (ETAPE 3)
# ===============================

# Export des tickets transformés (avec l'équipe assignée)
query_transformed = (
    df_transformed.writeStream
    .outputMode("append")  
        # "append" = ajoute seulement les nouveaux tickets

    .format("parquet")     
        # écrit les données au format Parquet (format optimisé pour l’analyse)

    .option("path", "output/tickets_parquet")  
        # dossier où les fichiers Parquet seront enregistrés

    .option("checkpointLocation", "output/checkpoint_tickets")  
        # dossier de sauvegarde obligatoire en streaming
        # permet à Spark de reprendre correctement en cas d’arrêt

    .trigger(processingTime="1 seconds")


    .start()  
        # démarre l’écriture en continu
)


# Export des statistiques (nombre de tickets par type)
# On utilise foreachBatch pour contourner le problème d'append avec un agrégat
def save_aggregated_batch(batch_df, batch_id):
    # Cette fonction sera appelée à chaque batch
    # batch_df contient le tableau complet des statistiques à ce moment
    batch_df.write.mode("overwrite").parquet("output/stats_parquet")
        # "overwrite" = écrase le fichier précédent pour garder toujours les stats à jour

query_aggregated = (
    df_aggregated.writeStream
    .foreachBatch(save_aggregated_batch)  
        # chaque batch appelle la fonction save_aggregated_batch

    .outputMode("complete")  
        # "complete" = réécrit tout le tableau des statistiques à chaque batch
        # nécessaire car on utilise groupBy + count

    .option("checkpointLocation", "output/checkpoint_stats")  
        # sauvegarde pour reprise en cas d’arrêt

    .trigger(processingTime="1 seconds")

    .start()
)


# Garde le programme actif
# Tant que des tickets arrivent, Spark continue d’écrire les fichiers
# Pour arrêter : Ctrl + C
query_transformed.awaitTermination()
query_aggregated.awaitTermination()