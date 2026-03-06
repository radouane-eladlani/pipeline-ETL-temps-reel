# POC Pipeline de Streaming avec Redpanda, PySpark et Docker

## Objectif du projet

Ce projet démontre la mise en place d’un **pipeline de données en streaming** permettant de simuler la gestion de tickets clients.

Les tickets sont générés par un **Producer Python**, envoyés dans **Redpanda (Kafka)** puis traités en **temps réel avec PySpark Structured Streaming**.

Les données sont ensuite stockées au format **Parquet** pour permettre leur analyse.

---

# Architecture du pipeline

Le pipeline suit les étapes suivantes :

1. Génération de tickets clients (Producer Python)
2. Envoi des tickets dans un topic Redpanda (Kafka)
3. Consommation des messages par PySpark Streaming
4. Transformation et enrichissement des données
5. Calcul des statistiques
6. Stockage des résultats en fichiers Parquet

---

# Diagramme du pipeline (Mermaid)

https://mermaid.live/edit#pako:eNptkM-O2jAQxl_FmjNLSQiB5FCJhd0eKlWocGqMKjeeJBbJOOs_ainigXgOXmxN2FX30JP9aeb3fTNzglJLhByqVv8uG2Ec2605cVoWG6OlL9GwzdE1mjinnSoP6CwrW4UUXqs6314vds_p4eEzeyy-o-wFScE-sa-iOogbo3tVvhE_3d1gfwt4ZDdmVWyO216YA9s6g6JTVA_V1VBdFzsjyFbadMIpTUyiZVITXS9og_nSWlXTvXS9vHjVI7O-77VxH1yeimVtrpd66AvUN939Mhi82Ns8LAzA3LHHAVoP0HPxrMpGobFsI8yLR_fhAO59qvvynJ4G5sv_mK0LudapIO0eRlAbJSF3xuMIOgyL3SScODHGwTXYIYc8fCVWwreOA6dzwMJdf2jdvZNG-7qBvBKtDcr3UjhcK1Eb8a8FSaJZaU8O8tngAPkJ_kAeJ_E4mmWzKI6ydDGJkxEcIU-ScZKkUTpdzOfTNEum5xH8HSIn43QRT-ZRlGTTJIujOD2_An0AxKo

# Presentation du projet
https://www.loom.com/share/85907b4d39d84e6c807cfb4e2240631b