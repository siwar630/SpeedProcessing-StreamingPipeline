# SpeedProcessing-StreamingPipeline

## À propos
Ce projet est un pipeline de traitement de données en temps réel intégrant **Apache Airflow** pour l'orchestration des tâches, **Apache Kafka** pour la gestion des flux de données, **Apache Spark** pour le traitement des données et **Cassandra** pour le stockage.

## Objectifs du Projet
- Manipuler les structures de Spark, notamment RDD et DataFrames.
- Tester Spark en mode cluster sans YARN.
- Implémenter la pipeline HDFS, Spark, Hive et Sqoop pour effectuer un batch processing.

## Spark Streaming : le traitement en temps réel
**Spark Streaming** est une extension de l’API principale de Spark. Elle permet de créer des applications de streaming évolutives, à haut débit et tolérantes aux pannes, tout cela sans avoir à utiliser un ensemble d’outils complètement différent et en restant dans l’environnement de Spark.


### Fonctionnement de Spark Streaming
Chaque application de streaming doit définir un **intervalle de lot**. Plus l’intervalle est court, plus la latence entre la disponibilité des données en entrée et le début de leur traitement est faible. Cependant, cela ne définit pas nécessairement le temps nécessaire pour terminer le traitement. Si le traitement prend plus de temps que l’intervalle de lot, alors le traitement s’accumule, ce qui peut entraîner une instabilité de l’application.

## Installation
Pour installer et exécuter ce projet, suivez ces étapes :
1. 
   Exécutez Docker pour démarrer les services :
   ```bash
  docker-compose up -d

