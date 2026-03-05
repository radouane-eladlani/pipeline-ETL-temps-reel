# KafkaProducer permet d'envoyer des messages vers un broker Kafka-compatible.
# Même si on utilise Redpanda, il est compatible avec Kafka.
# Donc cette classe va nous permettre d’envoyer des tickets vers Redpanda.
from kafka import KafkaProducer, KafkaAdminClient 

# json permet de transformer un dictionnaire Python
# en format JSON (format standard utilisé dans les systèmes distribués).
import json

# time permet d’ajouter une pause entre chaque envoi.
# On l’utilise pour simuler du "temps réel".
import time

# random permet de générer des valeurs aléatoires
# (exemple : priorité ou client_id).
import random

# datetime permet de récupérer la date et l’heure actuelles.
from datetime import datetime

# uuid permet de créer un identifiant unique.
# Chaque ticket doit avoir un ID unique comme en production.
import uuid


# ============================================================
# 2️ CONNEXION À REDPANDA
# ============================================================

# Ici on crée un "producteur".
# Un producteur = programme qui envoie des données vers un broker.
# Le broker ici est Redpanda.

producer = KafkaProducer(

    # Adresse du broker Kafka auquel se connecter
    # "redpanda-0" = nom du conteneur Redpanda défini dans docker-compose
    # 9092 = port interne de Kafka dans le conteneur
    # Ce port est celui que Spark et Python utiliseront
    bootstrap_servers="redpanda-0:9092",
    # Avant d’envoyer un message,
    # on doit le transformer en JSON et l’encoder en UTF-8.
    # Sinon Redpanda ne pourra pas le lire correctement.
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Boucle d’attente pour s’assurer que le topic existe
admin = KafkaAdminClient(bootstrap_servers="redpanda-0:9092")

print("Attente que le topic 'client_tickets' soit disponible...")
while True:
    try:
        topics = admin.list_topics()
        if "client_tickets" in topics:
            print("Topic 'client_tickets' trouvé, démarrage de la production...")
            break
    except Exception as e:
        pass
    time.sleep(2)  # vérifie toutes les 2 secondes

# Création du producer Kafka
producer = KafkaProducer(
    bootstrap_servers="redpanda-0:9092",
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# ============================================================
# 3️ LISTES DE VALEURS POSSIBLES
# ============================================================

# Types de demande possibles pour un ticket.
types_demande = [
    "Support Technique",
    "Facturation",
    "Information",
    "Réclamation"
]

# Niveaux de priorité possibles.
priorites = [
    "Basse",
    "Moyenne",
    "Haute",
    "Critique"
]


# ============================================================
# 4️ FONCTION QUI CRÉE UN TICKET
# ============================================================

def generer_ticket():

    # Cette fonction simule la création d’un ticket client.
    # Elle retourne un dictionnaire Python.
    # Ce dictionnaire sera envoyé dans Redpanda.

    return {

        # uuid4() crée un identifiant totalement unique.
        # On le transforme en texte avec str().
        "ticket_id": str(uuid.uuid4()),

        # On simule un client avec un numéro aléatoire.
        # Exemple : client 4587
        "client_id": random.randint(1000, 9999),

        # On enregistre la date et l'heure actuelles.
        # Format lisible : 2026-02-24 14:30:12
        "creation_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),

        # Texte fixe pour simuler une demande.
        "demande": "Problème rencontré avec le service.",

        # On choisit au hasard un type de demande.
        "type_demande": random.choice(types_demande),

        # On choisit au hasard un niveau de priorité.
        "priorite": random.choice(priorites)
    }


# ============================================================
# 5️ ENVOI CONTINU DES TICKETS (SIMULATION TEMPS RÉEL)
# ============================================================

print("Production de tickets en cours...")

nb_tickets = 0
max_tickets = 5 

# Boucle infinie :
# Cela simule un système qui reçoit des tickets 24h/24.
while nb_tickets < max_tickets:

    # On génère un ticket
    ticket = generer_ticket()

    # On envoie le ticket dans le topic "client_tickets".
    # Topic = canal de stockage des messages dans Redpanda.
    producer.send(
    "client_tickets",
    key=str(ticket["client_id"]).encode('utf-8'),
    value=ticket
    )
    # flush() force l’envoi immédiat.
    # Sans flush, les messages peuvent rester temporairement en mémoire.
    # Ici on veut s'assurer que chaque ticket part immédiatement.
    producer.flush()

    # On affiche dans le terminal pour voir ce qui est envoyé.
    print("Ticket envoyé :", ticket)

    # On attend 2 secondes avant d’envoyer le suivant.
    # Cela simule un flux réaliste.
    time.sleep(2)

    nb_tickets += 1

