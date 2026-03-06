# This script is supposed to do the following
# It should provide an API-Endpoint for The Things Network (TTN)
# The received Data should then be forwared (appended) to
# an existing SQL Database


# Library Imports
import pandas as pd
import requests
import threading
import waitress
import json
from flask import Flask, request
from queue import Queue
from dotenv import load_dotenv
from sqlalchemy import create_engine



# Settings
# todo: Hier arbeiten wir demnächst mit environment-variablen
DATA_MESSAGE_LIST_NAME = "rx.metadata" # Hier wird definiert, wo die Metadaten des Payloads liegen
DB_TABLE_NAME = "tabellenname" # todo: hier muss noch die env-variable rein
DB_NAME = "dbname" # todo: hier die env-variable…
DB_USER = "user" # todo: hier muss noch die env-variable rein
DB_HOST = "host"  # todo: hier muss noch die env-variable rein
DB_PASS = "dbpass" # todo: hier muss noch die env-variable rein

# Globale Variablen
data_queue = Queue()
sql_connection_string = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}"
sql_connection = create_engine(sql_connection_string)

listener = Flask(__name__)

processing_thread = threading.Thread(target=interpret_message, daemon=True)

def interpret_message():
    # hier ist die grundsätzliche Datenverarbeitung
    # des per POST empfangenen Pakets

    while True:
        item = data_queue.get()
        publish_sql(clean_data(item))
        data_queue.task_done()



@listener.route("/", methods=["POST"])
def handle_incoming():
    data_queue.put(request.get_json())
    return 200, "OK"


def clean_data(item: dict):
    # Wir gehen momentan mal davon aus, dass item ein dict mit den Daten ist
    df_raw = pd.json_normalize(item)

    # Diese logik wird die im item gelagerte Info (dict) in einen DF umwandeln
    # In diesem DF wird aber eine Spalte mite einer Liste von JSON-Strings sein
    # Das sind die Metadaten. Von denen nehmen wir nur die ersten

    metadata_text = df_raw[DATA_MESSAGE_LIST_NAME].apply(lambda x: x[0])
    metadata = json.loads(metadata_text)
    df_metadata = pd.json_normalize(metadata)

    df_cleaned = pd.concat(
        [
            df_raw.drop(columns=[DATA_MESSAGE_LIST_NAME]),
            df_metadata
        ],
        axis=1
    ) 

    return df_cleaned


def publish_sql(df_submit: pd.DataFrame):
    try:
        df_submit.to_sql(DB_TABLE_NAME, con=sql_connection, if_exists="append")
    except Exception as e:
        print("Appending the incoming data to the mysql table failed")



def main():
    # Wir starten den parallelen Prozess
    processing_thread.start()

    # Wir starten den Server
    waitress.serve(listener)

    # processing_thread.join() ist nicht nötig
    # weil der Prozess in einer Endlosschleife ist 
    # und daher nicht zuende geht