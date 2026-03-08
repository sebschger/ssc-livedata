# This script is supposed to do the following
# It should provide an API-Endpoint for The Things Network (TTN)
# The received Data should then be forwared (appended) to
# an existing SQL Database


# Library Imports
import pandas as pd
import threading
import waitress
from flask import Flask, request, Blueprint
from queue import Queue
from dotenv import load_dotenv
from sqlalchemy import create_engine
import os
import time


# Settings
# todo: Hier arbeiten wir demnächst mit environment-variablen

load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_TABLE = os.getenv("DB_TABLE")
DB_USER = os.getenv("DB_USER")
DB_PORT = os.getenv("DB_PORT")
DB_HOST = os.getenv("DB_HOST")
DB_PASS = os.getenv("DB_PASS")
API_PORT = os.getenv("API_PORT")
API_WEBHOOKNAME = os.getenv("API_WEBHOOKNAME")
DATA_MESSAGE_LIST_NAME = os.getenv("DATA_MESSAGE_LIST_NAME")

def timestamp():
    return time.strftime("%Y-%m-%d %H:%M:%s")

# Globale Variablen
data_queue = Queue()
sql_connection_string = (
    f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)
sql_connection = create_engine(sql_connection_string, pool_pre_ping=True)

listener = Flask(__name__)
bp = Blueprint("snsr-dt-pshr", __name__)


def interpret_message():
    # hier ist die grundsätzliche Datenverarbeitung
    # des per POST empfangenen Pakets

    while True:
        item = data_queue.get()
        try:
            publish_sql(clean_data(item))
        except Exception as e:
            print(timestamp(), "Problem beim Parsen der Daten:\n")
            print(item)
        finally:
            data_queue.task_done()


processing_thread = threading.Thread(target=interpret_message, daemon=True)


@bp.route(API_WEBHOOKNAME, methods=["POST"])
def handle_incoming():
    print(timestamp(), "Data was received.")
    data_queue.put(request.get_json())
    return "OK", 200


# Für den Health Check
@bp.route("/", methods=["GET"])
def answer_health_check():
    print(timestamp(), "Probably: Health check received")
    return "OK", 200


listener.register_blueprint(bp, url_prefix="/sensor-data-pusher")


def clean_data(item: dict):
    # Wir gehen momentan mal davon aus, dass item ein dict mit den Daten ist
    df_raw = pd.json_normalize(item)

    # Diese logik wird die im item gelagerte Info (dict) in einen DF umwandeln
    # In diesem DF wird aber eine Spalte mite einer Liste von JSON-Strings sein
    # Das sind die Metadaten. Von denen nehmen wir nur die ersten

    metadata_column = next(
        (
            column
            for column in df_raw.columns.to_list()
            if DATA_MESSAGE_LIST_NAME in column
        ),
        None,
    )

    if metadata_column in df_raw.columns:
        metadata = df_raw[metadata_column].apply(lambda x: x[0] if x else [])

        df_metadata = pd.json_normalize(metadata)
        df_metadata.index = df_raw.index
        df_metadata.columns = metadata_column + "." + df_metadata.columns

        df_cleaned = pd.concat(
            [df_raw.drop(columns=[metadata_column]), df_metadata], axis=1
        )

    else:
        df_cleaned = df_raw

    # Liste in Correlation_Ids in einen String umwandeln
    df_cleaned["correlation_ids"] = df_cleaned["correlation_ids"].apply(
        lambda x: "; ".join(x)
    )

    return df_cleaned


def publish_sql(df_submit: pd.DataFrame):
    try:
        df_submit.to_sql(DB_TABLE, con=sql_connection, if_exists="append")
    except Exception as e:
        print(timestamp(), f"Appending the incoming data to the mysql table failed:\n\n{e}")


def main():
    # Wir starten den parallelen Prozess
    processing_thread.start()

    # Wir starten den Server
    try:
        waitress.serve(listener, port=int(API_PORT), host="0.0.0.0")
    except Exception as e:
        print(timestamp(), f"Problem mit dem Waitress-Prozess (Server):\n{e}")
    finally:
        # Wir sollten aber warten, bis alle Queue-Objekte abgearbeitet sind
        data_queue.join()


if __name__ == "__main__":
    main()
