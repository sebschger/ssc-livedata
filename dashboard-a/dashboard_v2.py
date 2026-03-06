# Lernnotizen
# Vergessen hatte ich dash / html
# Dass die app ein .layout hat
# Unterelemente im Div als Liste machen
# app.callback muss vor main kommen!
# Die Inputs/Outputs brauchen die ID und das "Ziel" bzw. component property, die sie steuern, oder die von ihnen gesteuert wird
# Man kann Links und Rechts unterschiedliche Achsen in Plotly machen,
# wenn man plotly-go für die hauptfigure nutzt (leer), und dann
# die erste achse ohne secondary_y hinzufügt (gern aus dem px)
# die zweite achse mit secondary_y = True hinzufügt (auch gern aus dem px)
# flask_caching / Cache ist neu für mich
# Der Cache sorgt dafür, dass die Daten eine gewisse Gültigkeit haben,
# wenn sie häufiger abegfragt werden (z.B. bei Callbacks) spart das Last auf die Datenbank
# Eine zusätzliche Möglichkeit, daten user-state-basiert zwischenzuspeichern
# ist dss.store. Hiermit kann man im "Client" z.B. Datenreihen speichern,
# die dann für die Erstellung von Graphen hergenommen werden


from dash import Dash, Input, Output
from dash import dcc, html
from flask_caching import Cache

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
from pathlib import Path
import os

import dash_mantine_components as dmc


# Environmentdaten laden
current_dir = Path(__file__).resolve().parent
env_location = current_dir / ".env"
load_dotenv(env_location)

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_TABLE = os.getenv("DB_TABLE")


# Connection zu SQL vorbereiten
connection_url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_url, pool_pre_ping=True)


# App erstellen
app = Dash(
    __name__,
    url_base_pathname='/dashboard-a/'
)

app.title = "Dashboard Hygrothermic Motion Analysis"


app.layout = dmc.MantineProvider(
    forceColorScheme="light", # "light" oder "dark"
    theme={
        "primaryColor": "blue",
        "fontFamily": "sans-serif",
    },
    children=[
        dmc.Container([
            dmc.Title("Hygrothermische Messung Garage", order=1),
            dmc.Text("Das Problem mit withNormalizeCSS ist gelöst."),
            dcc.Graph(id="output_main_figure"),
            dcc.Interval(id="refresh_interval", interval=60 * 1000, n_intervals=0),

        ])
    ]
)


# Daten in den Cache laden
cache = Cache(
    app.server, config={"CACHE_TYPE": "FileSystemCache", "CACHE_DIR": "cache-directory"}
)


@cache.memoize(timeout=300)
def get_data_from_sql():
    df = pd.read_sql(sql=f"SELECT * FROM {DB_TABLE} WHERE 1", con=engine)
    print(df.columns.to_list())
    return df


def main():
    app.run(debug=True, port=8080, host="0.0.0.0")



@app.callback(
    Output("output_main_figure", "figure"),
    Input("refresh_interval", "n_intervals"),
)
def update_graphics(*args):
    df = get_data_from_sql()

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.update_yaxes(title_text="<b>Temperatur</b> °C", secondary_y=False)
    fig.update_yaxes(title_text="<b>Luftfeuchtigkeit</b> % RF", secondary_y=True)
    fig.update_xaxes(title_text="Datum und Uhrzeit")

    df_plot = df.sort_values(by="received_at")

    fig_px_temp = px.line(
        df_plot,
        x="received_at",
        y="uplink_message.decoded_payload.TempC_SHT",
        hover_data={"received_at": "|%b %d, %Y"},
    )
    fig_px_temp.update_traces(line_color="orange")
    fig_px_temp.update_traces(
        hovertemplate="""
    <b>Temperatur:</b> %{y}° C<br>
    <b>Datum:</b> %{x|%d.%m.%Y}<br>
    <b>Uhrzeit:</b> %{x|%H:%M:%S} Uhr
    <extra></extra>
    """
    )

    fig_px_hum = px.line(
        df_plot, x="received_at", y="uplink_message.decoded_payload.Hum_SHT"
    )
    fig_px_hum.update_traces(line_color="blue")
    fig_px_hum.update_traces(
        hovertemplate="""
    <b>Luftfeuchtigkeit:</b> %{y} %<br>
    <b>Datum:</b> %{x|%d.%m.%Y}<br>
    <b>Uhrzeit:</b> %{x|%H:%M:%S} Uhr
    <extra></extra>
    """
    )

    fig.add_trace(fig_px_temp.data[0], secondary_y=False)
    fig.add_trace(fig_px_hum.data[0], secondary_y=True)

    return fig


if __name__ == "__main__":
    main()
