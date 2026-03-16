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

import numpy as np

import dash_mantine_components as dmc


def berechne_taupunkt(temp_c, rel_hum):
    """
    Berechnet den Taupunkt in °C basierend auf der Magnus-Formel.

    :param temp_c: Temperatur in °C
    :param rel_hum: Relative Luftfeuchtigkeit in % (0-100)
    :return: Taupunkt in °C
    """
    # Konstanten für die Magnus-Formel (Werte für Wasser über 0°C)
    b = 17.62
    c = 243.12

    # Sicherheitshalber RH begrenzen, um Logarithmus-Fehler zu vermeiden
    rel_hum = max(0.01, min(100, rel_hum))

    # Zwischenschritt: Gamma berechnen
    gamma = np.log(rel_hum / 100.0) + (b * temp_c) / (c + temp_c)

    # Endberechnung
    taupunkt = (c * gamma) / (b - gamma)

    return round(taupunkt, 2)


hovertemplate_temp = """
    <b>Temperatur:</b> %{y}° C<br>
    <b>Datum:</b> %{x|%d.%m.%Y}<br>
    <b>Uhrzeit:</b> %{x|%H:%M:%S} Uhr
    <extra></extra>
    """

hovertemplate_humi = """
    <b>Luftfeuchtigkeit:</b> %{y} %<br>
    <b>Datum:</b> %{x|%d.%m.%Y}<br>
    <b>Uhrzeit:</b> %{x|%H:%M:%S} Uhr
    <extra></extra>
    """

hovertemplate_dew = """
    <b>Taupunkt:</b> %{y}° C<br>
    <b>Datum:</b> %{x|%d.%m.%Y}<br>
    <b>Uhrzeit:</b> %{x|%H:%M:%S} Uhr
    <extra></extra>
    """


hovertemplate_temp_avg = """
    <b>Temperatur:</b> %{y}° C<br>
    <b>Uhrzeit:</b> %{x} Uhr
    <extra></extra>
    """

hovertemplate_humi_avg = """
    <b>Luftfeuchtigkeit:</b> %{y} %<br>
    <b>Uhrzeit:</b> %{x} Uhr
    <extra></extra>
    """

hovertemplate_dew_avg = """
    <b>Taupunkt:</b> %{y}° C<br>
    <b>Uhrzeit:</b> %{x} Uhr
    <extra></extra>
    """


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

DISPLAY_TIMEZONE = "Europe/Berlin"


# Connection zu SQL vorbereiten
connection_url = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_url, pool_pre_ping=True)


# App erstellen
app = Dash(
    __name__,
    url_base_pathname="/dashboard-a/",
    external_scripts=["https://cdn.plot.ly/plotly-locale-de-latest.js"],
)

app.title = "Dashboard Hygrothermic Motion Analysis"


app.layout = dmc.MantineProvider(
    forceColorScheme="light",  # "light" oder "dark"
    theme={
        "primaryColor": "blue",
        "fontFamily": "sans-serif",
    },
    children=[
        dmc.Container(
            [
                dmc.Title("Live Sensor Data – LoRaWAN remote sensing", order=1),
                html.Br(),
                dmc.Title("Hygrothermische Messung", order=2),
                dcc.Graph(id="output_main_figure", config={"locale": "de"}),
                html.Br(),
                dmc.Title("Hygrothermische Messung: Tagesdurchschnitt", order=2),
                dcc.Graph(id="output_average_figure", config={"locale": "de"}),
                html.Br(),
                dmc.Title("Bewegungsmeldungen Garage", order=2),
                dcc.Graph(id="output_motion_figure", config={"locale": "de"}),
                dcc.Interval(id="refresh_interval", interval=60 * 1000, n_intervals=0),
            ]
        )
    ],
)


# FileSystemCache für die Speicherung auf der "Festplatte"
# Daten in den Cache laden
cache = Cache(
    app.server,
    config={
        "CACHE_TYPE": "FileSystemCache",
        "CACHE_DIR": "cache-directory",
        "CACHE_DEFAULT_TIMEOUT": 600,
        "CACHE_THRESHOLD": 10,
    },
)


@cache.memoize(timeout=300)
def get_data_from_sql():
    df = pd.read_sql(sql=f"SELECT * FROM {DB_TABLE} WHERE 1", con=engine)
    df["received_at"] = (
        df["received_at"].dt.tz_localize("UTC").dt.tz_convert(DISPLAY_TIMEZONE)
    )
    return df


def main():
    app.run(debug=False, port=8080, host="0.0.0.0")


@app.callback(
    [
        Output("output_main_figure", "figure"),
        Output("output_average_figure", "figure"),
        Output("output_motion_figure", "figure"),
    ],
    Input("refresh_interval", "n_intervals"),
)
def update_graphics(*args):
    # Basisdaten abrufen und global sortieren
    df = get_data_from_sql()
    df_plot = df.sort_values(by="received_at")

    # ==========================================
    # 1. Livedaten Temperatur und Luftfeuchtigkeit
    # ==========================================

    df_plot["Taupunkt"] = df_plot.apply(
        lambda row: berechne_taupunkt(
            row["uplink_message.decoded_payload.TempC_SHT"],
            row["uplink_message.decoded_payload.Hum_SHT"],
        ),
        axis=1,
    )

    # 1.1 Datenaufbereitung
    df_plot_molten = df_plot.melt(
        id_vars="received_at",
        value_vars=[
            "uplink_message.decoded_payload.Hum_SHT",
            "uplink_message.decoded_payload.TempC_SHT",
            "Taupunkt",
        ],
    )
    df_plot_molten["variable"] = df_plot_molten["variable"].map(
        {
            "uplink_message.decoded_payload.Hum_SHT": "Luftfeuchtigkeit",
            "uplink_message.decoded_payload.TempC_SHT": "Temperatur",
            "Taupunkt": "Taupunkt",
        }
    )

    # 1.2 Basis-Figur
    fig = make_subplots(
        specs=[[{"secondary_y": True}]],
        subplot_titles=["Livedaten Temperatur und Luftfeuchtigkeit"],
    )

    # 1.3 Zwischenplot (px)
    fig_px = px.line(df_plot_molten, x="received_at", y="value", color="variable")

    # 1.4 Traces übertragen
    fig.add_trace(fig_px.data[0], secondary_y=True)
    fig.add_trace(fig_px.data[1], secondary_y=False)
    fig.add_trace(fig_px.data[2], secondary_y=False)

    # 1.5 Trace-Farben + Hovertemplates
    fig.update_traces(line_color="blue", hovertemplate=hovertemplate_humi, selector=0)
    fig.update_traces(line_color="orange", hovertemplate=hovertemplate_temp, selector=1)
    fig.update_traces(
        visible="legendonly",
        hovertemplate=hovertemplate_dew,
        selector=2,
    )

    # 1.6 Achsen und Layout
    fig.update_yaxes(title_text="<b>Temperatur</b> °C", secondary_y=False)
    fig.update_yaxes(title_text="<b>Luftfeuchtigkeit</b> % RF", secondary_y=True)
    fig.update_xaxes(title_text=f"Datum und Uhrzeit ({DISPLAY_TIMEZONE})")

    # ==========================================
    # 2. Livedaten Bewegung (Motion Plot)
    # ==========================================

    # 2.1 Datenaufbereitung
    df_plot_motion = df_plot[
        ["received_at", "uplink_message.decoded_payload.Move_count"]
    ].copy()
    df_plot_motion["motiondiffs"] = df_plot_motion[
        "uplink_message.decoded_payload.Move_count"
    ].diff()

    # 2.2 Basis-Figur
    fig_motion = make_subplots(subplot_titles=["Livedaten Bewegung"])

    # 2.3 Zwischenplot (px)
    fig_motion_temp = px.line(df_plot_motion, x="received_at", y="motiondiffs")

    # 2.4 Traces übertragen
    fig_motion.add_trace(fig_motion_temp.data[0])

    # 2.5 Trace-Farben (Hier im Original keine explizit definiert, bleibt daher leer)

    # 2.6 Achsen und Layout
    fig_motion.update_yaxes(title_text="<b>Bewegungen</b> (Anzahl)")

    # ==========================================
    # 3. Tagesdurchschnitte (Weekly Plot)
    # ==========================================

    # 3.1 Datenaufbereitung
    df_weekly = df_plot[
        [
            "received_at",
            "uplink_message.decoded_payload.Hum_SHT",
            "uplink_message.decoded_payload.TempC_SHT",
        ]
    ].copy()

    df_weekly.index = pd.to_datetime(df_weekly["received_at"])
    df_weekly = df_weekly.drop(columns=["received_at"])

    # Duplikate aggregieren (Durchschnitt)
    df_weekly = df_weekly.groupby(df_weekly.index).mean()

    new_indices = pd.date_range(
        start=df_weekly.index.min().floor("h"),
        end=df_weekly.index.max().ceil("h"),
        freq="5min",
    )

    df_weekly = df_weekly.reindex(df_weekly.index.union(new_indices))

    df_weekly_interpolated = df_weekly.interpolate(method="time")
    df_weekly_interpolated = df_weekly_interpolated.reindex(new_indices)
    df_weekly_interpolated = df_weekly_interpolated.dropna()

    weekdayfilter = df_weekly_interpolated.index.dayofweek.map(
        lambda x: x in [0, 1, 2, 3, 4, 5, 6]
    )
    df_daily = df_weekly_interpolated[weekdayfilter].copy()

    df_daily_averaged = df_daily.groupby(df_daily.index.time).mean()
    df_daily_averaged.index = df_daily_averaged.index.rename("index")

    df_daily_averaged = df_daily_averaged.rename(
        columns={
            "uplink_message.decoded_payload.TempC_SHT": "Temperatur",
            "uplink_message.decoded_payload.Hum_SHT": "Luftfeuchtigkeit",
        }
    )

    df_daily_averaged["Taupunkt"] = df_daily_averaged.apply(
        lambda row: berechne_taupunkt(row["Temperatur"], row["Luftfeuchtigkeit"]),
        axis=1,
    )

    df_plot_averages = df_daily_averaged.reset_index().melt(id_vars=["index"])

    # 3.2 Basis-Figur
    fig_average_day = make_subplots(
        specs=[[{"secondary_y": True}]],
        subplot_titles=["Tagesdurchschnitte: Temperatur und Luftfeuchtigkeit"],
    )

    # 3.3 Zwischenplot (px)
    fig_average_day_px = px.line(
        df_plot_averages, x="index", y="value", color="variable"
    )

    # 3.4 Traces übertragen
    fig_average_day.add_trace(fig_average_day_px.data[0], secondary_y=True)
    fig_average_day.add_trace(fig_average_day_px.data[1], secondary_y=False)
    fig_average_day.add_trace(fig_average_day_px.data[2], secondary_y=False)

    # 3.5 Trace-Farben
    fig_average_day.update_traces(
        line_color="blue", hovertemplate=hovertemplate_humi_avg, selector=0
    )
    fig_average_day.update_traces(
        line_color="orange", hovertemplate=hovertemplate_humi_avg, selector=1
    )

    fig_average_day.update_traces(
        visible="legendonly", hovertemplate=hovertemplate_dew_avg, selector=2
    )

    # 3.6 Achsen und Layout
    fig_average_day.update_yaxes(title_text="<b>Temperatur</b> °C", secondary_y=False)
    fig_average_day.update_yaxes(
        title_text="<b>Luftfeuchtigkeit</b> % RF", secondary_y=True
    )
    fig_average_day.update_xaxes(title_text=f"Datum und Uhrzeit ({DISPLAY_TIMEZONE})")
    fig_average_day.update_legends()

    fig.update_layout(uirevision='keepzoom')
    fig_average_day.update_layout(uirevision='keepzoom')
    fig_motion.update_layout(uirevision='keepzoom')


    return fig, fig_average_day, fig_motion


if __name__ == "__main__":
    main()
