from flask import Flask, render_template, request, url_for
import os
import time
import pymysql
import pandas as pd
import folium
import plotly.express as px
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from geopy.exc import GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable
import getpass

app = Flask(__name__)

DB_USER = os.getenv("DB_USER")
DB_PWD = os.getenv("DB_PWD")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT"))


def connexion():
    user = DB_USER
    pwd = DB_PWD or getpass.getpass(f"Enter password for MySQL user '{user}': ")
    db_name = DB_NAME
    db_host = DB_HOST

    try:
        conn = pymysql.connect(
            host=db_host,
            user=user,
            password=pwd,
            db=db_name,
            port=DB_PORT,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10,
        )
        print("Connected successfully to MySQL!")
        return conn

    except pymysql.MySQLError as e:
        print(f"MySQL connection failed: {e}")
        return None


def create_velo_table():
    try:
        conn = connexion()
        cursor = conn.cursor()
        cursor.execute(
            """
        CREATE TABLE IF NOT EXISTS velo (
            id INT AUTO_INCREMENT PRIMARY KEY,
            station_name VARCHAR(255),
            lon DOUBLE,
            lat DOUBLE,
            city VARCHAR(100),
            adresse VARCHAR(255),
            available_bikes INT,
            available_bike_stands VARCHAR(3),
            status VARCHAR(50),
            last_update DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            UNIQUE KEY uniq_lon_lat (lon, lat)
        );
        """
        )
        cursor.close()
        conn.close()

    except pymysql.Error as e:
        print("Error in connection:", e)


def add_db(df):
    try:
        conn = connexion()
        cursor = conn.cursor()

        create_velo_table()

        # Iterate over the DataFrame rows
        for index, row in df.iterrows():

            lat = row["lat"]
            lon = row["lon"]

            results = get_one_db(lon, lat, conn)
            if results is not None and len(results) == 1:
                # Update the existing row
                query = """
                UPDATE velo
                SET station_name = %s, city = %s, adresse = %s, available_bikes = %s, available_bike_stands = %s, status = %s, last_update = %s
                WHERE lon = %s AND lat = %s
                """
                values = (
                    row["station_name"],
                    row["city"],
                    row["adresse"],
                    row["available_bikes"],
                    row["available_bike_stands"],
                    row["status"],
                    row["lon"],
                    row["lat"],
                    row["last_update"],
                )
            else:
                # Insert a new row
                query = """
                INSERT INTO velo (station_name, lon, lat, city, adresse, available_bikes, available_bike_stands, status, last_update)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                values = (
                    row["station_name"],
                    row["lon"],
                    row["lat"],
                    row["city"],
                    row["adresse"],
                    row["available_bikes"],
                    row["available_bike_stands"],
                    row["status"],
                    row["last_update"],
                )

            try:
                cursor.execute(query, values)
            except pymysql.Error as err:
                print(f"Error processing row {index}: {err}")
                print(f"Values: {values}")

        conn.commit()
        cursor.close()
        conn.close()
    except pymysql.Error as e:
        print("Error in add_db:", e)


def db_lenght():
    try:
        conn = connexion()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM velo")
        conn.close()
        return cursor.fetchall()
    except pymysql.Error as e:
        print("Error in connection:", e)


def update_all_db(df):
    try:
        conn = connexion()
        cursor = conn.cursor()
        cursor.execute(f"UPDATE velo {df}")
        conn.close()
    except pymysql.Error as e:
        print("Error in connection:", e)


def update_one_db(lon, lat, df):
    try:
        conn = connexion()
        cursor = conn.cursor()
        cursor.execute(f"UPDATE velo {df} WHERE lon = {lon} AND lat = {lat}")
        conn.close()
    except pymysql.Error as e:
        print("Error in connection:", e)


def get_all():
    try:
        conn = connexion()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM velo")
        results = cursor.fetchall()
        conn.close()
        return results
    except pymysql.Error as e:
        print("Error in connection:", e)


def get_all_from_city(city: str):
    try:
        conn = connexion()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM velo WHERE city = %s", (city,))
        results = cursor.fetchall()
        conn.close()
        return results
    except pymysql.Error as e:
        print("Error in connection:", e)
        return []


def get_restricted_all_from_city(
    city: str, center_lat: float, center_lon: float, nb_stand: int
):
    """
    Returns all bike stations in the given city within 'nb_stand' stands
    near the coordinates (center_lat, center_lon).
    """
    try:
        conn = connexion()
        cursor = conn.cursor()

        # Haversine distance formula (in kilometers)
        query = """
        SELECT *,
            (6371 * acos(
                cos(radians(%s)) * cos(radians(lat)) *
                cos(radians(lon) - radians(%s)) +
                sin(radians(%s)) * sin(radians(lat))
            )) AS distance_km
        FROM velo
        WHERE city = %s
        ORDER BY distance_km ASC
        LIMIT %s; 
        """

        values = (center_lat, center_lon, center_lat, city, nb_stand)
        cursor.execute(query, values)
        results = cursor.fetchall()

        conn.close()
        return results

    except pymysql.Error as e:
        print("Error in connection:", e)
        return []


def delete_one_db(lon, lat):
    try:
        conn = connexion()
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM velo WHERE lon = {lon} AND lat = {lat}")
        conn.close()
    except pymysql.Error as e:
        print("Error in connection:", e)


def get_one_db(lon, lat, conn):
    try:
        cursor = conn.cursor()
        query = "SELECT * FROM velo WHERE lon = %s AND lat = %s"
        values = (lon, lat)
        cursor.execute(query, values)

        return cursor.fetchall()
    except pymysql.Error as e:
        print("Error in connection:", e)


def get_user_pos(place: str):
    g = Nominatim(user_agent="conrad", timeout=5)

    # Use the ratelimited wrapper (Nominatim requires >= 1s between calls)
    geocode = RateLimiter(g.geocode, min_delay_seconds=1)

    try:
        # Ask for structured address details directly (avoid extra reverse call)
        loc = geocode(
            place,
            addressdetails=True,
            exactly_one=True,
            language="fr",
            country_codes="fr",  # remove if you want worldwide
        )
        if not loc:
            return None

        addr = (loc.raw or {}).get("address", {})
        city = (
            addr.get("city")
            or addr.get("town")
            or addr.get("village")
            or addr.get("municipality")
            or addr.get("county")  # fallback
        )

        return {
            "latitude": float(loc.latitude),
            "longitude": float(loc.longitude),
            "city": city,
            "address": addr,  # full structured address
            "display_name": loc.address,  # nice human string
        }

    except (GeocoderTimedOut, GeocoderServiceError, GeocoderUnavailable) as e:
        print(f"Geocoding error for '{place}': {e}")
        return None


def get_nearest_station(lat: float, lon: float):
    try:
        conn = connexion()
        cursor = conn.cursor()

        # Haversine distance formula (in kilometers)
        query = """
        SELECT *,
            (6371 * acos(
                cos(radians(%s)) * cos(radians(lat)) *
                cos(radians(lon) - radians(%s)) +
                sin(radians(%s)) * sin(radians(lat))
            )) AS distance_km
        FROM velo
        ORDER BY distance_km ASC
        LIMIT 1; 
        """

        values = (lat, lon, lat)
        cursor.execute(query, values)
        results = cursor.fetchall()

        conn.close()
        return results

    except pymysql.Error as e:
        print("Error in connection:", e)
        return []


@app.route("/")
def home():
    return render_template("index.html")


@app.route("/view", methods=["GET", "POST"])
def view():
    # default (Lille)
    user_lat, user_lon = 50.633331, 3.066670
    user_city = None
    nb_stand = None

    if request.method == "POST":
        addr_text = (request.form.get("addr") or "").strip()
        nb_stand_text = (request.form.get("nb_stand") or "").strip()

        # geocode
        pos = get_user_pos(addr_text) if addr_text else None
        if pos:
            print(pos)
            user_lat = pos["latitude"]
            user_lon = pos["longitude"]
            user_city = pos["city"]

        # parse radius
        try:
            nb_stand = int(nb_stand_text) if nb_stand_text else None
            if nb_stand is not None and nb_stand <= 0:
                nb_stand = None
        except ValueError:
            nb_stand = None

    # choose function to get stations
    if user_city and nb_stand:
        rows = get_restricted_all_from_city(user_city, user_lat, user_lon, nb_stand)
    elif user_city:
        rows = get_all_from_city(user_city)
    else:
        rows = get_all()

    # using records avoiding column order issues.
    df = pd.DataFrame.from_records(rows)
    for col in [
        "id",
        "station_name",
        "lon",
        "lat",
        "city",
        "adresse",
        "available_bikes",
        "available_bike_stands",
        "status",
        "last_update",
    ]:
        if col not in df.columns:
            df[col] = None

    # treemaps
    os.makedirs("static/plots", exist_ok=True)

    df_tm = df.dropna(subset=["city"]).copy()

    df_tm["available_bikes"] = pd.to_numeric(
        df_tm["available_bikes"], errors="coerce"
    ).fillna(0)

    # treemap: City -> Station
    if not df_tm.empty:
        fig_city = px.treemap(
            df_tm,
            path=["city", "station_name"],
            values="available_bikes",
            hover_data=["available_bike_stands", "status"],
            title="Disponibilité des vélos par Ville → Station (taille = vélos disponibles)",
        )
        fig_city.update_traces(root_color="lightgrey")
        fig_city.write_html(
            "static/plots/treemap_by_city.html", include_plotlyjs="cdn", full_html=True
        )

    if not df_tm.empty:
        # group to count stations per (city, status)
        grouped = (
            df_tm.groupby(["city", "status"], dropna=False)
            .size()
            .reset_index(name="stations")
        )
        fig_status = px.treemap(
            grouped,
            path=["city", "status"],
            values="stations",
            title="Nombre de stations par Ville → Statut (taille = nb stations)",
        )
        fig_status.update_traces(root_color="lightgrey")
        fig_status.write_html(
            "static/plots/treemap_by_status.html",
            include_plotlyjs="cdn",
            full_html=True,
        )

    # folium map
    m = folium.Map(location=[user_lat, user_lon], zoom_start=12)
    folium.Marker(
        [user_lat, user_lon],
        tooltip=f"Votre position{f' ({user_city})' if user_city else ''}",
        icon=folium.Icon(color="blue"),
    ).add_to(m)

    for _, row in df.iterrows():
        folium.Marker(
            [float(row["lat"]), float(row["lon"])],
            tooltip=row.get("station_name") or "Station",
            popup=(
                f"Vélos disponibles: {row.get('available_bikes')}"
                f"\nPlaces disponibles: {row.get('available_bike_stands')}"
                f"\nStatut: {row.get('status')}"
            ),
            icon=folium.Icon(
                color="green" if int(row.get("available_bikes") or 0) > 0 else "red"
            ),
        ).add_to(m)

    # getting nearest station and drawing route
    end_lat = end_lon = None
    nearest = get_nearest_station(user_lat, user_lon) or []
    if nearest:
        ns = nearest[0]
        try:
            end_lat = float(ns["lat"])
            end_lon = float(ns["lon"])
        except (KeyError, TypeError, ValueError):
            end_lat = end_lon = None

    if end_lat is not None and end_lon is not None:
        from folium import Element

        map_var = m.get_name()

        routing_inject = Element(
            f"""
    <link rel="stylesheet"
        href="https://unpkg.com/leaflet-routing-machine@latest/dist/leaflet-routing-machine.css" />
    <script src="https://unpkg.com/leaflet-routing-machine@latest/dist/leaflet-routing-machine.js"></script>
    <script>
    document.addEventListener('DOMContentLoaded', function() {{
    L.Routing.control({{
        waypoints: [
        L.latLng({user_lat}, {user_lon}),
        L.latLng({end_lat}, {end_lon})
        ],
        router: L.Routing.osrmv1({{ serviceUrl: 'https://router.project-osrm.org/route/v1' }}),
        lineOptions: {{ styles: [{{ color: 'red', weight: 5, opacity: 0.8 }}] }},
        addWaypoints: false,
        draggableWaypoints: false,
        fitSelectedRoutes: true,
        show: false,
        createMarker: function() {{ return null; }}  // 👈 désactive les marqueurs auto
    }}).addTo({map_var});
    }});
    </script>
    """
        )
    m.get_root().html.add_child(routing_inject)

    os.makedirs("static", exist_ok=True)
    m.save("static/map.html")
    return render_template("main.html")


if __name__ == "__main__":
    app.run(debug=True)
