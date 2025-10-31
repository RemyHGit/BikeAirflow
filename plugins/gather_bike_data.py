import os
import pymysql
import pandas as pd
import requests

import os
import pymysql
import getpass

DB_USER = os.getenv("DB_USER")
DB_PWD  = os.getenv("DB_PWD")
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
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor,
            connect_timeout=10,
        )
        print("✅ Connected successfully to MySQL!")
        return conn

    except pymysql.MySQLError as e:
        print(f"❌ MySQL connection failed: {e}")
        return None


def create_velo_table():
    try:
        conn = connexion()
        cursor = conn.cursor()
        cursor.execute("""
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
        """)
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
            
            lat = row['lat']
            lon = row['lon']
            

            results = get_one_db(lon, lat, conn)
            if results is not None and len(results) == 1:
                # Update the existing row
                query = """
                UPDATE velo
                SET station_name = %s, city = %s, adresse = %s, available_bikes = %s, available_bike_stands = %s, status = %s, last_update = %s
                WHERE lon = %s AND lat = %s
                """
                values = (
                    row['station_name'],
                    row['city'],
                    row['adresse'],
                    row['available_bikes'],
                    row['available_bike_stands'],
                    row['status'],
                    row['lon'],
                    row['lat'],
                    row['last_update']
                )
            else:
                # Insert a new row
                query = """
                INSERT INTO velo (station_name, lon, lat, city, adresse, available_bikes, available_bike_stands, status, last_update)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                values = (
                    row['station_name'],
                    row['lon'],
                    row['lat'],
                    row['city'],
                    row['adresse'],
                    row['available_bikes'],
                    row['available_bike_stands'],
                    row['status'],
                    row['last_update']
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


def delete_db(lon, lat):
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

# fetch data from Lille's API
def fetch_lille_bike_data():
    limit = 100
    offset = limit - 100
    dfL = pd.DataFrame()
    while True:
        url = f"https://data.lillemetropole.fr/data/ogcapi/collections/ilevia:vlille_temps_reel/items?f=json&limit={limit}&offset={offset}"
        r = requests.get(url)

        if r.status_code == 200:
            offset += 100
            data = r.json()
            maxLen = data.get("numberMatched", 0)
            all_data_lille = data.get("records", {})
            dfL = pd.concat([dfL, pd.DataFrame(all_data_lille)])

            if len(dfL) == maxLen:
                print(f"Fetching data of Lille's api: {len(dfL)}")
                return dfL

        else:
            print(
                f"Failed to fetch data of Lille's api from {url}: {r.status_code}"
            )
            return None

# fetch data from Paris's API
def fetch_paris_bike_data():
    limit = 100
    offset = limit - 100
    dfP = pd.DataFrame()
    while True:
        url = f"https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records?limit={limit}&offset={offset}"
        r = requests.get(url)
        
        if r.status_code == 200:
            offset += 100
            data = r.json()
            maxLen = data.get("total_count", 0)
            all_data_paris = data.get("results", {})
            dfP = pd.concat([dfP, pd.DataFrame(all_data_paris)])

            if len(dfP) == maxLen:
                print(f"Fetching data of Paris's api: {len(dfP)}")
                return dfP

        else:
            print(
                f"Failed to fetch data of Paris's api from {url}: {r.status_code}"
            )
            return None
        
# create a unified dataframe from both APIs
def global_dataframe():
    data_lille = fetch_lille_bike_data()
    data_paris = fetch_paris_bike_data()
    frames = []

    # --- Lille ---
    if not data_lille.empty:
        nb_places = pd.to_numeric(data_lille["nb_places_dispo"], errors="coerce").fillna(0)
        df_lille = pd.DataFrame({
            "station_name": data_lille["nom"],
            "lon": data_lille["x"],
            "lat": data_lille["y"],
            "city": "Lille",
            "adresse": data_lille["adresse"],
            "available_bikes": pd.to_numeric(data_lille["nb_velos_dispo"], errors="coerce").fillna(0).astype(int),
            "available_bike_stands": nb_places.gt(0).map({True: "Oui", False: "Non"}),
            "status": data_lille["etat"],
            "last_update": pd.to_datetime(data_lille["date_modification"])
        })
        frames.append(df_lille)

    # --- Paris ---
    if not data_paris.empty:
        # éclate lon/lat pour éviter les lambdas sur dict
        coords = pd.json_normalize(data_paris["coordonnees_geo"])
        dp = pd.concat([data_paris.reset_index(drop=True), coords.rename(columns={"lon":"lon","lat":"lat"})], axis=1)

        numdocks = pd.to_numeric(dp["numdocksavailable"], errors="coerce").fillna(0)
        numbikes = pd.to_numeric(dp["numbikesavailable"], errors="coerce").fillna(0)

        df_paris = pd.DataFrame({
            "station_name": dp["name"],
            "lon": dp["lon"],
            "lat": dp["lat"],
            "city": "Paris",
            "adresse": dp["name"] + " " + dp["nom_arrondissement_communes"] + ", " + dp["code_insee_commune"],
            "available_bikes": numbikes.astype(int),
            "available_bike_stands": numdocks.gt(0).map({True: "Oui", False: "Non"}),
            "status": dp["is_renting"].astype(str).str.upper().isin(["OUI","YES","TRUE","1"]).map({True:"EN SERVICE",False:"HORS SERVICE"}),
            "last_update": pd.to_datetime(dp["duedate"])
        })
        frames.append(df_paris)

    cols = ["station_name","lon","lat","city","adresse","available_bikes","available_bike_stands","status", "last_update"]
    df_global = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=cols)
    return df_global

# cleaning
df = global_dataframe()

# Handle missing values
df['station_name'].fillna('Unknown', inplace=True)
df['lon'].fillna(0, inplace=True)
df['lat'].fillna(0, inplace=True)
df['adresse'].fillna('Unknown', inplace=True)

# Convert 'available_bikes' and 'available_bike_stands' to numeric types
df['available_bikes'] = pd.to_numeric(df['available_bikes'], errors='coerce').fillna(0).astype(int)

# Convert 'station_name', 'adresse' and 'status' to string
df['station_name'] = df['station_name'].astype(str)
df['adresse'] = df['adresse'].astype(str)
df['status'] = df['status'].astype(str)

# date
df['last_update'] = pd.to_datetime(df['last_update'], errors='coerce')

# Drop rows where 'lon' or 'lat' are 0
df = df[(df['lon'] != 0) & (df['lat'] != 0)]

print("\n")

# add or update database
add_db(df)