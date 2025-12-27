import mysql.connector

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "asdfmt09",
    "database": "skiapi"
}


def create_table():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS resorts (
            id VARCHAR(50) PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            country VARCHAR(50) NOT NULL,
            region VARCHAR(50) NOT NULL,
            continent VARCHAR(20) NOT NULL,
            latitude DOUBLE NOT NULL,
            longitude DOUBLE NOT NULL,
            village_m INT NOT NULL,
            min_m INT NOT NULL,
            max_m INT NOT NULL,
            ski_area_name VARCHAR(100) NOT NULL,
            ski_area_type VARCHAR(20) NOT NULL
        )
    """)

    conn.commit()
    cursor.close()
    conn.close()


def get_user_input():
    print("\nBitte gib die Skigebiets-Daten ein:\n")

    return {
        "id": input("ID (z. B. at-st-anton): "),
        "name": input("Name des Skigebiets: "),
        "country": input("Land: "),
        "region": input("Region: "),
        "continent": input("Kontinent: "),
        "latitude": float(input("Breitengrad (Latitude): ")),
        "longitude": float(input("Längengrad (Longitude): ")),
        "village_m": int(input("Höhe Ort (m): ")),
        "min_m": int(input("Minimale Höhe (m): ")),
        "max_m": int(input("Maximale Höhe (m): ")),
        "ski_area_name": input("Name des Skigebietsverbunds: "),
        "ski_area_type": input(
            "Skigebietstyp (alpine, nordic, mixed, glacier, ...): "
        )
    }


def insert_resort(data):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    sql = """
        INSERT INTO resorts (
            id, name, country, region, continent,
            latitude, longitude,
            village_m, min_m, max_m,
            ski_area_name, ski_area_type
        )
        VALUES (
            %(id)s, %(name)s, %(country)s, %(region)s, %(continent)s,
            %(latitude)s, %(longitude)s,
            %(village_m)s, %(min_m)s, %(max_m)s,
            %(ski_area_name)s, %(ski_area_type)s
        )
    """

    cursor.execute(sql, data)
    conn.commit()

    cursor.close()
    conn.close()


def main():
    create_table()
    data = get_user_input()
    insert_resort(data)
    print("\n✅ Skigebiet erfolgreich in MySQL gespeichert!")


if __name__ == "__main__":
    main()