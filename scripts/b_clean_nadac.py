import pandas as pd
import os

#-------------------------------------------------------------------------------------------------------
# Este script hace una limpieza del archivo nadac.csv y lo guarda en la carpeta data/cleaned.
# La limpieza consiste en pasar a formato fecha la columna 'As of Date' y filtrar por la más reciente
# con tal de tener los medicamentos publicados más recientemente. Hace un drop de nulos de algunas columnas,
# y comprueba que los valores de 'NDC' todos tengan 11 dígitos.
#-------------------------------------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NADAC_PATH = os.path.join(BASE_DIR, "data", "raw", "nadac.csv")
CLEANED_DIR = os.path.join(BASE_DIR, "data", "cleaned")
CLEANED_PATH = os.path.join(CLEANED_DIR, "nadac_clean.csv")

def clean_nadac(path=NADAC_PATH):
    """
    Limpia y transforma el archivo NADAC.

    Pasos principales:
    1. Carga el archivo NADAC conservando los ceros a la izquierda en la columna `NDC`.
    2. Convierte la columna `As of Date` a formato datetime y elimina fechas inválidas.
    3. Filtra únicamente los registros correspondientes a la fecha más reciente.
    4. Elimina registros con `NDC` nulo o con longitud distinta de 11 dígitos.
    5. Elimina duplicados por `NDC`.
    6. Elimina registros con `NADAC Per Unit` nulo.
    7. Descarta columnas irrelevantes para el análisis.
    8. Exporta el dataset limpio a un archivo CSV.

    Parameters
    ----------
    path : str, optional
        Ruta al archivo NADAC en bruto. Por defecto se utiliza `NADAC_PATH`.

    Returns
    -------
    pandas.DataFrame
        DataFrame resultante con los datos NADAC limpios, correspondiente a la fecha más reciente.
    """
    print("Cargando archivo NADAC...")
    # Lee la columna NDC como string, para conservar los ceros al incio en caso que se ocupen
    df = pd.read_csv(path, dtype={'NDC': str})

    # Convierte el formato de la columna de fecha
    df['As of Date'] = pd.to_datetime(df['As of Date'], format="%m/%d/%Y", errors='coerce')

    # Quito filas con fechas no válidas
    df = df.dropna(subset=['As of Date'])

    # Filtro solo la fecha más reciente
    latest_date = df['As of Date'].max()
    df = df[df['As of Date'] == latest_date]
    print(f"Filtrado previo a limpieza. Fecha más reciente: {latest_date.date()}, registros resultantes: {len(df)}")

    # Limpieza NDC
    df = df.dropna(subset=['NDC'])  # elimina NDCs nulos
    df = df[df['NDC'].astype(str).str.len() == 11]  # filtrar solo NDCs de 11 dígitos

    # Elimina duplicados por NDC
    df = df.drop_duplicates(subset='NDC')

    # --- Elimina filas con NADAC per Unit nulo ---
    df = df.dropna(subset=['NADAC Per Unit'])

    # Elimina columnas innecesarias
    columns_to_drop = [
        'Pharmacy Type Indicator',
        'Explanation Code',
        'Classification for Rate Setting',
        'Corresponding Generic Drug NADAC Per Unit',
        'Corresponding Generic Drug Effective Date'
    ]

    df = df.drop(columns=columns_to_drop, errors='ignore')

    print(f"Limpieza finalizada. Fecha más reciente: {latest_date.date()}, registros: {len(df)}")

    # Guardo como CSV limpio
    os.makedirs(CLEANED_DIR, exist_ok=True)
    df.to_csv(CLEANED_PATH, index=False)
    print(f"Archivo limpio guardado en: {CLEANED_PATH}")

    return df

if __name__ == "__main__":
    df_clean = clean_nadac()

# correr script desde terminal con
# python scripts/b_clean_nadac.py