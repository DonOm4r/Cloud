import snowflake.connector
import pandas as pd
import streamlit as st
from plotly import express as px

# Función para conectarse a Snowflake
def connect_to_snowflake():
    conn = snowflake.connector.connect(
        user=st.secrets["SNOWFLAKE"]["USER"],
        password=st.secrets["SNOWFLAKE"]["PASSWORD"],
        account=st.secrets["SNOWFLAKE"]["ACCOUNT"],
        warehouse=st.secrets["SNOWFLAKE"]["WAREHOUSE"],
        database=st.secrets["SNOWFLAKE"]["DATABASE"],
        schema=st.secrets["SNOWFLAKE"]["SCHEMA"],
        role=st.secrets["SNOWFLAKE"]["ROLE"]
    )
    return conn

# Función para ejecutar una consulta SQL
def fetch_data(query):
    conn = connect_to_snowflake()
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    columns = [col[0] for col in cursor.description]
    cursor.close()
    conn.close()
    return pd.DataFrame(results, columns=columns)

# Título de la aplicación
st.title("Dashboard Interactivo: Conexión a Snowflake")

# Consulta SQL para cargar datos
query = """
SELECT
    DATA:"deviceId"::STRING AS DEVICE_ID,
    DATA:"messageId"::INT AS MESSAGE_ID,
    DATA:"sessionId"::STRING AS SESSION_ID,
    payload.value:"name"::STRING AS NAME,
    TO_TIMESTAMP_NTZ(payload.value:"time"::VARCHAR) AS TIME,
    payload.value:"values"."x"::FLOAT AS X,
    payload.value:"values"."y"::FLOAT AS Y,
    payload.value:"values"."z"::FLOAT AS Z
FROM
    MY_TABLE,
    LATERAL FLATTEN(input => DATA:"payload") AS PAYLOAD
ORDER BY TIME
LIMIT 500
"""

try:
    # Cargar datos desde Snowflake
    st.write("Conectando a Snowflake...")
    df = fetch_data(query)

    # Mostrar datos en la interfaz
    st.write("Datos cargados (primeros 500 registros):")
    st.write(df.head())

    # Gráficos
    st.subheader("Gráfico de Coordenadas X, Y, Z vs Tiempo")
    fig = px.line(df, x="TIME", y=["X", "Y", "Z"], title="Evolución de Coordenadas")
    st.plotly_chart(fig)

    st.subheader("Trayectoria 3D")
    fig_3d = px.scatter_3d(df, x="X", y="Y", z="Z", color="TIME", title="Trayectoria 3D")
    st.plotly_chart(fig_3d)

except Exception as e:
    st.error(f"Error al cargar los datos: {e}")

