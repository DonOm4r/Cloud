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

# Título de la aplicación
st.title("Visualización de datos desde Snowflake 🎨")

# Consulta SQL actualizada
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
LIMIT 10000
"""

try:
    # Conectar y ejecutar la consulta
    conn = connect_to_snowflake()
    st.write("Conectando a Snowflake...")
    df = pd.read_sql(query, conn)

    if df.empty:
        st.error("No se encontraron datos en la consulta. Verifique la tabla o los filtros.")
    else:
        # Crear pestañas
        tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(
            ["Vista General", "Filtros Dinámicos", "Gráficos", "Análisis de Anomalías",
             "Comparación", "Trayectoria 3D", "Consultas Personalizadas"]
        )

        # --- TAB 1: Vista General ---
        with tab1:
            st.subheader("Vista General")
            st.write("Datos cargados:", df.head())
            st.write("Estadísticas generales:")
            st.write(df.describe())

        # --- TAB 2: Filtros Dinámicos ---
        with tab2:
            st.subheader("Filtros Dinámicos")
            if "DEVICE_ID" in df.columns and not df["DEVICE_ID"].isnull().all():
                device_id_filter = st.selectbox(
                    "Selecciona un dispositivo:",
                    options=df["DEVICE_ID"].unique(),
                    index=0
                )
            else:
                st.warning("La columna DEVICE_ID no tiene valores disponibles.")
                device_id_filter = None

            date_range = st.date_input("Selecciona rango de fechas:", [])
            filtered_df = df[df["DEVICE_ID"] == device_id_filter] if device_id_filter else df
            if date_range:
                filtered_df = filtered_df[
                    (filtered_df["TIME"] >= pd.to_datetime(date_range[0])) &
                    (filtered_df["TIME"] <= pd.to_datetime(date_range[1]))
                ]
            st.write("Datos filtrados:", filtered_df)

        # --- TAB 3: Gráficos ---
        with tab3:
            st.subheader("Gráficos")
            st.line_chart(df.set_index("TIME")[["X", "Y", "Z"]])
            bar_data = df.groupby("SESSION_ID")["MESSAGE_ID"].count().reset_index()
            bar_data.columns = ["SESSION_ID", "MESSAGE_COUNT"]
            st.bar_chart(bar_data.set_index("SESSION_ID"))

        # --- TAB 4: Análisis de Anomalías ---
        with tab4:
            st.subheader("Análisis de Anomalías")
            x_limit = st.slider("Límite superior para X", float(df["X"].min()), float(df["X"].max()), value=10.0)
            anomalies = df[df["X"] > x_limit]
            st.write("Registros con valores anómalos en X:", anomalies)

        # --- TAB 5: Comparación ---
        with tab5:
            st.subheader("Comparación de Dispositivos")
            if "SESSION_ID" in df.columns and not df["SESSION_ID"].isnull().all():
                session_filter = st.selectbox(
                    "Selecciona una sesión para comparar:",
                    options=df["SESSION_ID"].unique(),
                    index=0
                )
                session_data = df[df["SESSION_ID"] == session_filter]
                st.line_chart(session_data.set_index("TIME")[["X", "Y", "Z"]])
            else:
                st.warning("No hay sesiones disponibles para comparar.")

        # --- TAB 6: Trayectoria 3D ---
        with tab6:
            st.subheader("Trayectoria 3D")
            fig = px.scatter_3d(df, x='X', y='Y', z='Z', color='TIME', title='Trayectoria 3D')
            st.plotly_chart(fig)

        # --- TAB 7: Consultas Personalizadas ---
        with tab7:
            st.subheader("Consultas Personalizadas")
            user_query = st.text_area("Escribe tu consulta SQL:", "SELECT * FROM MY_TABLE LIMIT 10")
            try:
                user_df = pd.read_sql(user_query, conn)
                st.write("Resultados de tu consulta:", user_df)
            except Exception as e:
                st.error(f"Error al ejecutar la consulta: {e}")

except Exception as e:
    st.error(f"Error al cargar los datos o generar gráficos: {e}")


