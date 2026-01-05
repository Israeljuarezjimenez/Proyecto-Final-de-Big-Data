import glob
import os

import pandas as pd
import streamlit as st

DATA_DIR = os.getenv("TLC_EXPORT_DIR", "data/export")

st.set_page_config(page_title="NYC TLC Pipeline", layout="wide")

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;600;700&display=swap');
    html, body, [class*="css"]  {
        font-family: 'Space Grotesk', sans-serif;
    }
    .stApp {
        background: radial-gradient(circle at 10% 20%, #f6f1e8 0%, #f2efe9 45%, #e9f0ef 100%);
    }
    .kpi-card {
        padding: 16px;
        border-radius: 12px;
        background: #ffffffcc;
        border: 1px solid #e1e3e6;
        box-shadow: 0 6px 18px rgba(10, 10, 10, 0.08);
    }
    .titulo {
        font-size: 28px;
        font-weight: 700;
        color: #1f2a35;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown('<div class="titulo">Pipeline NYC TLC - Dashboard</div>', unsafe_allow_html=True)

def listar_periodos(base_dir):
    patron_year = os.path.join(base_dir, "year=*")
    years = glob.glob(patron_year)
    periodos = []
    for year_dir in years:
        year = os.path.basename(year_dir).split("=")[-1]
        for month_dir in glob.glob(os.path.join(year_dir, "month=*")):
            month = os.path.basename(month_dir).split("=")[-1]
            periodos.append((year, month))
    return sorted(periodos)

periodos = listar_periodos(DATA_DIR)
if periodos:
    opciones = [f"{y}-{m}" for y, m in periodos]
    seleccion = st.selectbox("Periodo", opciones, index=len(opciones) - 1)
    year_sel, month_sel = seleccion.split("-")
    DATA_DIR = os.path.join(DATA_DIR, f"year={year_sel}", f"month={month_sel}")


def cargar_tabla(nombre):
    patron = os.path.join(DATA_DIR, nombre, "*.csv")
    archivos = glob.glob(patron)
    if not archivos:
        return None
    return pd.concat([pd.read_csv(a) for a in archivos], ignore_index=True)


def mostrar_kpis(kpis):
    if kpis is None or kpis.empty:
        st.warning("No hay KPIs exportados")
        return

    total_viajes = int(kpis.loc[0, "total_viajes"])
    duracion_promedio = float(kpis.loc[0, "duracion_promedio_min"])
    tarifa_promedio = float(kpis.loc[0, "tarifa_promedio"])

    col1, col2, col3 = st.columns(3)
    col1.metric("Total viajes", f"{total_viajes:,}")
    col2.metric("Duracion promedio (min)", f"{duracion_promedio:.2f}")
    col3.metric("Tarifa promedio", f"${tarifa_promedio:.2f}")

kpis = cargar_tabla("kpis")
mostrar_kpis(kpis)

st.markdown("---")

viajes = cargar_tabla("viajes_por_hora_dia")
duracion = cargar_tabla("duracion_promedio_hora")
tarifa = cargar_tabla("tarifa_promedio_hora")

if viajes is not None and not viajes.empty:
    viajes_hora = (
        viajes.groupby("pickup_hour")["total_viajes"].sum().reset_index()
    )
    st.subheader("Viajes por hora")
    st.line_chart(viajes_hora.set_index("pickup_hour"))

    viajes_dia = viajes.groupby("pickup_dow")["total_viajes"].sum().reset_index()
    st.subheader("Viajes por dia de la semana")
    st.bar_chart(viajes_dia.set_index("pickup_dow"))
else:
    st.warning("No se encontraron datos de viajes por hora y dia")

if duracion is not None and not duracion.empty:
    st.subheader("Duracion promedio por hora")
    st.line_chart(duracion.set_index("pickup_hour"))

if tarifa is not None and not tarifa.empty:
    st.subheader("Tarifa promedio por hora")
    st.line_chart(tarifa.set_index("pickup_hour"))
