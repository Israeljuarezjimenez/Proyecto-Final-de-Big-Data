import glob
import os

import altair as alt
import pandas as pd
import streamlit as st

DATA_DIR_BASE = os.getenv("TLC_EXPORT_DIR", "data/export")
DATA_DIR_SELECCION = DATA_DIR_BASE

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

MAPA_DIAS = {
    1: "Dom",
    2: "Lun",
    3: "Mar",
    4: "Mie",
    5: "Jue",
    6: "Vie",
    7: "Sab",
}
ORDEN_DIAS = ["Dom", "Lun", "Mar", "Mie", "Jue", "Vie", "Sab"]

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


def extraer_periodo_desde_ruta(ruta):
    partes = ruta.split(os.sep)
    year = None
    month = None
    for parte in partes:
        if parte.startswith("year="):
            year = parte.split("=")[-1]
        if parte.startswith("month="):
            month = parte.split("=")[-1]
    return year, month

periodos = listar_periodos(DATA_DIR_BASE)
if periodos:
    opciones = [f"{y}-{m}" for y, m in periodos]
    seleccion = st.selectbox("Periodo", opciones, index=len(opciones) - 1)
    year_sel, month_sel = seleccion.split("-")
    DATA_DIR_SELECCION = os.path.join(
        DATA_DIR_BASE, f"year={year_sel}", f"month={month_sel}"
    )


def cargar_tabla(nombre):
    patron = os.path.join(DATA_DIR_SELECCION, nombre, "*.csv")
    archivos = glob.glob(patron)
    if not archivos:
        return None
    return pd.concat([pd.read_csv(a) for a in archivos], ignore_index=True)


def cargar_kpis_historico(base_dir):
    patron = os.path.join(base_dir, "year=*", "month=*", "kpis", "*.csv")
    archivos = glob.glob(patron)
    if not archivos:
        return None
    filas = []
    for archivo in archivos:
        df = pd.read_csv(archivo)
        if df.empty:
            continue
        year, month = extraer_periodo_desde_ruta(archivo)
        df["year"] = year
        df["month"] = month
        filas.append(df)
    if not filas:
        return None
    historico = pd.concat(filas, ignore_index=True)
    historico["periodo"] = historico["year"].astype(str) + "-" + historico["month"].astype(str)
    historico["periodo_orden"] = (
        historico["year"].astype(int) * 100 + historico["month"].astype(int)
    )
    return historico.sort_values("periodo_orden")


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

historico = cargar_kpis_historico(DATA_DIR_BASE)
if historico is not None and not historico.empty:
    st.subheader("Tendencia mensual de KPIs")
    hist_base = historico[["periodo", "total_viajes", "duracion_promedio_min", "tarifa_promedio"]].copy()
    chart_kpi = (
        alt.Chart(hist_base)
        .transform_fold(
            ["total_viajes", "duracion_promedio_min", "tarifa_promedio"],
            as_=["metrica", "valor"],
        )
        .mark_line(point=True)
        .encode(
            x=alt.X("periodo:N", title="Periodo", sort=hist_base["periodo"].tolist()),
            y=alt.Y("valor:Q", title="Valor"),
            color=alt.Color("metrica:N", title="Metrica"),
            tooltip=[
                alt.Tooltip("periodo:N"),
                alt.Tooltip("metrica:N"),
                alt.Tooltip("valor:Q"),
            ],
        )
    )
    st.altair_chart(chart_kpi, use_container_width=True)

st.markdown("---")

viajes = cargar_tabla("viajes_por_hora_dia")
duracion = cargar_tabla("duracion_promedio_hora")
tarifa = cargar_tabla("tarifa_promedio_hora")

if viajes is not None and not viajes.empty:
    viajes_hora = (
        viajes.groupby("pickup_hour")["total_viajes"].sum().reset_index()
    )
    viajes_hora["pickup_hour"] = viajes_hora["pickup_hour"].astype(int)
    st.subheader("Viajes por hora")
    st.line_chart(viajes_hora.set_index("pickup_hour"))

    viajes_dia = viajes.groupby("pickup_dow")["total_viajes"].sum().reset_index()
    viajes_dia["pickup_dow"] = viajes_dia["pickup_dow"].astype(int)
    viajes_dia["dia"] = viajes_dia["pickup_dow"].map(MAPA_DIAS)
    st.subheader("Viajes por dia de la semana")
    st.bar_chart(viajes_dia.set_index("dia"))

    total_viajes = viajes_hora["total_viajes"].sum()
    if total_viajes > 0:
        hora_pico = viajes_hora.loc[viajes_hora["total_viajes"].idxmax(), "pickup_hour"]
        dia_pico = viajes_dia.loc[viajes_dia["total_viajes"].idxmax(), "dia"]
        viajes_fin = viajes_dia[viajes_dia["pickup_dow"].isin([1, 7])]["total_viajes"].sum()
        viajes_noche = viajes_hora[
            (viajes_hora["pickup_hour"] >= 22) | (viajes_hora["pickup_hour"] <= 5)
        ]["total_viajes"].sum()

        col_a, col_b, col_c, col_d = st.columns(4)
        col_a.metric("Hora pico", f"{int(hora_pico):02d}:00")
        col_b.metric("Dia pico", dia_pico)
        col_c.metric("Fin de semana (%)", f"{(viajes_fin / total_viajes) * 100:.1f}")
        col_d.metric("Viajes nocturnos (%)", f"{(viajes_noche / total_viajes) * 100:.1f}")

        viajes_hora["participacion_pct"] = (
            viajes_hora["total_viajes"] / total_viajes * 100
        )
        st.subheader("Participacion por hora (%)")
        st.bar_chart(viajes_hora.set_index("pickup_hour")["participacion_pct"])

        top_horas = viajes_hora.sort_values("total_viajes", ascending=False).head(5)
        bottom_horas = viajes_hora.sort_values("total_viajes", ascending=True).head(5)
        col_top, col_bottom = st.columns(2)
        col_top.subheader("Top 5 horas con mas viajes")
        col_top.dataframe(top_horas[["pickup_hour", "total_viajes"]], use_container_width=True)
        col_bottom.subheader("Top 5 horas con menos viajes")
        col_bottom.dataframe(bottom_horas[["pickup_hour", "total_viajes"]], use_container_width=True)

        viajes_semana = viajes_dia[~viajes_dia["pickup_dow"].isin([1, 7])]["total_viajes"].sum()
        comparativo = pd.DataFrame(
            {
                "grupo": ["Semana", "Fin de semana"],
                "total_viajes": [viajes_semana, viajes_fin],
            }
        )
        st.subheader("Comparativo semana vs fin de semana")
        st.bar_chart(comparativo.set_index("grupo"))

        demanda = viajes_hora.sort_values("pickup_hour").copy()
        demanda["acumulado_pct"] = demanda["total_viajes"].cumsum() / total_viajes * 100
        st.subheader("Demanda acumulada por hora (%)")
        st.line_chart(demanda.set_index("pickup_hour")["acumulado_pct"])

    st.subheader("Mapa de calor: viajes por hora y dia")
    heatmap = viajes.copy()
    heatmap["pickup_hour"] = heatmap["pickup_hour"].astype(int)
    heatmap["pickup_dow"] = heatmap["pickup_dow"].astype(int)
    heatmap["dia"] = heatmap["pickup_dow"].map(MAPA_DIAS)
    chart_heat = (
        alt.Chart(heatmap)
        .mark_rect()
        .encode(
            x=alt.X("pickup_hour:O", title="Hora", sort=list(range(24))),
            y=alt.Y("dia:O", title="Dia", sort=ORDEN_DIAS),
            color=alt.Color("total_viajes:Q", title="Viajes", scale=alt.Scale(scheme="blues")),
            tooltip=["pickup_hour", "dia", "total_viajes"],
        )
    )
    st.altair_chart(chart_heat, use_container_width=True)
else:
    st.warning("No se encontraron datos de viajes por hora y dia")

if duracion is not None and not duracion.empty:
    st.subheader("Duracion promedio por hora")
    st.line_chart(duracion.set_index("pickup_hour"))

if tarifa is not None and not tarifa.empty:
    st.subheader("Tarifa promedio por hora")
    st.line_chart(tarifa.set_index("pickup_hour"))

if duracion is not None and tarifa is not None and not duracion.empty and not tarifa.empty:
    st.subheader("Relacion entre tarifa y duracion por hora")
    merge_hora = duracion.merge(tarifa, on="pickup_hour", how="inner")
    merge_hora["tarifa_por_min"] = (
        merge_hora["tarifa_promedio"] / merge_hora["duracion_promedio_min"]
    )
    scatter = (
        alt.Chart(merge_hora)
        .mark_circle(size=80, opacity=0.7)
        .encode(
            x=alt.X("duracion_promedio_min:Q", title="Duracion promedio (min)"),
            y=alt.Y("tarifa_promedio:Q", title="Tarifa promedio"),
            color=alt.Color("pickup_hour:O", title="Hora"),
            tooltip=["pickup_hour", "duracion_promedio_min", "tarifa_promedio"],
        )
    )
    st.altair_chart(scatter, use_container_width=True)

    st.subheader("Tarifa promedio por minuto")
    tarifa_min = merge_hora.copy()
    tarifa_min["tarifa_por_min"] = tarifa_min["tarifa_por_min"].round(3)
    st.line_chart(tarifa_min.set_index("pickup_hour")["tarifa_por_min"])

    top_tarifa = tarifa_min.sort_values("tarifa_por_min", ascending=False).head(5)
    st.subheader("Top horas con tarifa por minuto mas alta")
    st.dataframe(
        top_tarifa[["pickup_hour", "tarifa_por_min"]], use_container_width=True
    )

metricas = cargar_tabla("metricas_modelo")
if metricas is not None and not metricas.empty:
    st.markdown("---")
    st.subheader("Resumen del modelo")
    met = metricas.iloc[0].to_dict()
    col_m1, col_m2, col_m3 = st.columns(3)
    col_m1.metric("RMSE", f"{float(met.get('rmse', 0)):.3f}")
    col_m2.metric("MAE", f"{float(met.get('mae', 0)):.3f}")
    col_m3.metric("R2", f"{float(met.get('r2', 0)):.3f}")
    st.write(
        {
            "algoritmo": met.get("algoritmo"),
            "rows_train": met.get("rows_train"),
            "rows_test": met.get("rows_test"),
            "fecha_entrenamiento": met.get("fecha_entrenamiento"),
        }
    )

errores = cargar_tabla("errores_por_hora")
if errores is not None and not errores.empty:
    st.subheader("Error por hora (MAE y RMSE)")
    errores["pickup_hour"] = errores["pickup_hour"].astype(int)
    chart_err = (
        alt.Chart(errores)
        .transform_fold(["mae", "rmse"], as_=["metrica", "valor"])
        .mark_line(point=True)
        .encode(
            x=alt.X("pickup_hour:O", title="Hora", sort=list(range(24))),
            y=alt.Y("valor:Q", title="Error"),
            color=alt.Color("metrica:N", title="Metrica"),
            tooltip=[
                alt.Tooltip("pickup_hour:O"),
                alt.Tooltip("metrica:N"),
                alt.Tooltip("valor:Q"),
            ],
        )
    )
    st.altair_chart(chart_err, use_container_width=True)

top_origen = cargar_tabla("top_origen")
top_destino = cargar_tabla("top_destino")
if top_origen is not None and top_destino is not None:
    st.markdown("---")
    st.subheader("Top zonas de origen y destino")
    col_o, col_d = st.columns(2)
    col_o.dataframe(top_origen, use_container_width=True)
    col_d.dataframe(top_destino, use_container_width=True)

pagos = cargar_tabla("pagos")
if pagos is not None and not pagos.empty:
    st.subheader("Distribucion por tipo de pago")
    st.bar_chart(pagos.set_index("payment_type")["total_viajes"])

vendor = cargar_tabla("vendor")
if vendor is not None and not vendor.empty:
    st.subheader("Distribucion por vendor")
    st.bar_chart(vendor.set_index("vendor_id")["total_viajes"])

distancia_bins = cargar_tabla("distancia_bins")
if distancia_bins is not None and not distancia_bins.empty:
    st.subheader("Duracion y tarifa promedio por distancia")
    distancia_bins = distancia_bins.sort_values("distancia_orden")
    st.line_chart(
        distancia_bins.set_index("distancia_bin")[
            ["duracion_promedio_min", "tarifa_promedio"]
        ]
    )

variabilidad_hora = cargar_tabla("variabilidad_hora")
if variabilidad_hora is not None and not variabilidad_hora.empty:
    st.subheader("Variabilidad de duracion por hora")
    variabilidad_hora["pickup_hour"] = variabilidad_hora["pickup_hour"].astype(int)
    band = (
        alt.Chart(variabilidad_hora)
        .mark_area(opacity=0.25)
        .encode(
            x=alt.X("pickup_hour:O", title="Hora", sort=list(range(24))),
            y=alt.Y("duracion_p25:Q", title="Duracion (min)"),
            y2="duracion_p75:Q",
        )
    )
    line = (
        alt.Chart(variabilidad_hora)
        .mark_line(color="#2b5b84")
        .encode(
            x=alt.X("pickup_hour:O", title="Hora", sort=list(range(24))),
            y=alt.Y("duracion_promedio_min:Q", title="Duracion (min)"),
            tooltip=["pickup_hour", "duracion_promedio_min", "duracion_p50"],
        )
    )
    st.altair_chart(band + line, use_container_width=True)

variabilidad_dia = cargar_tabla("variabilidad_dia")
if variabilidad_dia is not None and not variabilidad_dia.empty:
    st.subheader("Variabilidad de duracion por dia")
    variabilidad_dia["pickup_dow"] = variabilidad_dia["pickup_dow"].astype(int)
    variabilidad_dia["dia"] = variabilidad_dia["pickup_dow"].map(MAPA_DIAS)
    chart_dia = (
        alt.Chart(variabilidad_dia)
        .mark_bar()
        .encode(
            x=alt.X("dia:O", title="Dia", sort=ORDEN_DIAS),
            y=alt.Y("duracion_promedio_min:Q", title="Duracion (min)"),
            color=alt.Color("duracion_std:Q", title="Std duracion", scale=alt.Scale(scheme="tealblues")),
            tooltip=["dia", "duracion_promedio_min", "duracion_std"],
        )
    )
    st.altair_chart(chart_dia, use_container_width=True)
