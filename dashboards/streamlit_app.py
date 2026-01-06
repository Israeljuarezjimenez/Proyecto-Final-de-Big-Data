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
MAPA_PAGOS = {
    "1": "Tarjeta credito",
    "2": "Efectivo",
    "3": "Sin cargo",
    "4": "Disputa",
    "5": "Desconocido",
    "6": "Anulado",
}
MAPA_VENDOR = {
    "1": "CMT",
    "2": "VTS",
    "3": "Desconocido",
}

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
year_sel = None
month_sel = None
es_anual = False
if periodos:
    years = sorted({y for y, _ in periodos})
    opciones = [str(y) for y in years] + [f"{y}-{m}" for y, m in periodos]
    seleccion = st.selectbox("Periodo", opciones, index=len(opciones) - 1)
    if "-" in seleccion:
        year_sel, month_sel = seleccion.split("-")
        DATA_DIR_SELECCION = os.path.join(
            DATA_DIR_BASE, f"year={year_sel}", f"month={month_sel}"
        )
    else:
        year_sel = seleccion
        es_anual = True
        DATA_DIR_SELECCION = None


def cargar_tabla(nombre):
    if DATA_DIR_SELECCION is None:
        return None
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


def cargar_tabla_historico(base_dir, nombre, year=None):
    patron = os.path.join(base_dir, "year=*", "month=*", nombre, "*.csv")
    archivos = glob.glob(patron)
    if not archivos:
        return None
    filas = []
    for archivo in archivos:
        if year and f"year={year}" not in archivo:
            continue
        df = pd.read_csv(archivo)
        if df.empty:
            continue
        y, m = extraer_periodo_desde_ruta(archivo)
        df["year"] = y
        df["month"] = m
        filas.append(df)
    if not filas:
        return None
    return pd.concat(filas, ignore_index=True)


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

historico = cargar_kpis_historico(DATA_DIR_BASE)
historico_year = None
if historico is not None and not historico.empty and year_sel:
    historico_year = historico[historico["year"].astype(str) == str(year_sel)]
else:
    historico_year = historico

kpis = None
viajes = None
duracion = None
tarifa = None
metricas = None
errores = None
top_origen = None
top_destino = None
pagos = None
vendor = None
distancia_bins = None
variabilidad_hora = None
variabilidad_dia = None

if not es_anual:
    kpis = cargar_tabla("kpis")
    mostrar_kpis(kpis)

    viajes = cargar_tabla("viajes_por_hora_dia")
    duracion = cargar_tabla("duracion_promedio_hora")
    tarifa = cargar_tabla("tarifa_promedio_hora")
    metricas = cargar_tabla("metricas_modelo")
    errores = cargar_tabla("errores_por_hora")
    top_origen = cargar_tabla("top_origen")
    top_destino = cargar_tabla("top_destino")
    pagos = cargar_tabla("pagos")
    vendor = cargar_tabla("vendor")
    distancia_bins = cargar_tabla("distancia_bins")
    variabilidad_hora = cargar_tabla("variabilidad_hora")
    variabilidad_dia = cargar_tabla("variabilidad_dia")

if es_anual and historico_year is not None and not historico_year.empty:
    st.subheader(f"Resumen anual {year_sel}")
    total_viajes_year = historico_year["total_viajes"].sum()
    if total_viajes_year > 0:
        duracion_ponderada = (
            (historico_year["duracion_promedio_min"] * historico_year["total_viajes"]).sum()
            / total_viajes_year
        )
        tarifa_ponderada = (
            (historico_year["tarifa_promedio"] * historico_year["total_viajes"]).sum()
            / total_viajes_year
        )
    else:
        duracion_ponderada = 0.0
        tarifa_ponderada = 0.0

    mes_pico = historico_year.loc[historico_year["total_viajes"].idxmax(), "periodo"]
    mes_bajo = historico_year.loc[historico_year["total_viajes"].idxmin(), "periodo"]

    col_g1, col_g2, col_g3, col_g4 = st.columns(4)
    col_g1.metric("Total viajes (anual)", f"{int(total_viajes_year):,}")
    col_g2.metric("Duracion promedio (ponderada)", f"{duracion_ponderada:.2f} min")
    col_g3.metric("Tarifa promedio (ponderada)", f"${tarifa_ponderada:.2f}")
    col_g4.metric("Mes pico / mes bajo", f"{mes_pico} / {mes_bajo}")

    metricas_year = cargar_tabla_historico(DATA_DIR_BASE, "metricas_modelo", year_sel)
    if metricas_year is not None and not metricas_year.empty:
        metricas_year["rmse"] = pd.to_numeric(metricas_year["rmse"], errors="coerce")
        metricas_year["mae"] = pd.to_numeric(metricas_year["mae"], errors="coerce")
        rmse_prom = float(metricas_year["rmse"].mean())
        mae_prom = float(metricas_year["mae"].mean())
        col_m1, col_m2 = st.columns(2)
        col_m1.metric("RMSE promedio (anual)", f"{rmse_prom:.3f}")
        col_m2.metric("MAE promedio (anual)", f"{mae_prom:.3f}")

    viajes_year = cargar_tabla_historico(DATA_DIR_BASE, "viajes_por_hora_dia", year_sel)
    if viajes_year is not None and not viajes_year.empty:
        viajes_year["pickup_hour"] = viajes_year["pickup_hour"].astype(int)
        viajes_year["pickup_dow"] = viajes_year["pickup_dow"].astype(int)
        viajes_year["dia"] = viajes_year["pickup_dow"].map(MAPA_DIAS)

        viajes_hora_anual = (
            viajes_year.groupby("pickup_hour")["total_viajes"].sum().reset_index()
        )
        viajes_dia_anual = (
            viajes_year.groupby("pickup_dow")["total_viajes"].sum().reset_index()
        )
        viajes_dia_anual["dia"] = viajes_dia_anual["pickup_dow"].map(MAPA_DIAS)

        st.subheader("Viajes por hora (anual)")
        chart_viajes_hora_anual = (
            alt.Chart(viajes_hora_anual)
            .mark_line(point=True)
            .encode(
                x=alt.X("pickup_hour:O", title="Hora", sort=list(range(24))),
                y=alt.Y("total_viajes:Q", title="Total de viajes"),
                tooltip=[
                    alt.Tooltip("pickup_hour:O", title="Hora"),
                    alt.Tooltip("total_viajes:Q", title="Viajes"),
                ],
            )
        )
        st.altair_chart(chart_viajes_hora_anual, use_container_width=True)

        st.subheader("Viajes por dia de la semana (anual)")
        chart_viajes_dia_anual = (
            alt.Chart(viajes_dia_anual)
            .mark_bar()
            .encode(
                x=alt.X("dia:O", title="Dia", sort=ORDEN_DIAS),
                y=alt.Y("total_viajes:Q", title="Total de viajes"),
                tooltip=[
                    alt.Tooltip("dia:O", title="Dia"),
                    alt.Tooltip("total_viajes:Q", title="Viajes"),
                ],
            )
        )
        st.altair_chart(chart_viajes_dia_anual, use_container_width=True)

        st.subheader("Mapa de calor anual: viajes por hora y dia")
        heat_anual = (
            viajes_year.groupby(["pickup_hour", "dia"])["total_viajes"]
            .sum()
            .reset_index()
        )
        chart_heat_anual = (
            alt.Chart(heat_anual)
            .mark_rect()
            .encode(
                x=alt.X("pickup_hour:O", title="Hora", sort=list(range(24))),
                y=alt.Y("dia:O", title="Dia", sort=ORDEN_DIAS),
                color=alt.Color("total_viajes:Q", title="Viajes", scale=alt.Scale(scheme="blues")),
                tooltip=[
                    alt.Tooltip("pickup_hour:O", title="Hora"),
                    alt.Tooltip("dia:O", title="Dia"),
                    alt.Tooltip("total_viajes:Q", title="Viajes"),
                ],
            )
        )
        st.altair_chart(chart_heat_anual, use_container_width=True)

        viajes_hora_mes = (
            viajes_year.groupby(["year", "month", "pickup_hour"])["total_viajes"]
            .sum()
            .reset_index()
        )
        duracion_year = cargar_tabla_historico(
            DATA_DIR_BASE, "duracion_promedio_hora", year_sel
        )
        tarifa_year = cargar_tabla_historico(
            DATA_DIR_BASE, "tarifa_promedio_hora", year_sel
        )
        if duracion_year is not None and not duracion_year.empty:
            duracion_year["pickup_hour"] = duracion_year["pickup_hour"].astype(int)
            duracion_merge = duracion_year.merge(
                viajes_hora_mes, on=["year", "month", "pickup_hour"], how="inner"
            )
            duracion_anual = (
                duracion_merge.groupby("pickup_hour")
                .apply(
                    lambda g: (g["duracion_promedio_min"] * g["total_viajes"]).sum()
                    / g["total_viajes"].sum()
                )
                .reset_index(name="duracion_promedio_min")
            )
            st.subheader("Duracion promedio por hora (anual)")
            chart_duracion_anual = (
                alt.Chart(duracion_anual)
                .mark_line(point=True)
                .encode(
                    x=alt.X("pickup_hour:O", title="Hora", sort=list(range(24))),
                    y=alt.Y("duracion_promedio_min:Q", title="Duracion promedio (min)"),
                    tooltip=[
                        alt.Tooltip("pickup_hour:O", title="Hora"),
                        alt.Tooltip("duracion_promedio_min:Q", title="Duracion promedio (min)", format=".2f"),
                    ],
                )
            )
            st.altair_chart(chart_duracion_anual, use_container_width=True)

        if tarifa_year is not None and not tarifa_year.empty:
            tarifa_year["pickup_hour"] = tarifa_year["pickup_hour"].astype(int)
            tarifa_merge = tarifa_year.merge(
                viajes_hora_mes, on=["year", "month", "pickup_hour"], how="inner"
            )
            tarifa_anual = (
                tarifa_merge.groupby("pickup_hour")
                .apply(
                    lambda g: (g["tarifa_promedio"] * g["total_viajes"]).sum()
                    / g["total_viajes"].sum()
                )
                .reset_index(name="tarifa_promedio")
            )
            st.subheader("Tarifa promedio por hora (anual)")
            chart_tarifa_anual = (
                alt.Chart(tarifa_anual)
                .mark_line(point=True)
                .encode(
                    x=alt.X("pickup_hour:O", title="Hora", sort=list(range(24))),
                    y=alt.Y("tarifa_promedio:Q", title="Tarifa promedio"),
                    tooltip=[
                        alt.Tooltip("pickup_hour:O", title="Hora"),
                        alt.Tooltip("tarifa_promedio:Q", title="Tarifa promedio", format=".2f"),
                    ],
                )
            )
            st.altair_chart(chart_tarifa_anual, use_container_width=True)

    pagos_year = cargar_tabla_historico(DATA_DIR_BASE, "pagos", year_sel)
    if pagos_year is not None and not pagos_year.empty:
        pagos_year["payment_type"] = pagos_year["payment_type"].astype(str)
        pagos_agg = (
            pagos_year.groupby("payment_type")["total_viajes"].sum().reset_index()
        )
        pagos_agg["payment_label"] = (
            pagos_agg["payment_type"].map(MAPA_PAGOS).fillna("Otro")
        )
        pagos_agg["payment_label"] = (
            pagos_agg["payment_type"] + " - " + pagos_agg["payment_label"]
        )
        st.subheader("Distribucion por tipo de pago (anual)")
        chart_pagos_anual = (
            alt.Chart(pagos_agg)
            .mark_bar()
            .encode(
                x=alt.X("payment_label:O", title="Tipo de pago", sort="-y"),
                y=alt.Y("total_viajes:Q", title="Total de viajes"),
                color=alt.Color("payment_label:O", title="Leyenda"),
                tooltip=[
                    alt.Tooltip("payment_label:O", title="Tipo de pago"),
                    alt.Tooltip("total_viajes:Q", title="Viajes"),
                ],
            )
        )
        st.altair_chart(chart_pagos_anual, use_container_width=True)

    vendor_year = cargar_tabla_historico(DATA_DIR_BASE, "vendor", year_sel)
    if vendor_year is not None and not vendor_year.empty:
        vendor_year["vendor_id"] = vendor_year["vendor_id"].astype(str)
        vendor_agg = (
            vendor_year.groupby("vendor_id")["total_viajes"].sum().reset_index()
        )
        vendor_agg["vendor_label"] = (
            vendor_agg["vendor_id"].map(MAPA_VENDOR).fillna("Otro")
        )
        vendor_agg["vendor_label"] = (
            vendor_agg["vendor_id"] + " - " + vendor_agg["vendor_label"]
        )
        st.subheader("Distribucion por vendor (anual)")
        chart_vendor_anual = (
            alt.Chart(vendor_agg)
            .mark_bar()
            .encode(
                x=alt.X("vendor_label:O", title="Vendor", sort="-y"),
                y=alt.Y("total_viajes:Q", title="Total de viajes"),
                color=alt.Color("vendor_label:O", title="Leyenda"),
                tooltip=[
                    alt.Tooltip("vendor_label:O", title="Vendor"),
                    alt.Tooltip("total_viajes:Q", title="Viajes"),
                ],
            )
        )
        st.altair_chart(chart_vendor_anual, use_container_width=True)

    distancia_year = cargar_tabla_historico(DATA_DIR_BASE, "distancia_bins", year_sel)
    if distancia_year is not None and not distancia_year.empty:
        distancia_year["total_viajes"] = pd.to_numeric(
            distancia_year["total_viajes"], errors="coerce"
        )
        dist_agg = (
            distancia_year.groupby(["distancia_bin", "distancia_orden"])
            .apply(
                lambda g: pd.Series(
                    {
                        "total_viajes": g["total_viajes"].sum(),
                        "duracion_promedio_min": (
                            (g["duracion_promedio_min"] * g["total_viajes"]).sum()
                            / g["total_viajes"].sum()
                        )
                        if g["total_viajes"].sum() > 0
                        else 0.0,
                        "tarifa_promedio": (
                            (g["tarifa_promedio"] * g["total_viajes"]).sum()
                            / g["total_viajes"].sum()
                        )
                        if g["total_viajes"].sum() > 0
                        else 0.0,
                    }
                )
            )
            .reset_index()
            .sort_values("distancia_orden")
        )
        st.subheader("Duracion y tarifa promedio por distancia (anual)")
        chart_dist_anual = (
            alt.Chart(dist_agg)
            .transform_fold(
                ["duracion_promedio_min", "tarifa_promedio"],
                as_=["metrica", "valor"],
            )
            .mark_line(point=True)
            .encode(
                x=alt.X("distancia_bin:O", title="Rango de distancia"),
                y=alt.Y("valor:Q", title="Valor"),
                color=alt.Color("metrica:N", title="Leyenda"),
                tooltip=[
                    alt.Tooltip("distancia_bin:O", title="Rango"),
                    alt.Tooltip("metrica:N", title="Metrica"),
                    alt.Tooltip("valor:Q", title="Valor", format=".2f"),
                ],
            )
        )
        st.altair_chart(chart_dist_anual, use_container_width=True)

    errores_year = cargar_tabla_historico(DATA_DIR_BASE, "errores_por_hora", year_sel)
    if errores_year is not None and not errores_year.empty:
        errores_year["pickup_hour"] = errores_year["pickup_hour"].astype(int)
        errores_year["total_viajes"] = pd.to_numeric(
            errores_year["total_viajes"], errors="coerce"
        )
        err_agg = (
            errores_year.groupby("pickup_hour")
            .apply(
                lambda g: pd.Series(
                    {
                        "mae": (
                            (g["mae"] * g["total_viajes"]).sum()
                            / g["total_viajes"].sum()
                        )
                        if g["total_viajes"].sum() > 0
                        else 0.0,
                        "rmse": (
                            (g["rmse"] * g["total_viajes"]).sum()
                            / g["total_viajes"].sum()
                        )
                        if g["total_viajes"].sum() > 0
                        else 0.0,
                    }
                )
            )
            .reset_index()
        )
        st.subheader("Error por hora (anual)")
        chart_err_anual = (
            alt.Chart(err_agg)
            .transform_fold(["mae", "rmse"], as_=["metrica", "valor"])
            .mark_line(point=True)
            .encode(
                x=alt.X("pickup_hour:O", title="Hora", sort=list(range(24))),
                y=alt.Y("valor:Q", title="Error"),
                color=alt.Color("metrica:N", title="Metrica"),
                tooltip=[
                    alt.Tooltip("pickup_hour:O", title="Hora"),
                    alt.Tooltip("metrica:N", title="Metrica"),
                    alt.Tooltip("valor:Q", title="Valor", format=".3f"),
                ],
            )
        )
        st.altair_chart(chart_err_anual, use_container_width=True)

    top_origen_year = cargar_tabla_historico(DATA_DIR_BASE, "top_origen", year_sel)
    top_destino_year = cargar_tabla_historico(DATA_DIR_BASE, "top_destino", year_sel)
    if top_origen_year is not None and not top_origen_year.empty:
        top_origen_year["total_viajes"] = pd.to_numeric(
            top_origen_year["total_viajes"], errors="coerce"
        )
        origen_agg = (
            top_origen_year.groupby("pu_location")
            .agg(
                total_viajes=("total_viajes", "sum"),
                duracion_promedio_min=("duracion_promedio_min", "mean"),
                tarifa_promedio=("tarifa_promedio", "mean"),
            )
            .reset_index()
            .sort_values("total_viajes", ascending=False)
            .head(10)
        )
        st.subheader("Top zonas de origen (anual)")
        st.dataframe(origen_agg, use_container_width=True)

    if top_destino_year is not None and not top_destino_year.empty:
        top_destino_year["total_viajes"] = pd.to_numeric(
            top_destino_year["total_viajes"], errors="coerce"
        )
        destino_agg = (
            top_destino_year.groupby("do_location")
            .agg(
                total_viajes=("total_viajes", "sum"),
                duracion_promedio_min=("duracion_promedio_min", "mean"),
                tarifa_promedio=("tarifa_promedio", "mean"),
            )
            .reset_index()
            .sort_values("total_viajes", ascending=False)
            .head(10)
        )
        st.subheader("Top zonas de destino (anual)")
        st.dataframe(destino_agg, use_container_width=True)

if not es_anual:
    st.subheader("Resumen del periodo")
    resumen_items = []
    if kpis is not None and not kpis.empty:
        resumen_items.append(
            f"KPIs: total_viajes={int(kpis.loc[0, 'total_viajes']):,}, "
            f"duracion_promedio_min={float(kpis.loc[0, 'duracion_promedio_min']):.2f}, "
            f"tarifa_promedio=${float(kpis.loc[0, 'tarifa_promedio']):.2f}"
        )

    if viajes is not None and not viajes.empty:
        viajes_hora_res = viajes.groupby("pickup_hour")["total_viajes"].sum().reset_index()
        viajes_dia_res = viajes.groupby("pickup_dow")["total_viajes"].sum().reset_index()
        viajes_hora_res["pickup_hour"] = viajes_hora_res["pickup_hour"].astype(int)
        viajes_dia_res["pickup_dow"] = viajes_dia_res["pickup_dow"].astype(int)
        total_viajes_res = viajes_hora_res["total_viajes"].sum()
        if total_viajes_res > 0:
            hora_pico = int(
                viajes_hora_res.loc[
                    viajes_hora_res["total_viajes"].idxmax(), "pickup_hour"
                ]
            )
            dia_pico = int(
                viajes_dia_res.loc[
                    viajes_dia_res["total_viajes"].idxmax(), "pickup_dow"
                ]
            )
            viajes_fin = viajes_dia_res[
                viajes_dia_res["pickup_dow"].isin([1, 7])
            ]["total_viajes"].sum()
            viajes_noche = viajes_hora_res[
                (viajes_hora_res["pickup_hour"] >= 22)
                | (viajes_hora_res["pickup_hour"] <= 5)
            ]["total_viajes"].sum()
            resumen_items.append(
                "Viajes: hora_pico={:02d}:00, dia_pico={}, fin_de_semana={:.1f}%, nocturnos={:.1f}%".format(
                    hora_pico,
                    MAPA_DIAS.get(dia_pico, str(dia_pico)),
                    (viajes_fin / total_viajes_res) * 100,
                    (viajes_noche / total_viajes_res) * 100,
                )
            )

    if duracion is not None and tarifa is not None and not duracion.empty and not tarifa.empty:
        merge_res = duracion.merge(tarifa, on="pickup_hour", how="inner")
        merge_res["tarifa_por_min"] = (
            merge_res["tarifa_promedio"] / merge_res["duracion_promedio_min"]
        )
        hora_tarifa_max = int(
            merge_res.loc[merge_res["tarifa_por_min"].idxmax(), "pickup_hour"]
        )
        tarifa_max = float(merge_res["tarifa_por_min"].max())
        resumen_items.append(
            f"Tarifa por minuto: hora_max={hora_tarifa_max:02d}:00, valor={tarifa_max:.3f}"
        )

    if pagos is not None and not pagos.empty:
        pagos["payment_type"] = pagos["payment_type"].astype(str)
        pagos["payment_label"] = pagos["payment_type"].map(MAPA_PAGOS).fillna("Otro")
        pagos["payment_label"] = pagos["payment_type"] + " - " + pagos["payment_label"]
        total_pagos = pagos["total_viajes"].sum()
        top_pago = pagos.sort_values("total_viajes", ascending=False).iloc[0]
        resumen_items.append(
            "Pagos: top_tipo={}, participacion={:.1f}%".format(
                top_pago["payment_label"],
                (top_pago["total_viajes"] / total_pagos) * 100 if total_pagos else 0,
            )
        )

    if vendor is not None and not vendor.empty:
        vendor["vendor_id"] = vendor["vendor_id"].astype(str)
        vendor["vendor_label"] = vendor["vendor_id"].map(MAPA_VENDOR).fillna("Otro")
        vendor["vendor_label"] = vendor["vendor_id"] + " - " + vendor["vendor_label"]
        total_vendor = vendor["total_viajes"].sum()
        top_vendor = vendor.sort_values("total_viajes", ascending=False).iloc[0]
        resumen_items.append(
            "Vendor: top_vendor={}, participacion={:.1f}%".format(
                top_vendor["vendor_label"],
                (top_vendor["total_viajes"] / total_vendor) * 100 if total_vendor else 0,
            )
        )

    if distancia_bins is not None and not distancia_bins.empty:
        top_bin = distancia_bins.sort_values("total_viajes", ascending=False).iloc[0]
        resumen_items.append(
            "Distancia: rango_top={}, viajes={}, duracion_promedio={:.2f} min, tarifa_promedio=${:.2f}".format(
                top_bin["distancia_bin"],
                int(top_bin["total_viajes"]),
                float(top_bin["duracion_promedio_min"]),
                float(top_bin["tarifa_promedio"]),
            )
        )

    if variabilidad_hora is not None and not variabilidad_hora.empty:
        variabilidad_hora["pickup_hour"] = variabilidad_hora["pickup_hour"].astype(int)
        hora_var = int(
            variabilidad_hora.loc[
                variabilidad_hora["duracion_std"].idxmax(), "pickup_hour"
            ]
        )
        var_val = float(variabilidad_hora["duracion_std"].max())
        resumen_items.append(
            f"Variabilidad: hora_max_std={hora_var:02d}:00, std_duracion={var_val:.2f}"
        )

    if metricas is not None and not metricas.empty:
        met = metricas.iloc[0].to_dict()
        resumen_items.append(
            "Modelo: algoritmo={}, RMSE={:.3f}, MAE={:.3f}".format(
                met.get("algoritmo"),
                float(met.get("rmse", 0)),
                float(met.get("mae", 0)),
            )
        )

    if errores is not None and not errores.empty:
        errores["pickup_hour"] = errores["pickup_hour"].astype(int)
        hora_err = int(errores.loc[errores["mae"].idxmax(), "pickup_hour"])
        mae_max = float(errores["mae"].max())
        resumen_items.append(
            f"Error: hora_max_mae={hora_err:02d}:00, MAE_max={mae_max:.3f}"
        )

    if resumen_items:
        st.markdown("\n".join(f"- {item}" for item in resumen_items))
    else:
        st.info("No hay datos suficientes para generar el resumen.")

historico_plot = historico_year
if historico_plot is not None and not historico_plot.empty:
    st.subheader("Tendencia mensual de KPIs")
    hist_base = historico_plot[
        ["periodo", "total_viajes", "duracion_promedio_min", "tarifa_promedio"]
    ].copy()
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

if es_anual:
    st.stop()

st.markdown("---")

if viajes is not None and not viajes.empty:
    viajes_hora = (
        viajes.groupby("pickup_hour")["total_viajes"].sum().reset_index()
    )
    viajes_hora["pickup_hour"] = viajes_hora["pickup_hour"].astype(int)
    st.subheader("Viajes por hora")
    chart_viajes_hora = (
        alt.Chart(viajes_hora)
        .mark_line(point=True)
        .encode(
            x=alt.X("pickup_hour:O", title="Hora", sort=list(range(24))),
            y=alt.Y("total_viajes:Q", title="Total de viajes"),
            tooltip=[
                alt.Tooltip("pickup_hour:O", title="Hora"),
                alt.Tooltip("total_viajes:Q", title="Viajes"),
            ],
        )
    )
    st.altair_chart(chart_viajes_hora, use_container_width=True)

    viajes_dia = viajes.groupby("pickup_dow")["total_viajes"].sum().reset_index()
    viajes_dia["pickup_dow"] = viajes_dia["pickup_dow"].astype(int)
    viajes_dia["dia"] = viajes_dia["pickup_dow"].map(MAPA_DIAS)
    st.subheader("Viajes por dia de la semana")
    chart_viajes_dia = (
        alt.Chart(viajes_dia)
        .mark_bar()
        .encode(
            x=alt.X("dia:O", title="Dia", sort=ORDEN_DIAS),
            y=alt.Y("total_viajes:Q", title="Total de viajes"),
            tooltip=[
                alt.Tooltip("dia:O", title="Dia"),
                alt.Tooltip("total_viajes:Q", title="Viajes"),
            ],
        )
    )
    st.altair_chart(chart_viajes_dia, use_container_width=True)

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
        chart_participacion = (
            alt.Chart(viajes_hora)
            .mark_bar()
            .encode(
                x=alt.X("pickup_hour:O", title="Hora", sort=list(range(24))),
                y=alt.Y("participacion_pct:Q", title="Participacion (%)"),
                tooltip=[
                    alt.Tooltip("pickup_hour:O", title="Hora"),
                    alt.Tooltip("participacion_pct:Q", title="Participacion (%)", format=".2f"),
                ],
            )
        )
        st.altair_chart(chart_participacion, use_container_width=True)

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
        chart_comparativo = (
            alt.Chart(comparativo)
            .mark_bar()
            .encode(
                x=alt.X("grupo:O", title="Grupo"),
                y=alt.Y("total_viajes:Q", title="Total de viajes"),
                tooltip=[
                    alt.Tooltip("grupo:O", title="Grupo"),
                    alt.Tooltip("total_viajes:Q", title="Viajes"),
                ],
            )
        )
        st.altair_chart(chart_comparativo, use_container_width=True)

        demanda = viajes_hora.sort_values("pickup_hour").copy()
        demanda["acumulado_pct"] = demanda["total_viajes"].cumsum() / total_viajes * 100
        st.subheader("Demanda acumulada por hora (%)")
        chart_demanda = (
            alt.Chart(demanda)
            .mark_line(point=True)
            .encode(
                x=alt.X("pickup_hour:O", title="Hora", sort=list(range(24))),
                y=alt.Y("acumulado_pct:Q", title="Acumulado (%)"),
                tooltip=[
                    alt.Tooltip("pickup_hour:O", title="Hora"),
                    alt.Tooltip("acumulado_pct:Q", title="Acumulado (%)", format=".2f"),
                ],
            )
        )
        st.altair_chart(chart_demanda, use_container_width=True)

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
    duracion["pickup_hour"] = duracion["pickup_hour"].astype(int)
    chart_duracion = (
        alt.Chart(duracion)
        .mark_line(point=True)
        .encode(
            x=alt.X("pickup_hour:O", title="Hora", sort=list(range(24))),
            y=alt.Y("duracion_promedio_min:Q", title="Duracion promedio (min)"),
            tooltip=[
                alt.Tooltip("pickup_hour:O", title="Hora"),
                alt.Tooltip("duracion_promedio_min:Q", title="Duracion promedio (min)", format=".2f"),
            ],
        )
    )
    st.altair_chart(chart_duracion, use_container_width=True)

if tarifa is not None and not tarifa.empty:
    st.subheader("Tarifa promedio por hora")
    tarifa["pickup_hour"] = tarifa["pickup_hour"].astype(int)
    chart_tarifa = (
        alt.Chart(tarifa)
        .mark_line(point=True)
        .encode(
            x=alt.X("pickup_hour:O", title="Hora", sort=list(range(24))),
            y=alt.Y("tarifa_promedio:Q", title="Tarifa promedio"),
            tooltip=[
                alt.Tooltip("pickup_hour:O", title="Hora"),
                alt.Tooltip("tarifa_promedio:Q", title="Tarifa promedio", format=".2f"),
            ],
        )
    )
    st.altair_chart(chart_tarifa, use_container_width=True)

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
    chart_tarifa_min = (
        alt.Chart(tarifa_min)
        .mark_line(point=True)
        .encode(
            x=alt.X("pickup_hour:O", title="Hora", sort=list(range(24))),
            y=alt.Y("tarifa_por_min:Q", title="Tarifa por minuto"),
            tooltip=[
                alt.Tooltip("pickup_hour:O", title="Hora"),
                alt.Tooltip("tarifa_por_min:Q", title="Tarifa por minuto", format=".3f"),
            ],
        )
    )
    st.altair_chart(chart_tarifa_min, use_container_width=True)

    top_tarifa = tarifa_min.sort_values("tarifa_por_min", ascending=False).head(5)
    st.subheader("Top horas con tarifa por minuto mas alta")
    st.dataframe(
        top_tarifa[["pickup_hour", "tarifa_por_min"]], use_container_width=True
    )

if metricas is not None and not metricas.empty:
    st.markdown("---")
    st.subheader("Resumen del modelo")
    met = metricas.iloc[0].to_dict()
    col_m1, col_m2 = st.columns(2)
    col_m1.metric("RMSE", f"{float(met.get('rmse', 0)):.3f}")
    col_m2.metric("MAE", f"{float(met.get('mae', 0)):.3f}")
    st.write(
        {
            "algoritmo": met.get("algoritmo"),
            "rows_train": met.get("rows_train"),
            "rows_test": met.get("rows_test"),
            "fecha_entrenamiento": met.get("fecha_entrenamiento"),
        }
    )

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

if top_origen is not None and top_destino is not None:
    st.markdown("---")
    st.subheader("Top zonas de origen y destino")
    col_o, col_d = st.columns(2)
    col_o.dataframe(top_origen, use_container_width=True)
    col_d.dataframe(top_destino, use_container_width=True)

if pagos is not None and not pagos.empty:
    st.subheader("Distribucion por tipo de pago")
    pagos["payment_type"] = pagos["payment_type"].astype(str)
    pagos["payment_label"] = pagos["payment_type"].map(MAPA_PAGOS).fillna("Otro")
    pagos["payment_label"] = pagos["payment_type"] + " - " + pagos["payment_label"]
    chart_pagos = (
        alt.Chart(pagos)
        .mark_bar()
        .encode(
            x=alt.X("payment_label:O", title="Tipo de pago", sort="-y"),
            y=alt.Y("total_viajes:Q", title="Total de viajes"),
            color=alt.Color("payment_label:O", title="Leyenda"),
            tooltip=[
                alt.Tooltip("payment_label:O", title="Tipo de pago"),
                alt.Tooltip("total_viajes:Q", title="Viajes"),
                alt.Tooltip("duracion_promedio_min:Q", title="Duracion promedio", format=".2f"),
                alt.Tooltip("tarifa_promedio:Q", title="Tarifa promedio", format=".2f"),
            ],
        )
    )
    st.altair_chart(chart_pagos, use_container_width=True)

if vendor is not None and not vendor.empty:
    st.subheader("Distribucion por vendor")
    vendor["vendor_id"] = vendor["vendor_id"].astype(str)
    vendor["vendor_label"] = vendor["vendor_id"].map(MAPA_VENDOR).fillna("Otro")
    vendor["vendor_label"] = vendor["vendor_id"] + " - " + vendor["vendor_label"]
    chart_vendor = (
        alt.Chart(vendor)
        .mark_bar()
        .encode(
            x=alt.X("vendor_label:O", title="Vendor", sort="-y"),
            y=alt.Y("total_viajes:Q", title="Total de viajes"),
            color=alt.Color("vendor_label:O", title="Leyenda"),
            tooltip=[
                alt.Tooltip("vendor_label:O", title="Vendor"),
                alt.Tooltip("total_viajes:Q", title="Viajes"),
                alt.Tooltip("duracion_promedio_min:Q", title="Duracion promedio", format=".2f"),
                alt.Tooltip("tarifa_promedio:Q", title="Tarifa promedio", format=".2f"),
            ],
        )
    )
    st.altair_chart(chart_vendor, use_container_width=True)

if distancia_bins is not None and not distancia_bins.empty:
    st.subheader("Duracion y tarifa promedio por distancia")
    distancia_bins = distancia_bins.sort_values("distancia_orden")
    chart_distancia = (
        alt.Chart(distancia_bins)
        .transform_fold(
            ["duracion_promedio_min", "tarifa_promedio"],
            as_=["metrica", "valor"],
        )
        .mark_line(point=True)
        .encode(
            x=alt.X("distancia_bin:O", title="Rango de distancia"),
            y=alt.Y("valor:Q", title="Valor"),
            color=alt.Color("metrica:N", title="Leyenda"),
            tooltip=[
                alt.Tooltip("distancia_bin:O", title="Rango"),
                alt.Tooltip("metrica:N", title="Metrica"),
                alt.Tooltip("valor:Q", title="Valor", format=".2f"),
            ],
        )
    )
    st.altair_chart(chart_distancia, use_container_width=True)

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
