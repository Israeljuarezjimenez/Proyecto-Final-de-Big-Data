#!/usr/bin/env bash
set -euo pipefail

mostrar_uso() {
  echo "Uso: $0 --year YYYY --month MM | --months MM,MM,MM | --quarter Q [--local-dir data/raw] [--hdfs-raw-root /data/tlc/raw] [--hdfs-uri hdfs://namenode:8020] [--skip-missing]"
}

YEAR=""
MONTH=""
MONTHS=""
QUARTER=""
LOCAL_DIR="data/raw"
HDFS_RAW_ROOT="/data/tlc/raw"
HDFS_URI="hdfs://namenode:8020"
HDFS_BIN=""
SKIP_MISSING="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --year)
      YEAR="$2"
      shift 2
      ;;
    --month)
      MONTH="$2"
      shift 2
      ;;
    --months)
      MONTHS="$2"
      shift 2
      ;;
    --quarter)
      QUARTER="$2"
      shift 2
      ;;
    --local-dir)
      LOCAL_DIR="$2"
      shift 2
      ;;
    --hdfs-raw-root)
      HDFS_RAW_ROOT="$2"
      shift 2
      ;;
    --hdfs-uri)
      HDFS_URI="$2"
      shift 2
      ;;
    --skip-missing)
      SKIP_MISSING="true"
      shift 1
      ;;
    -h|--help)
      mostrar_uso
      exit 0
      ;;
    *)
      echo "Parametro desconocido: $1"
      mostrar_uso
      exit 1
      ;;
  esac
 done

if [[ -z "$YEAR" ]]; then
  mostrar_uso
  exit 1
fi

opciones=0
[[ -n "$MONTH" ]] && opciones=$((opciones+1))
[[ -n "$MONTHS" ]] && opciones=$((opciones+1))
[[ -n "$QUARTER" ]] && opciones=$((opciones+1))
if [[ "$opciones" -ne 1 ]]; then
  mostrar_uso
  exit 1
fi

if [[ -n "$QUARTER" ]]; then
  case "$QUARTER" in
    1) MONTHS="01,02,03" ;;
    2) MONTHS="04,05,06" ;;
    3) MONTHS="07,08,09" ;;
    4) MONTHS="10,11,12" ;;
    *)
      echo "Trimestre fuera de rango (1-4)"
      exit 1
      ;;
  esac
fi

if [[ -n "$MONTH" ]]; then
  MONTHS="$MONTH"
fi

HDFS_BIN=$(command -v hdfs || true)
if [[ -z "$HDFS_BIN" && -x "/opt/hadoop-3.2.1/bin/hdfs" ]]; then
  HDFS_BIN="/opt/hadoop-3.2.1/bin/hdfs"
fi
if [[ -z "$HDFS_BIN" && -x "/opt/hadoop/bin/hdfs" ]]; then
  HDFS_BIN="/opt/hadoop/bin/hdfs"
fi
if [[ -z "$HDFS_BIN" ]]; then
  echo "No se encontro el binario hdfs"
  exit 1
fi

IFS=',' read -ra MESES_ARR <<< "$MONTHS"
for MES_ITEM in "${MESES_ARR[@]}"; do
  MES_NUM=$((10#$MES_ITEM))
  MES=$(printf "%02d" "$MES_NUM")
  ARCHIVO_LOCAL="${LOCAL_DIR}/yellow_tripdata_${YEAR}-${MES}.parquet"
  if [[ ! -f "$ARCHIVO_LOCAL" ]]; then
    if [[ "$SKIP_MISSING" == "true" ]]; then
      echo "No se encontro el archivo, se omite: $ARCHIVO_LOCAL"
      continue
    fi
    echo "No se encontro el archivo: $ARCHIVO_LOCAL"
    exit 1
  fi

  DESTINO_HDFS="${HDFS_RAW_ROOT}/year=${YEAR}/month=${MES}"

  echo "Creando ruta en HDFS: $DESTINO_HDFS"
  $HDFS_BIN dfs -fs "$HDFS_URI" -mkdir -p "$DESTINO_HDFS"

  echo "Subiendo archivo a HDFS"
  $HDFS_BIN dfs -fs "$HDFS_URI" -put -f "$ARCHIVO_LOCAL" "$DESTINO_HDFS/"

  $HDFS_BIN dfs -fs "$HDFS_URI" -ls "$DESTINO_HDFS"
done
