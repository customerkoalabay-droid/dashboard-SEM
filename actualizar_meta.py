"""
actualizar_meta.py
------------------
Script de actualización diaria: descarga los últimos N días de Meta Ads
y hace upsert en Google Sheets (no borra el histórico).
Corregido: Normalización de decimales y tipos numéricos.
"""

import json
import os
import time
from datetime import datetime, timedelta

import gspread
import pandas as pd
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi
from facebook_business.exceptions import FacebookRequestError
from google.oauth2.service_account import Credentials

# ============================================================
# CONFIGURACIÓN
# ============================================================
META_ACCESS_TOKEN = os.environ.get("META_ACCESS_TOKEN", "")
META_APP_SECRET   = "51a7369b56ff58ac9d90723fe3dd19a2"
META_APP_ID       = "794814423158870"
AD_ACCOUNT_ID     = "act_122669098066867"
SHEET_ID          = "1evv-YemzQfKFUr4mZyLEqne2ALqPD6v8rzFUlp68fcE"

DIAS_ATRAS = 90  
CHUNK_DAYS = 15
PAUSA_ENTRE_CHUNKS = 5

RETRY_WAIT_INICIAL = 60
RETRY_MAX_INTENTOS = 4

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

CODIGOS_REINTENTABLES = {2, 4, 17, 32, 613}

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

# ── Utilidades de fechas ─────────────────────────────────────

def chunked_date_ranges(desde, hasta, chunk_days=15):
    inicio = datetime.strptime(desde, "%Y-%m-%d")
    fin    = datetime.strptime(hasta, "%Y-%m-%d")
    ranges = []
    cursor = inicio
    while cursor <= fin:
        tramo_fin = min(cursor + timedelta(days=chunk_days - 1), fin)
        ranges.append((cursor.strftime("%Y-%m-%d"), tramo_fin.strftime("%Y-%m-%d")))
        cursor = tramo_fin + timedelta(days=1)
    return ranges

# ── Retry con Meta API ───────────────────────────────────────

def get_insights_con_retry(account, fields, params, label=""):
    espera = RETRY_WAIT_INICIAL
    for intento in range(1, RETRY_MAX_INTENTOS + 1):
        try:
            rows = []
            cursor = account.get_insights(fields=fields, params=params)
            for item in cursor:
                rows.append(item)
            return rows
        except FacebookRequestError as e:
            codigo = e.api_error_code()
            debe_reintentar = codigo in CODIGOS_REINTENTABLES or e.api_transient_error()
            if debe_reintentar and intento < RETRY_MAX_INTENTOS:
                log(f"   ⏳ Error Meta {codigo} [{label}]. Reintento {intento}...")
                time.sleep(espera)
                espera = min(espera * 2, 600)
            else:
                log(f"   ⚠️ Chunk saltado [{label}] - Error {codigo}: {e.api_error_message()}")
                return []
        except Exception as e:
            log(f"   ⚠️ Error inesperado [{label}]: {e}")
            return []

# ── Conexiones ───────────────────────────────────────────────

def conectar_sheets():
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT")
    if sa_json:
        info = json.loads(sa_json)
    else:
        with open("service_account.json", "r") as f:
            info = json.load(f)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)

def conectar_meta():
    FacebookAdsApi.init(META_APP_ID, META_APP_SECRET, META_ACCESS_TOKEN)
    return AdAccount(AD_ACCOUNT_ID)

# ── Extracción y Procesamiento ──────────────────────────────

def parsear_conversiones(insight):
    conversiones, valor = 0, 0
    for action in insight.get("actions", []):
        if action["action_type"] in ("purchase", "omni_purchase"):
            conversiones = action["value"]
    for action in insight.get("action_values", []):
        if action["action_type"] in ("purchase", "omni_purchase"):
            valor = action["value"]
    return conversiones, valor

def get_campaign_metrics(account, desde, hasta):
    fields = ["campaign_name", "adset_name", "impressions", "clicks", "spend", "reach", "ctr", "cpc", "actions", "action_values"]
    params_base = {"level": "adset", "time_increment": 1}
    all_rows = []
    for chunk_desde, chunk_hasta in chunked_date_ranges(desde, hasta, CHUNK_DAYS):
        params = {**params_base, "time_range": {"since": chunk_desde, "until": chunk_hasta}}
        items = get_insights_con_retry(account, fields, params, label=f"camp_{chunk_desde}")
        for i in items:
            conv, valor = parsear_conversiones(i)
            all_rows.append({
                "fecha": i.get("date_start"),
                "campaña": i.get("campaign_name"),
                "adset": i.get("adset_name"),
                "impresiones": int(i.get("impressions", 0)),
                "clics": int(i.get("clicks", 0)),
                "gasto": float(i.get("spend") or 0),
                "alcance": int(i.get("reach", 0)),
                "ctr": float(i.get("ctr") or 0),
                "cpc": float(i.get("cpc") or 0),
                "conversiones": conv,
                "valor_conversiones": float(valor or 0),
            })
    return pd.DataFrame(all_rows)

def get_creative_performance(account, desde, hasta):
    fields = ["ad_name", "adset_name", "campaign_name", "impressions", "clicks", "spend", "ctr", "cpc", "actions", "action_values"]
    params_base = {"level": "ad", "time_increment": 1}
    all_rows = []
    for chunk_desde, chunk_hasta in chunked_date_ranges(desde, hasta, CHUNK_DAYS):
        params = {**params_base, "time_range": {"since": chunk_desde, "until": chunk_hasta}}
        items = get_insights_con_retry(account, fields, params, label=f"crea_{chunk_desde}")
        for i in items:
            conv, valor = parsear_conversiones(i)
            all_rows.append({
                "fecha": i.get("date_start"),
                "anuncio": i.get("ad_name"),
                "adset": i.get("adset_name"),
                "campaña": i.get("campaign_name"),
                "impresiones": int(i.get("impressions", 0)),
                "clics": int(i.get("clicks", 0)),
                "gasto": float(i.get("spend") or 0),
                "ctr": float(i.get("ctr") or 0),
                "cpc": float(i.get("cpc") or 0),
                "conversiones": conv,
                "valor_conversiones": float(valor or 0),
            })
    return pd.DataFrame(all_rows)

def get_breakdown(account, breakdown, desde, hasta, nivel="adset"):
    fields = ["campaign_name", "adset_name", "impressions", "clicks", "spend", "reach", "ctr", "cpc", "actions", "action_values"]
    params_base = {"level": nivel, "time_increment": 1, "breakdowns": breakdown}
    all_rows = []
    for chunk_desde, chunk_hasta in chunked_date_ranges(desde, hasta, CHUNK_DAYS):
        params = {**params_base, "time_range": {"since": chunk_desde, "until": chunk_hasta}}
        items = get_insights_con_retry(account, fields, params, label=f"break_{breakdown[0]}")
        for i in items:
            conv, valor = parsear_conversiones(i)
            row = {
                "fecha": i.get("date_start"),
                "campaña": i.get("campaign_name"),
                "adset": i.get("adset_name"),
                "impresiones": int(i.get("impressions", 0)),
                "clics": int(i.get("clicks", 0)),
                "gasto": float(i.get("spend") or 0),
                "alcance": int(i.get("reach", 0)),
                "ctr": float(i.get("ctr") or 0),
                "cpc": float(i.get("cpc") or 0),
                "conversiones": conv,
                "valor_conversiones": float(valor or 0),
                "mercado": extraer_mercado(i.get("campaign_name")),
                "tipo_campaña": extraer_tipo(i.get("campaign_name")),
            }
            if breakdown == ["age", "gender"]:
                row["edad"], row["genero"] = i.get("age"), i.get("gender")
            elif breakdown == ["publisher_platform", "platform_position"]:
                row["plataforma"], row["placement"] = i.get("publisher_platform"), i.get("platform_position")
            elif breakdown == ["impression_device"]:
                row["dispositivo"] = i.get("impression_device")
            elif breakdown == ["country"]:
                row["pais"] = i.get("country")
            all_rows.append(row)
    return pd.DataFrame(all_rows)

def extraer_mercado(nombre):
    n = str(nombre or "").upper()
    if n.startswith("ES_"): return "España"
    if n.startswith("DE_"): return "Alemania"
    if n.startswith("FR_"): return "Francia"
    return "Otro"

def extraer_tipo(nombre):
    n = str(nombre or "").upper()
    if "PROSPECTING" in n: return "Prospecting"
    if "REMARKETING" in n: return "Remarketing"
    return "Otro"

# ── Upsert en Google Sheets (NORMALIZADO) ───────────────────

def upsert_sheet(sheet, df, nombre_pestaña, claves):
    if df.empty:
        log(f"  ⚠️ '{nombre_pestaña}': DataFrame vacío.")
        return

    # Normalizar tipos en el nuevo DataFrame
    cols_float = ["gasto", "valor_conversiones", "ctr", "cpc"]
    cols_int = ["impresiones", "clics", "alcance", "conversiones"]

    for col in df.columns:
        if col in cols_float:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(float)
        elif col in cols_int:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    for intento in range(3):
        try:
            ws = sheet.worksheet(nombre_pestaña)
            existentes = ws.get_all_records()

            if not existentes:
                ws.update([df.columns.tolist()] + df.fillna("").values.tolist(), value_input_option='USER_ENTERED')
                log(f"  ✅ '{nombre_pestaña}': {len(df)} filas escritas.")
                return

            df_exist = pd.DataFrame(existentes)

            # Normalizar tipos en el DataFrame existente para comparación justa
            for col in df.columns:
                if col in df_exist.columns:
                    if col in cols_float:
                        df_exist[col] = pd.to_numeric(df_exist[col], errors='coerce').fillna(0).astype(float)
                    elif col in cols_int:
                        df_exist[col] = pd.to_numeric(df_exist[col], errors='coerce').fillna(0).astype(int)

            # Normalizar claves como strings
            for clave in claves:
                if clave in df_exist.columns: df_exist[clave] = df_exist[clave].astype(str).str.strip()
                if clave in df.columns: df[clave] = df[clave].astype(str).str.strip()

            # Merge y eliminación de duplicados (mantenemos el último dato descargado)
            df_merged = pd.concat([df_exist, df], ignore_index=True).drop_duplicates(subset=claves, keep="last")
            df_merged = df_merged.sort_values(by=claves[0], ascending=False)

            ws.clear()
            # value_input_option='USER_ENTERED' es CRÍTICO para los decimales
            ws.update([df_merged.columns.tolist()] + df_merged.fillna("").values.tolist(), value_input_option='USER_ENTERED')
            log(f"  ✅ '{nombre_pestaña}': {len(df_merged)} filas totales.")
            return

        except Exception as e:
            log(f"  ⚠️ Intento {intento+1} fallido en '{nombre_pestaña}': {e}")
            time.sleep(5)

# ── Main ─────────────────────────────────────────────────────

def main():
    ayer  = datetime.today() - timedelta(days=1)
    desde = (ayer - timedelta(days=DIAS_ATRAS - 1)).strftime("%Y-%m-%d")
    hasta = ayer.strftime("%Y-%m-%d")

    log(f"🚀 Iniciando del {desde} al {hasta}")
    gc = conectar_sheets()
    sheet = gc.open_by_key(SHEET_ID)
    account = conectar_meta()

    # Ejecución de secciones
    log("📥 Descargando campaign_metrics...")
    df_campaigns = get_campaign_metrics(account, desde, hasta)
    upsert_sheet(sheet, df_campaigns, "campaign_metrics", claves=["fecha", "campaña", "adset"])

    log("📥 Descargando creative_performance...")
    df_creatives = get_creative_performance(account, desde, hasta)
    upsert_sheet(sheet, df_creatives, "creative_performance", claves=["fecha", "campaña", "anuncio"])

    breakdowns = [
        ("demographics", ["age", "gender"], ["fecha", "campaña", "edad", "genero"]),
        ("platforms", ["publisher_platform", "platform_position"], ["fecha", "campaña", "plataforma", "placement"]),
        ("devices", ["impression_device"], ["fecha", "campaña", "dispositivo"]),
        ("countries", ["country"], ["fecha", "campaña", "pais"]),
    ]

    for nombre, bdown, claves in breakdowns:
        log(f"📥 Descargando {nombre}...")
        df_bd = get_breakdown(account, bdown, desde, hasta)
        upsert_sheet(sheet, df_bd, nombre, claves=claves)

    log("🎉 Actualización completada.")

if __name__ == "__main__":
    main()
