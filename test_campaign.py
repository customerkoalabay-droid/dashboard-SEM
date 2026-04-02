"""
test_campaign_metrics.py
------------------------
Script de prueba aislado SOLAMENTE para campaign_metrics.
Incluye la corrección del parseo de números para evitar errores
con los decimales en la configuración regional de España.
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

DIAS_ATRAS = 450
CHUNK_DAYS = 15
PAUSA_ENTRE_CHUNKS = 5
RETRY_WAIT_INICIAL = 60
RETRY_MAX_INTENTOS = 4

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
# ============================================================

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


# ── Retry ────────────────────────────────────────────────────

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
                log(f"   ⏳ Error Meta código {codigo}{' [' + label + ']' if label else ''}. "
                    f"Esperando {espera}s (intento {intento}/{RETRY_MAX_INTENTOS - 1})...")
                time.sleep(espera)
                espera = min(espera * 2, 600)
            else:
                log(f"   ⚠️  Chunk saltado{' [' + label + ']' if label else ''} "
                    f"tras {intento} intento(s) — código {codigo}: {e.api_error_message()}")
                return []

        except Exception as e:
            log(f"   ⚠️  Error inesperado{' [' + label + ']' if label else ''}: {e}")
            return []


# ── Autenticación ────────────────────────────────────────────

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


# ── Extracción de datos ──────────────────────────────────────

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
    fields = ["campaign_name", "adset_name", "impressions", "clicks",
              "spend", "reach", "ctr", "cpc", "actions", "action_values"]
    params_base = {"level": "adset", "time_increment": 1}
    all_rows = []
    
    for chunk_desde, chunk_hasta in chunked_date_ranges(desde, hasta, CHUNK_DAYS):
        log(f"   → Chunk {chunk_desde} / {chunk_hasta}")
        params = {**params_base, "time_range": {"since": chunk_desde, "until": chunk_hasta}}
        items = get_insights_con_retry(account, fields, params, label=f"campaign_metrics {chunk_desde}")
        
        for i in items:
            conv, valor = parsear_conversiones(i)
            all_rows.append({
                "fecha":               i.get("date_start"),
                "campaña":             i.get("campaign_name"),
                "adset":               i.get("adset_name"),
                "impresiones":         i.get("impressions", 0),
                "clics":               i.get("clicks", 0),
                "gasto":               float(i.get("spend") or 0),
                "alcance":             i.get("reach", 0),
                "ctr":                 float(i.get("ctr")   or 0),
                "cpc":                 float(i.get("cpc")   or 0),
                "conversiones":        conv,
                "valor_conversiones":  float(valor or 0),
            })
        time.sleep(PAUSA_ENTRE_CHUNKS)
        
    return pd.DataFrame(all_rows)


# ── Upsert en Google Sheets (CON LA CORRECCIÓN APLICADA) ──────

def upsert_sheet(sheet, df, nombre_pestaña, claves):
    if df.empty:
        log(f"  ⚠️  '{nombre_pestaña}': DataFrame vacío, se omite escritura.")
        return

    # Convertir columnas numéricas del df nuevo
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except (ValueError, TypeError):
            pass

    for intento in range(3):
        try:
            ws = sheet.worksheet(nombre_pestaña)
            existentes = ws.get_all_records()

            if not existentes:
                ws.update([df.columns.tolist()] + df.fillna("").values.tolist())
                log(f"  ✅ '{nombre_pestaña}': {len(df)} filas escritas (primera vez)")
                return

            df_exist = pd.DataFrame(existentes)

            # Normalizar tipos del df existente para que coincidan con df nuevo
            # 🛠️ AQUÍ ESTÁ LA CORRECCIÓN APLICADA
            for col in df.columns:
                if col in df_exist.columns and col not in claves:
                    try:
                        if pd.api.types.is_numeric_dtype(df[col]):
                            
                            def limpiar_numeros(x):
                                if isinstance(x, str):
                                    # Solo manipula strings, respeta si ya es float/int
                                    return x.replace(".", "").replace(",", ".").strip()
                                return x
                            
                            df_exist[col] = pd.to_numeric(
                                df_exist[col].apply(limpiar_numeros), 
                                errors="coerce"
                            ).fillna(0)
                    except Exception:
                        pass

            # Normalizar claves como strings en ambos
            for clave in claves:
                if clave in df_exist.columns:
                    df_exist[clave] = df_exist[clave].astype(str).str.strip()
                if clave in df.columns:
                    df[clave] = df[clave].astype(str).str.strip()

            df_merged = (
                pd.concat([df_exist, df], ignore_index=True)
                .drop_duplicates(subset=claves, keep="last")
                .sort_values(by=claves[0], ascending=False)
            )

            ws.clear()
            ws.update([df_merged.columns.tolist()] + df_merged.fillna("").values.tolist())

            nuevas = len(df_merged) - len(df_exist)
            log(f"  ✅ '{nombre_pestaña}': {len(df_merged)} filas totales "
                f"({max(nuevas, 0)} nuevas / {len(df)} descargadas)")
            return

        except Exception as e:
            log(f"  ⚠️  Intento {intento+1} fallido en '{nombre_pestaña}': {e}")
            if intento < 2:
                log("  🔄 Reintentando en 5s...")
                time.sleep(5)
            else:
                log(f"  ❌ No se pudo actualizar '{nombre_pestaña}' tras 3 intentos")


# ── Main ─────────────────────────────────────────────────────

def main():
    ayer  = datetime.today() - timedelta(days=1)
    desde = (ayer - timedelta(days=DIAS_ATRAS - 1)).strftime("%Y-%m-%d")
    hasta = ayer.strftime("%Y-%m-%d")

    total_chunks = len(chunked_date_ranges(desde, hasta, CHUNK_DAYS))
    log(f"🚀 PRUEBA AISLADA: Actualizando del {desde} al {hasta} "
        f"({total_chunks} chunks de {CHUNK_DAYS} días)")
    log("─" * 50)

    log("🔑 Conectando a Google Sheets...")
    gc    = conectar_sheets()
    sheet = gc.open_by_key(SHEET_ID)
    log("✅ Google Sheets conectado")

    log("🔑 Conectando a Meta API...")
    account = conectar_meta()
    log("✅ Meta API conectada")
    log("─" * 50)

    # Solo Campaign metrics
    log("📥 Descargando campaign_metrics...")
    df_campaigns = get_campaign_metrics(account, desde, hasta)
    log(f"   {len(df_campaigns)} filas descargadas")
    
    upsert_sheet(sheet, df_campaigns, "campaign_metrics",
                 claves=["fecha", "campaña", "adset"])

    log("─" * 50)
    log("🎉 Prueba de campaign_metrics completada.")


if __name__ == "__main__":
    main()
