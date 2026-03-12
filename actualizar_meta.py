"""
actualizar_meta.py
------------------
Script de actualización diaria: descarga los últimos 8 días de Meta Ads
y hace upsert en Google Sheets (no borra el histórico).

Autenticación: Google Service Account (compatible con GitHub Actions)

Uso local:
    python actualizar_meta.py

Requisitos:
    pip install gspread google-auth facebook-business pandas
"""

import json
import os
import time
from datetime import datetime, timedelta

import gspread
import pandas as pd
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.api import FacebookAdsApi
from google.oauth2.service_account import Credentials

# ============================================================
# CONFIGURACIÓN — edita solo esta sección
# ============================================================
META_ACCESS_TOKEN = os.environ.get("META_ACCESS_TOKEN", "")
META_APP_SECRET   = "51a7369b56ff58ac9d90723fe3dd19a2"
META_APP_ID       = "794814423158870"
AD_ACCOUNT_ID     = "act_122669098066867"
SHEET_ID          = "1evv-YemzQfKFUr4mZyLEqne2ALqPD6v8rzFUlp68fcE"

# Días hacia atrás a descargar (8 para cubrir ventana de atribución de 7 días)
DIAS_ATRAS = 8

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
# ============================================================


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


# ── Autenticación ────────────────────────────────────────────

def conectar_sheets():
    """
    Autenticación via Service Account.
    Lee las credenciales desde la variable de entorno GOOGLE_SERVICE_ACCOUNT
    (GitHub Actions) o desde el archivo service_account.json (local).
    """
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT")

    if sa_json:
        # GitHub Actions: credenciales en variable de entorno
        info = json.loads(sa_json)
    else:
        # Local: credenciales en archivo
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
              "spend", "reach", "ctr", "cpc", "actions"]
    params = {
        "level": "adset",
        "time_range": {"since": desde, "until": hasta},
        "time_increment": 1,
    }
    rows = []
    for i in account.get_insights(fields=fields, params=params):
        conv, _ = parsear_conversiones(i)
        rows.append({
            "fecha":        i.get("date_start"),
            "campaña":      i.get("campaign_name"),
            "adset":        i.get("adset_name"),
            "impresiones":  i.get("impressions", 0),
            "clics":        i.get("clicks", 0),
            "gasto":        i.get("spend", 0),
            "alcance":      i.get("reach", 0),
            "ctr":          i.get("ctr", 0),
            "cpc":          i.get("cpc", 0),
            "conversiones": conv,
        })
    return pd.DataFrame(rows)


def get_creative_performance(account, desde, hasta):
    fields = ["ad_name", "adset_name", "campaign_name", "impressions",
              "clicks", "spend", "ctr", "cpc", "actions"]
    params = {
        "level": "ad",
        "time_range": {"since": desde, "until": hasta},
        "time_increment": 1,
    }
    rows = []
    for i in account.get_insights(fields=fields, params=params):
        conv, _ = parsear_conversiones(i)
        rows.append({
            "fecha":        i.get("date_start"),
            "anuncio":      i.get("ad_name"),
            "adset":        i.get("adset_name"),
            "campaña":      i.get("campaign_name"),
            "impresiones":  i.get("impressions", 0),
            "clics":        i.get("clicks", 0),
            "gasto":        i.get("spend", 0),
            "ctr":          i.get("ctr", 0),
            "cpc":          i.get("cpc", 0),
            "conversiones": conv,
        })
    return pd.DataFrame(rows)


def get_breakdown(account, breakdown, desde, hasta, nivel="adset"):
    fields = ["campaign_name", "adset_name", "impressions", "clicks",
              "spend", "reach", "ctr", "cpc", "actions", "action_values"]
    params = {
        "level": nivel,
        "time_range": {"since": desde, "until": hasta},
        "time_increment": 1,
        "breakdowns": breakdown,
    }
    rows = []
    for i in account.get_insights(fields=fields, params=params):
        conv, valor = parsear_conversiones(i)
        row = {
            "fecha":              i.get("date_start"),
            "campaña":            i.get("campaign_name"),
            "adset":              i.get("adset_name"),
            "impresiones":        i.get("impressions", 0),
            "clics":              i.get("clicks", 0),
            "gasto":              i.get("spend", 0),
            "alcance":            i.get("reach", 0),
            "ctr":                i.get("ctr", 0),
            "cpc":                i.get("cpc", 0),
            "conversiones":       conv,
            "valor_conversiones": valor,
            "mercado":            extraer_mercado(i.get("campaign_name")),
            "tipo_campaña":       extraer_tipo(i.get("campaign_name")),
        }
        if breakdown == ["age", "gender"]:
            row["edad"]       = i.get("age")
            row["genero"]     = i.get("gender")
        elif breakdown == ["publisher_platform", "platform_position"]:
            row["plataforma"] = i.get("publisher_platform")
            row["placement"]  = i.get("platform_position")
        elif breakdown == ["impression_device"]:
            row["dispositivo"] = i.get("impression_device")
        elif breakdown == ["country"]:
            row["pais"] = i.get("country")
        rows.append(row)
    return pd.DataFrame(rows)


# ── Clasificación ────────────────────────────────────────────

def extraer_mercado(nombre):
    if pd.isna(nombre) or nombre is None:
        return "Desconocido"
    n = str(nombre).upper()
    if n.startswith("ES_"): return "España"
    if n.startswith("DE_"): return "Alemania"
    if n.startswith("FR_"): return "Francia"
    return "Otro"


def extraer_tipo(nombre):
    if pd.isna(nombre) or nombre is None:
        return "Desconocido"
    n = str(nombre).upper()
    if "PROSPECTING" in n: return "Prospecting"
    if "REMARKETING" in n: return "Remarketing"
    return "Otro"


# ── Upsert en Google Sheets ──────────────────────────────────

def upsert_sheet(sheet, df, nombre_pestaña, claves):
    for intento in range(3):
        try:
            ws = sheet.worksheet(nombre_pestaña)

            for col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col])
                except (ValueError, TypeError):
                    pass

            existentes = ws.get_all_records()

            if not existentes:
                ws.update([df.columns.tolist()] + df.fillna("").values.tolist())
                log(f"  ✅ '{nombre_pestaña}': {len(df)} filas escritas (primera vez)")
                return

            df_exist  = pd.DataFrame(existentes)
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

    log(f"🚀 Actualizando datos del {desde} al {hasta}")
    log("─" * 50)

    log("🔑 Conectando a Google Sheets...")
    gc    = conectar_sheets()
    sheet = gc.open_by_key(SHEET_ID)
    log("✅ Google Sheets conectado")

    log("🔑 Conectando a Meta API...")
    account = conectar_meta()
    log("✅ Meta API conectada")
    log("─" * 50)

    # 1. Campaign metrics
    log("📥 Descargando campaign_metrics...")
    df_campaigns = get_campaign_metrics(account, desde, hasta)
    log(f"   {len(df_campaigns)} filas descargadas")
    upsert_sheet(sheet, df_campaigns, "campaign_metrics",
                 claves=["fecha", "campaña", "adset"])
    time.sleep(2)

    # 2. Creative performance
    log("📥 Descargando creative_performance...")
    df_creatives = get_creative_performance(account, desde, hasta)
    log(f"   {len(df_creatives)} filas descargadas")
    upsert_sheet(sheet, df_creatives, "creative_performance",
                 claves=["fecha", "campaña", "anuncio"])
    time.sleep(2)

    # 3. Breakdowns
    breakdowns = [
        ("demographics", ["age", "gender"],                           ["fecha", "campaña", "edad", "genero"]),
        ("platforms",    ["publisher_platform", "platform_position"], ["fecha", "campaña", "plataforma", "placement"]),
        ("devices",      ["impression_device"],                       ["fecha", "campaña", "dispositivo"]),
        ("countries",    ["country"],                                 ["fecha", "campaña", "pais"]),
    ]

    for nombre, breakdown, claves in breakdowns:
        log(f"📥 Descargando {nombre}...")
        df_bd = get_breakdown(account, breakdown, desde, hasta)
        log(f"   {len(df_bd)} filas descargadas")
        upsert_sheet(sheet, df_bd, nombre, claves=claves)
        time.sleep(2)

    log("─" * 50)
    log("🎉 Actualización completada.")


if __name__ == "__main__":
    main()
