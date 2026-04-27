import json
import os
import time
import re
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

DIAS_ATRAS = 481
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

# ── Utilidades de limpieza y tipos ───────────────────────────

def limpiar_num(val, default=0):
    """Asegura que el valor sea un número, eliminando ruidos de formato."""
    if val is None or val == "":
        return default
    try:
        if isinstance(val, str):
            # Primero reemplazamos coma decimal por punto (por si acaso)
            # y luego eliminamos cualquier carácter que no sea dígito, punto o menos
            val = val.strip().replace(',', '.')
            val = re.sub(r'[^\d.-]', '', val)
        return float(val)
    except:
        return default

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
    # ── FIX: añadimos campaign_id y adset_id para deduplicar por ID estable ──
    fields = [
        "campaign_id", "campaign_name",
        "adset_id", "adset_name",
        "impressions", "clicks", "spend", "reach", "ctr", "cpc",
        "actions", "action_values"
    ]
    params_base = {"level": "adset", "time_increment": 1}
    all_rows = []
    for chunk_desde, chunk_hasta in chunked_date_ranges(desde, hasta, CHUNK_DAYS):
        params = {**params_base, "time_range": {"since": chunk_desde, "until": chunk_hasta}}
        items = get_insights_con_retry(account, fields, params, label=f"camp_{chunk_desde}")
        for i in items:
            conv, valor = parsear_conversiones(i)
            all_rows.append({
                "fecha":              i.get("date_start"),
                # IDs estables (no cambian al renombrar)
                "campaign_id":        str(i.get("campaign_id", "")),
                "adset_id":           str(i.get("adset_id", "")),
                # Nombres (pueden cambiar, se actualizan automáticamente)
                "campaña":            i.get("campaign_name"),
                "adset":              i.get("adset_name"),
                "impresiones":        int(limpiar_num(i.get("impressions"))),
                "clics":              int(limpiar_num(i.get("clicks"))),
                "gasto":              limpiar_num(i.get("spend")),
                "alcance":            int(limpiar_num(i.get("reach"))),
                "ctr":                limpiar_num(i.get("ctr")),
                "cpc":                limpiar_num(i.get("cpc")),
                "conversiones":       int(limpiar_num(conv)),
                "valor_conversiones": limpiar_num(valor),
            })
    df = pd.DataFrame(all_rows)

    # ── FIX: deduplicar dentro de la propia respuesta de la API ──────────────
    # Meta puede devolver la misma combinación fecha+adset_id bajo distintos
    # nombres de campaña si hubo un renombre. Nos quedamos con el último
    # (nombre más reciente), ordenando por fecha desc para que keep="last"
    # coincida con la fila más reciente del chunk.
    if not df.empty and "adset_id" in df.columns:
        antes = len(df)
        df = (df
              .sort_values("fecha", ascending=True)
              .drop_duplicates(subset=["fecha", "adset_id"], keep="last")
              .reset_index(drop=True))
        eliminados = antes - len(df)
        if eliminados:
            log(f"  🔁 Deduplicados {eliminados} registros duplicados en respuesta de API (renombre de campaña)")

    return df

def get_creative_performance(account, desde, hasta):
    fields = [
        "ad_id", "ad_name",
        "adset_name", "campaign_name",
        "impressions", "clicks", "spend", "ctr", "cpc",
        "actions", "action_values"
    ]
    params_base = {"level": "ad", "time_increment": 1}
    all_rows = []
    for chunk_desde, chunk_hasta in chunked_date_ranges(desde, hasta, CHUNK_DAYS):
        params = {**params_base, "time_range": {"since": chunk_desde, "until": chunk_hasta}}
        items = get_insights_con_retry(account, fields, params, label=f"crea_{chunk_desde}")
        for i in items:
            conv, valor = parsear_conversiones(i)
            all_rows.append({
                "fecha":              i.get("date_start"),
                "ad_id":              str(i.get("ad_id", "")),
                "anuncio":            i.get("ad_name"),
                "adset":              i.get("adset_name"),
                "campaña":            i.get("campaign_name"),
                "impresiones":        int(limpiar_num(i.get("impressions"))),
                "clics":              int(limpiar_num(i.get("clicks"))),
                "gasto":              limpiar_num(i.get("spend")),
                "ctr":                limpiar_num(i.get("ctr")),
                "cpc":                limpiar_num(i.get("cpc")),
                "conversiones":       int(limpiar_num(conv)),
                "valor_conversiones": limpiar_num(valor),
            })
    df = pd.DataFrame(all_rows)

    # Deduplicar por ad_id estable
    if not df.empty and "ad_id" in df.columns:
        antes = len(df)
        df = (df
              .sort_values("fecha", ascending=True)
              .drop_duplicates(subset=["fecha", "ad_id"], keep="last")
              .reset_index(drop=True))
        eliminados = antes - len(df)
        if eliminados:
            log(f"  🔁 creative_performance: {eliminados} duplicados eliminados por ad_id")

    return df

def get_breakdown(account, breakdown, desde, hasta, nivel="adset"):
    fields = [
        "campaign_name", "adset_name",
        "impressions", "clicks", "spend", "reach", "ctr", "cpc",
        "actions", "action_values"
    ]
    params_base = {"level": nivel, "time_increment": 1, "breakdowns": breakdown}
    all_rows = []
    for chunk_desde, chunk_hasta in chunked_date_ranges(desde, hasta, CHUNK_DAYS):
        params = {**params_base, "time_range": {"since": chunk_desde, "until": chunk_hasta}}
        items = get_insights_con_retry(account, fields, params, label=f"break_{breakdown[0]}")
        for i in items:
            conv, valor = parsear_conversiones(i)
            row = {
                "fecha":              i.get("date_start"),
                "campaña":            i.get("campaign_name"),
                "adset":              i.get("adset_name"),
                "impresiones":        int(limpiar_num(i.get("impressions"))),
                "clics":              int(limpiar_num(i.get("clicks"))),
                "gasto":              limpiar_num(i.get("spend")),
                "alcance":            int(limpiar_num(i.get("reach"))),
                "ctr":                limpiar_num(i.get("ctr")),
                "cpc":                limpiar_num(i.get("cpc")),
                "conversiones":       int(limpiar_num(conv)),
                "valor_conversiones": limpiar_num(valor),
                "mercado":            extraer_mercado(i.get("campaign_name")),
                "tipo_campaña":       extraer_tipo(i.get("campaign_name")),
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

# ── Upsert en Google Sheets ──────────────────────────────────

def upsert_sheet(sheet, df, nombre_pestaña, claves):
    if df.empty:
        log(f"  ⚠️ '{nombre_pestaña}': DataFrame vacío.")
        return

    cols_float  = ["gasto", "valor_conversiones", "ctr", "cpc"]
    cols_int    = ["impresiones", "clics", "alcance", "conversiones"]
    # IDs siempre como string para evitar problemas de tipo
    cols_str_id = ["campaign_id", "adset_id", "ad_id"]

    def normalizar_df(frame, es_existente=False):
        """Normaliza tipos en un DataFrame (nuevo o leído de Sheets)."""
        for col in frame.columns:
            if col in cols_float:
                if es_existente:
                    # Sheets puede devolver "79,59" en locale europeo
                    frame[col] = frame[col].astype(str).str.replace(',', '.', regex=False)
                frame[col] = pd.to_numeric(frame[col], errors='coerce').fillna(0.0).astype(float)
            elif col in cols_int:
                if es_existente:
                    frame[col] = frame[col].astype(str).str.replace(',', '.', regex=False)
                frame[col] = pd.to_numeric(frame[col], errors='coerce').fillna(0).astype(int)
            elif col in claves or col in cols_str_id:
                frame[col] = frame[col].astype(str).str.strip()
        return frame

    try:
        ws = sheet.worksheet(nombre_pestaña)
        existentes = ws.get_all_records()

        # Normalizar df nuevo
        df = normalizar_df(df.copy(), es_existente=False)

        if existentes:
            df_exist = pd.DataFrame(existentes)
            df_exist = normalizar_df(df_exist, es_existente=True)

            # ── FIX: detectar si la hoja existente tiene las columnas clave ──
            claves_ausentes = [k for k in claves if k not in df_exist.columns]
            if claves_ausentes:
                log(f"  ⚠️ '{nombre_pestaña}': columnas clave ausentes en hoja ({claves_ausentes}). "
                    f"Reescribiendo con nuevo esquema...")
                # Primer run post-fix: descartamos datos viejos y reescribimos
                df_merged = df
            else:
                # Aseguramos que df_exist tenga las mismas columnas que df
                # (añadimos las que falten con valor vacío)
                for col in df.columns:
                    if col not in df_exist.columns:
                        df_exist[col] = "" if col not in cols_float + cols_int else 0

                # Merge: el dato nuevo pisa al viejo (misma clave = mismo adset+fecha)
                df_merged = (
                    pd.concat([df_exist, df], ignore_index=True)
                    .drop_duplicates(subset=claves, keep="last")
                )
        else:
            df_merged = df

        df_merged = df_merged.sort_values(by=claves[0], ascending=False)

        # Rellenar NaNs antes de subir
        for col in df_merged.columns:
            if col in cols_float:
                df_merged[col] = df_merged[col].fillna(0.0)
            elif col in cols_int:
                df_merged[col] = df_merged[col].fillna(0)
            else:
                df_merged[col] = df_merged[col].fillna("")

        ws.clear()

        # RAW envía floats como numberValue, evitando que el locale europeo
        # interprete el punto como separador de miles
        matriz_final = [df_merged.columns.tolist()] + [
            [v.item() if hasattr(v, 'item') else v for v in row]
            for row in df_merged.itertuples(index=False, name=None)
        ]

        ws.update(matriz_final, value_input_option='RAW')
        log(f"  ✅ '{nombre_pestaña}': {len(df_merged)} filas totales actualizadas.")

    except Exception as e:
        log(f"  ⚠️ Fallo crítico en '{nombre_pestaña}': {e}")

# ── Main ─────────────────────────────────────────────────────

def main():
    ayer  = datetime.today() - timedelta(days=1)
    desde = (ayer - timedelta(days=DIAS_ATRAS - 1)).strftime("%Y-%m-%d")
    hasta = ayer.strftime("%Y-%m-%d")

    log(f"🚀 Iniciando del {desde} al {hasta}")
    gc      = conectar_sheets()
    sheet   = gc.open_by_key(SHEET_ID)
    account = conectar_meta()

    # ── campaign_metrics ────────────────────────────────────
    # CLAVE: ahora usamos IDs estables en lugar de nombres de campaña/adset
    log("📥 Descargando campaign_metrics...")
    df_campaigns = get_campaign_metrics(account, desde, hasta)
    upsert_sheet(sheet, df_campaigns, "campaign_metrics",
                 claves=["fecha", "campaign_id", "adset_id"])

    # ── creative_performance ────────────────────────────────
    log("📥 Descargando creative_performance...")
    df_creatives = get_creative_performance(account, desde, hasta)
    upsert_sheet(sheet, df_creatives, "creative_performance",
                 claves=["fecha", "ad_id"])

    # ── breakdowns ──────────────────────────────────────────
    breakdowns = [
        ("demographics", ["age", "gender"],                              ["fecha", "campaña", "edad", "genero"]),
        ("platforms",    ["publisher_platform", "platform_position"],    ["fecha", "campaña", "plataforma", "placement"]),
        ("devices",      ["impression_device"],                          ["fecha", "campaña", "dispositivo"]),
        ("countries",    ["country"],                                    ["fecha", "campaña", "pais"]),
    ]

    for nombre, bdown, claves in breakdowns:
        log(f"📥 Descargando {nombre}...")
        df_bd = get_breakdown(account, bdown, desde, hasta)
        upsert_sheet(sheet, df_bd, nombre, claves=claves)

    log("🎉 Actualización completada.")

if __name__ == "__main__":
    main()
