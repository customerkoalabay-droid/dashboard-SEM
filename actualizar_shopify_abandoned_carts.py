"""
actualizar_shopify_abandonados.py
----------------------------------
Descarga checkouts abandonados de Shopify y los vuelca a Google Sheets.
Convierte todos los importes a EUR usando tipos de cambio en tiempo real
(exchangerate-api.com — plan gratuito, sin tarjeta).

Requisitos:
    pip install gspread google-auth pandas requests

Variables de entorno necesarias:
    GOOGLE_SERVICE_ACCOUNT   → JSON de la service account
    SHOPIFY_STORE            → subdominio (ej: "koalabay")
    SHOPIFY_API_KEY          → API key de Shopify
    SHOPIFY_API_SECRET       → API secret de Shopify
    EXCHANGERATE_API_KEY     → clave de exchangerate-api.com (plan gratuito)
"""

import json
import os
import re
import time
from datetime import datetime, timedelta

import gspread
import pandas as pd
import requests
from google.oauth2.service_account import Credentials

# ============================================================
# CONFIGURACIÓN
# ============================================================
SHEET_ID   = "1j84VyucNRrRx7haLKm16ppRuaL28p5BaX75D4BHaqWs"
PESTANA    = "shopify_abandoned_checkouts"
DIAS_ATRAS = 130

SHOPIFY_STORE  = os.environ.get("SHOPIFY_STORE", "koalabay")
SHOPIFY_KEY    = os.environ.get("SHOPIFY_API_KEY")
SHOPIFY_SECRET = os.environ.get("SHOPIFY_API_SECRET")

# Obtén tu clave gratuita en https://www.exchangerate-api.com/
EXCHANGERATE_API_KEY = os.environ.get("EXCHANGERATE_API_KEY", "")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
# ============================================================


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


# ── Tipos de cambio ──────────────────────────────────────────

def obtener_tipos_cambio():
    """
    Descarga los tipos de cambio actuales con base EUR desde exchangerate-api.com.
    Devuelve un dict {moneda: tasa_a_eur}, ej: {"USD": 0.92, "GBP": 1.17, ...}
    Si falla, devuelve {} y los importes se dejarán sin convertir.
    """
    if not EXCHANGERATE_API_KEY:
        log("⚠️  EXCHANGERATE_API_KEY no configurada — importes NO se convertirán a EUR.")
        return {}

    try:
        url  = f"https://v6.exchangerate-api.com/v6/{EXCHANGERATE_API_KEY}/latest/EUR"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        if data.get("result") != "success":
            log(f"⚠️  exchangerate-api error: {data.get('error-type', 'desconocido')}")
            return {}

        # Las tasas vienen como "cuántas unidades de X por 1 EUR"
        # Para convertir X → EUR: importe_eur = importe_x / tasa_x
        tasas = data["conversion_rates"]
        log(f"✅ Tipos de cambio obtenidos ({len(tasas)} monedas, base EUR, "
            f"actualizado: {data.get('time_last_update_utc', '?')})")
        return tasas

    except Exception as e:
        log(f"⚠️  No se pudieron obtener tipos de cambio: {e}")
        return {}


def convertir_a_eur(importe, moneda, tasas):
    """
    Convierte un importe en `moneda` a EUR.
    Si la moneda ya es EUR o no hay tasa disponible, devuelve el importe original.
    """
    if not moneda or moneda.upper() == "EUR" or not tasas:
        return importe, False

    tasa = tasas.get(moneda.upper())
    if tasa and tasa > 0:
        return round(importe / tasa, 4), True

    log(f"   ⚠️  Moneda '{moneda}' sin tasa de cambio — importe original conservado.")
    return importe, False


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


def obtener_access_token():
    url  = f"https://{SHOPIFY_STORE}.myshopify.com/admin/oauth/access_token"
    resp = requests.post(url, data={
        "grant_type":    "client_credentials",
        "client_id":     SHOPIFY_KEY,
        "client_secret": SHOPIFY_SECRET,
    }, timeout=30)
    resp.raise_for_status()
    token = resp.json().get("access_token")
    log("✅ Token Shopify obtenido")
    return token


def shopify_get(endpoint, params=None, token=None):
    url = f"https://{SHOPIFY_STORE}.myshopify.com/admin/api/2024-01/{endpoint}"
    resp = requests.get(
        url,
        headers={"X-Shopify-Access-Token": token},
        params=params or {},
        timeout=30,
    )
    resp.raise_for_status()
    return resp


# ── Obtención de checkouts abandonados ──────────────────────

def obtener_abandonados(fecha_inicio, fecha_fin, token):
    todos = []
    dt_inicio = datetime.strptime(fecha_inicio[:10], "%Y-%m-%d")
    dt_fin    = datetime.strptime(fecha_fin[:10],    "%Y-%m-%d")

    cursor = dt_inicio
    while cursor <= dt_fin:
        if cursor.month < 12:
            mes_fin = datetime(cursor.year, cursor.month + 1, 1) - timedelta(days=1)
        else:
            mes_fin = datetime(cursor.year + 1, 1, 1) - timedelta(days=1)
        mes_fin = min(mes_fin, dt_fin)

        rango_inicio = cursor.strftime("%Y-%m-%dT00:00:00Z")
        rango_fin    = mes_fin.strftime("%Y-%m-%dT23:59:59Z")
        log(f"   📅 Mes: {rango_inicio[:7]}...")

        params = {
            "status":         "abandoned",
            "created_at_min": rango_inicio,
            "created_at_max": rango_fin,
            "limit":          250,
            # Sin fields -> Shopify devuelve el JSON completo en todas las paginas
        }

        page = 1
        while True:
            log(f"      Página {page}...")
            resp = shopify_get("checkouts.json", params, token=token)
            data = resp.json().get("checkouts", [])
            todos.extend(data)

            link_header = resp.headers.get("Link", "")
            if 'rel="next"' not in link_header:
                break

            match = re.search(r'<[^>]*page_info=([^&>]+)[^>]*>;\s*rel="next"', link_header)
            if not match:
                break
            # Sin fields -> JSON completo tambien en paginas siguientes
            params = {
                "limit":     250,
                "page_info": match.group(1),
            }
            page  += 1
            time.sleep(0.5)

        if cursor.month == 12:
            cursor = datetime(cursor.year + 1, 1, 1)
        else:
            cursor = datetime(cursor.year, cursor.month + 1, 1)
        time.sleep(1)

    return todos


# ── Parseo de checkouts ──────────────────────────────────────

def extraer_moneda(ck):
    """
    Extrae el código de moneda del checkout.
    El campo 'currency' puede venir como dict {'currency': 'USD'} o como string 'USD'.
    """
    raw = ck.get("currency") or ck.get("presentment_currency") or {}
    if isinstance(raw, dict):
        return raw.get("currency", "EUR").upper()
    if isinstance(raw, str):
        return raw.upper()
    return "EUR"


def parsear_checkout(ck, tasas):
    shipping   = ck.get("shipping_address") or ck.get("billing_address") or {}
    line_items = ck.get("line_items", [])
    discounts  = ck.get("discount_codes", [])

    productos = " | ".join([
        f"{li.get('title', '?')} x{li.get('quantity', 1)}"
        for li in line_items
    ])
    skus = " | ".join([
        str(li.get("sku", ""))
        for li in line_items if li.get("sku")
    ])
    codigos = ", ".join([
        d.get("code", "") or (d.get("discount_code") or {}).get("code", "")
        for d in discounts
    ])

    moneda = extraer_moneda(ck)

    importe_total_orig  = float(ck.get("total_price",      0) or 0)
    subtotal_orig       = float(ck.get("subtotal_price",   0) or 0)
    descuentos_orig     = float(ck.get("total_discounts",  0) or 0)

    importe_total_eur, convertido = convertir_a_eur(importe_total_orig, moneda, tasas)
    subtotal_eur,      _          = convertir_a_eur(subtotal_orig,      moneda, tasas)
    descuentos_eur,    _          = convertir_a_eur(descuentos_orig,    moneda, tasas)

    return {
        "fecha":                   ck.get("created_at", "")[:10],
        "checkout_id":             str(ck.get("id")),
        "token":                   ck.get("token"),
        "email":                   ck.get("email", ""),
        # Moneda original
        "moneda":                  moneda,
        "importe_total_orig":      importe_total_orig,
        "subtotal_orig":           subtotal_orig,
        "total_discounts_orig":    descuentos_orig,
        # Importes convertidos a EUR
        "importe_total_eur":       importe_total_eur,
        "subtotal_eur":            subtotal_eur,
        "total_discounts_eur":     descuentos_eur,
        "convertido_a_eur":        "Sí" if convertido else "No (ya EUR o sin tasa)",
        # Resto de campos
        "codigos_descuento":       codigos,
        "url_recuperacion":        ck.get("abandoned_checkout_url", ""),
        "pais":                    shipping.get("country", ""),
        "ciudad":                  shipping.get("city", ""),
        "zip":                     shipping.get("zip", ""),
        "num_productos":           sum(li.get("quantity", 1) for li in line_items),
        "productos":               productos,
        "skus":                    skus,
        "referring_site":          ck.get("referring_site", "") or "",
        "source_name":             ck.get("source_name", ""),
        "recuperado":              "Sí" if ck.get("completed_at") else "No",
    }


# ── Upsert en Google Sheets ──────────────────────────────────

def upsert_sheet(sheet, df, nombre_pestana, claves):
    log(f"   Preparando upsert en '{nombre_pestana}'...")
    claves_validas = [c for c in claves if c in df.columns]

    if not claves_validas:
        log(f"  ⚠️  Ninguna clave válida encontrada en df — abortando upsert.")
        return

    for intento in range(3):
        try:
            ws            = sheet.worksheet(nombre_pestana)
            valores_exist = ws.get_all_values()

            # Hoja vacía → escribir directo
            if not valores_exist or len(valores_exist) < 2:
                ws.clear()
                _write_in_chunks(ws, [df.columns.tolist()] + df.fillna("").values.tolist())
                log(f"  ✅ '{nombre_pestana}': {len(df)} filas escritas (primera vez).")
                return

            h_exist    = valores_exist[0]
            df_exist   = pd.DataFrame(valores_exist[1:], columns=h_exist)

            # Normalizar clave a string en ambos df para comparar bien
            for c in claves_validas:
                if c in df_exist.columns:
                    df_exist[c] = df_exist[c].astype(str).str.strip()
                if c in df.columns:
                    df[c] = df[c].astype(str).str.strip()

            claves_merge = [c for c in claves_validas if c in df_exist.columns]

            n_antes   = len(df_exist)
            df_merged = (
                pd.concat([df_exist, df], ignore_index=True)
                  .drop_duplicates(subset=claves_merge, keep="last")
                  .sort_values(by="fecha", ascending=False)
            )

            ws.clear()
            _write_in_chunks(ws, [df_merged.columns.tolist()] + df_merged.fillna("").values.tolist())

            nuevas = max(len(df_merged) - n_antes, 0)
            log(f"  ✅ '{nombre_pestana}': {len(df_merged)} filas totales "
                f"({nuevas} nuevas / {len(df)} procesadas).")
            return

        except Exception as e:
            log(f"  ⚠️ Intento {intento+1} fallido: {e}")
            if intento < 2:
                time.sleep(5)
            else:
                log(f"  ❌ No se pudo actualizar '{nombre_pestana}' tras 3 intentos.")


def _write_in_chunks(ws, data, chunk_rows=5000):
    if not data:
        return
    headers = data[0]
    rows    = data[1:]
    ws.update(range_name="A1", values=[headers], value_input_option="USER_ENTERED")
    for start in range(0, len(rows), chunk_rows):
        chunk     = rows[start:start + chunk_rows]
        row_start = start + 2
        ws.update(range_name=f"A{row_start}", values=chunk, value_input_option="USER_ENTERED")
        time.sleep(1)


# ── Main ─────────────────────────────────────────────────────

def main():
    log("🚀 Iniciando extracción de carritos abandonados Shopify")
    log("─" * 50)

    # 1. Tipos de cambio (una sola llamada al inicio)
    log("💱 Obteniendo tipos de cambio...")
    tasas = obtener_tipos_cambio()

    # 2. Token Shopify
    token = obtener_access_token()

    # 3. Rango de fechas
    fecha_fin    = datetime.utcnow()
    fecha_inicio = fecha_fin - timedelta(days=DIAS_ATRAS)
    log(f"📅 Rango: {fecha_inicio.strftime('%Y-%m-%d')} → {fecha_fin.strftime('%Y-%m-%d')}")

    # 4. Descarga
    checkouts = obtener_abandonados(
        fecha_inicio.strftime("%Y-%m-%dT00:00:00Z"),
        fecha_fin.strftime("%Y-%m-%dT23:59:59Z"),
        token,
    )

    if not checkouts:
        log("⚠️  No se encontraron checkouts abandonados.")
        return

    log(f"📦 {len(checkouts)} checkouts descargados. Parseando...")

    # 5. Parseo con conversión de moneda
    filas = [parsear_checkout(c, tasas) for c in checkouts]
    df    = pd.DataFrame(filas)

    monedas_encontradas = df["moneda"].value_counts().to_dict()
    log(f"💱 Monedas encontradas: {monedas_encontradas}")

    # 6. Volcar a Sheets
    log("🔑 Conectando a Sheets...")
    gc    = conectar_sheets()
    sheet = gc.open_by_key(SHEET_ID)

    upsert_sheet(sheet, df, PESTANA, claves=["checkout_id"])

    log("─" * 50)
    log("🎉 Proceso finalizado.")


if __name__ == "__main__":
    main()
