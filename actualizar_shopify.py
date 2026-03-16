"""
actualizar_shopify.py
---------------------
Lee los pedidos de Shopify via API y los vuelca al dashboard.

Hoja destino:
  - SHEET_ID definido abajo → pestaña: shopify_orders

Uso:
    python actualizar_shopify.py

Requisitos:
    pip install gspread google-auth pandas requests
"""

import json
import os
import re
import time
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs

import gspread
import pandas as pd
import requests
from google.oauth2.service_account import Credentials

# ============================================================
# CONFIGURACIÓN
# ============================================================
SHEET_ID      = "1j84VyucNRrRx7haLKm16ppRuaL28p5BaX75D4BHaqWs"  # Dashboard Shopify
PESTANA       = "shopify_orders"
DIAS_ATRAS    = 440  # ventana de actualización diaria

SHOPIFY_STORE  = os.environ.get("SHOPIFY_STORE", "koalabay")
SHOPIFY_KEY    = os.environ.get("SHOPIFY_API_KEY")
SHOPIFY_SECRET = os.environ.get("SHOPIFY_API_SECRET")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
# ============================================================


def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


# ── Autenticación Google Sheets ──────────────────────────────

def conectar_sheets():
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT")
    if sa_json:
        info = json.loads(sa_json)
    else:
        with open("service_account.json", "r") as f:
            info = json.load(f)
    creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


# ── Shopify API ──────────────────────────────────────────────

def obtener_access_token():
    """Obtiene un access token de Shopify via OAuth client credentials."""
    url = f"https://{SHOPIFY_STORE}.myshopify.com/admin/oauth/access_token"
    resp = requests.post(url, data={
        "grant_type":    "client_credentials",
        "client_id":     SHOPIFY_KEY,
        "client_secret": SHOPIFY_SECRET,
    }, timeout=30)
    resp.raise_for_status()
    token = resp.json().get("access_token")
    log(f"✅ Token Shopify obtenido")
    return token


def shopify_get(endpoint, params=None, token=None):
    """Hace una petición GET a la API de Shopify con access token."""
    url = f"https://{SHOPIFY_STORE}.myshopify.com/admin/api/2024-01/{endpoint}"
    response = requests.get(
        url,
        headers={"X-Shopify-Access-Token": token},
        params=params or {},
        timeout=30,
    )
    response.raise_for_status()
    return response


def obtener_pedidos(fecha_inicio, fecha_fin, token):
    """Obtiene todos los pedidos en el rango de fechas dado, iterando por meses."""
    todos_pedidos = []

    # Convertir a datetime para iterar por meses
    dt_inicio = datetime.strptime(fecha_inicio[:10], "%Y-%m-%d")
    dt_fin    = datetime.strptime(fecha_fin[:10], "%Y-%m-%d")

    # Iterar mes a mes para evitar límites de Shopify en rangos largos
    cursor = dt_inicio
    while cursor <= dt_fin:
        mes_fin = min(
            datetime(cursor.year, cursor.month + 1 if cursor.month < 12 else 1,
                     1, tzinfo=None) - timedelta(days=1)
            if cursor.month < 12
            else datetime(cursor.year + 1, 1, 1) - timedelta(days=1),
            dt_fin
        )

        rango_inicio = cursor.strftime("%Y-%m-%dT00:00:00Z")
        rango_fin    = mes_fin.strftime("%Y-%m-%dT23:59:59Z")
        log(f"   📅 Mes: {rango_inicio[:7]}...")

        params = {
            "status":         "any",
            "created_at_min": rango_inicio,
            "created_at_max": rango_fin,
            "limit":          250,
            "fields": (
                "id,created_at,financial_status,fulfillment_status,"
                "total_price,subtotal_price,total_discounts,currency,"
                "billing_address,shipping_address,"
                "landing_site,referring_site,source_name,"
                "line_items,tags"
            ),
        }

        page = 1
        while True:
            log(f"      Página {page}...")
            resp = shopify_get("orders.json", params, token=token)
            data = resp.json().get("orders", [])
            todos_pedidos.extend(data)

            link_header = resp.headers.get("Link", "")
            if 'rel="next"' not in link_header:
                break

            match = re.search(r'<[^>]*page_info=([^&>]+)[^>]*>;\s*rel="next"', link_header)
            if not match:
                break
            params = {"limit": 250, "page_info": match.group(1)}
            page += 1
            time.sleep(0.5)

        # Avanzar al mes siguiente
        if cursor.month == 12:
            cursor = datetime(cursor.year + 1, 1, 1)
        else:
            cursor = datetime(cursor.year, cursor.month + 1, 1)
        time.sleep(1)

    return todos_pedidos


# ── Parseo de pedidos ────────────────────────────────────────

def extraer_utm(landing_site):
    """Extrae UTM params de la landing_site URL."""
    if not landing_site:
        return {}, None
    try:
        parsed = urlparse(landing_site if landing_site.startswith("http") else f"https://x.com{landing_site}")
        qs = parse_qs(parsed.query)
        utms = {
            "utm_source":   qs.get("utm_source",   [None])[0],
            "utm_medium":   qs.get("utm_medium",   [None])[0],
            "utm_campaign": qs.get("utm_campaign", [None])[0],
            "utm_content":  qs.get("utm_content",  [None])[0],
            "utm_term":     qs.get("utm_term",     [None])[0],
        }
        # Extraer mercado/idioma de la subcarpeta (/de-de/, /es-es/, etc.)
        match = re.search(r"/([a-z]{2}-[a-z]{2})/", parsed.path)
        idioma = match.group(1) if match else None
        return utms, idioma
    except Exception:
        return {}, None


def extraer_mercado(idioma, pais):
    """Infiere el mercado a partir del idioma o país."""
    if idioma:
        prefijo = idioma.split("-")[1].upper() if "-" in idioma else idioma.upper()
        if prefijo == "ES": return "España"
        if prefijo == "DE": return "Alemania"
        if prefijo == "FR": return "Francia"
        if prefijo == "EU": return "Europa"
    if pais:
        p = pais.upper()
        if p in ["ES", "SPAIN", "ESPAÑA"]:           return "España"
        if p in ["DE", "GERMANY", "ALEMANIA"]:        return "Alemania"
        if p in ["FR", "FRANCE", "FRANCIA"]:          return "Francia"
    return "Otro"


def parsear_pedido(pedido):
    """Convierte un pedido de Shopify en una fila para el dashboard."""
    landing    = pedido.get("landing_site", "")
    utms, idioma = extraer_utm(landing)

    shipping = pedido.get("shipping_address") or pedido.get("billing_address") or {}
    pais_codigo = shipping.get("country_code", "")
    pais_nombre = shipping.get("country", "")
    ciudad      = shipping.get("city", "")

    mercado = extraer_mercado(idioma, pais_codigo or pais_nombre)

    # Productos del pedido (resumen)
    line_items  = pedido.get("line_items", [])
    productos   = " | ".join([f"{li.get('name','?')} x{li.get('quantity',1)}" for li in line_items[:3]])
    num_items   = sum(li.get("quantity", 1) for li in line_items)

    return {
        "fecha":              pedido.get("created_at", "")[:10],
        "pedido_id":          pedido.get("id"),
        "estado_pago":        pedido.get("financial_status", ""),
        "estado_envio":       pedido.get("fulfillment_status", "") or "unfulfilled",
        "importe_total":      float(pedido.get("total_price", 0) or 0),
        "subtotal":           float(pedido.get("subtotal_price", 0) or 0),
        "descuentos":         float(pedido.get("total_discounts", 0) or 0),
        "moneda":             pedido.get("currency", ""),
        "pais":               pais_nombre,
        "pais_codigo":        pais_codigo,
        "ciudad":             ciudad,
        "idioma":             idioma or "",
        "mercado":            mercado,
        "utm_source":         utms.get("utm_source") or pedido.get("source_name", "") or "",
        "utm_medium":         utms.get("utm_medium") or "",
        "utm_campaign":       utms.get("utm_campaign") or "",
        "utm_content":        utms.get("utm_content") or "",
        "utm_term":           utms.get("utm_term") or "",
        "referring_site":     pedido.get("referring_site", "") or "",
        "num_productos":      num_items,
        "productos":          productos,
        "tags":               pedido.get("tags", "") or "",
    }


# ── Upsert en Google Sheets ──────────────────────────────────

def upsert_sheet(sheet, df, nombre_pestana, claves):
    log(f"   Columnas en df ({len(df.columns)}): {list(df.columns)}")
    claves_validas = [c for c in claves if c in df.columns]

    for intento in range(3):
        try:
            ws = sheet.worksheet(nombre_pestana)
            valores_exist = ws.get_all_values()

            if not valores_exist or len(valores_exist) < 2 or not claves_validas:
                ws.clear()
                _write_in_chunks(ws, [df.columns.tolist()] + df.fillna("").values.tolist())
                log(f"  ✅ '{nombre_pestana}': {len(df)} filas escritas")
                return

            h_exist     = valores_exist[0]
            idx_validos = [i for i, h in enumerate(h_exist) if h.strip() != ""]
            h_limpios   = [h_exist[i] for i in idx_validos]
            filas_exist = [[fila[i] if i < len(fila) else "" for i in idx_validos] for fila in valores_exist[1:]]
            filas_exist = [f for f in filas_exist if any(v.strip() != "" for v in f)]
            df_exist    = pd.DataFrame(filas_exist, columns=h_limpios)

            claves_merge = [c for c in claves_validas if c in df_exist.columns]
            if not claves_merge:
                ws.clear()
                _write_in_chunks(ws, [df.columns.tolist()] + df.fillna("").values.tolist())
                log(f"  ✅ '{nombre_pestana}': {len(df)} filas escritas (reescritura completa)")
                return

            df_merged = (
                pd.concat([df_exist, df], ignore_index=True)
                  .drop_duplicates(subset=claves_merge, keep="last")
                  .sort_values(by=claves_merge[0], ascending=False)
            )

            ws.clear()
            _write_in_chunks(ws, [df_merged.columns.tolist()] + df_merged.fillna("").values.tolist())

            nuevas = len(df_merged) - len(df_exist)
            log(f"  ✅ '{nombre_pestana}': {len(df_merged)} filas totales "
                f"({max(nuevas, 0)} nuevas / {len(df)} procesadas)")
            return

        except Exception as e:
            import traceback
            log(f"  ⚠️  Intento {intento+1} fallido en '{nombre_pestana}': {e}")
            log(f"      {traceback.format_exc().splitlines()[-1]}")
            if intento < 2:
                log("  🔄 Reintentando en 5s...")
                time.sleep(5)
            else:
                log(f"  ❌ No se pudo actualizar '{nombre_pestana}' tras 3 intentos")


def _write_in_chunks(ws, data, chunk_rows=5000):
    if not data:
        return
    headers = data[0]
    rows    = data[1:]
    total_filas_necesarias = len(rows) + 1

    filas_actuales = ws.row_count
    if filas_actuales < total_filas_necesarias:
        ws.add_rows(total_filas_necesarias - filas_actuales + 1000)
        time.sleep(1)

    ws.update(range_name="A1", values=[headers], value_input_option="USER_ENTERED")
    for start in range(0, len(rows), chunk_rows):
        chunk     = rows[start:start + chunk_rows]
        row_start = start + 2
        ws.update(range_name=f"A{row_start}", values=chunk, value_input_option="USER_ENTERED")
        time.sleep(1)


# ── Main ─────────────────────────────────────────────────────

def main():
    log("🚀 Iniciando sincronización Shopify → Dashboard")
    log("─" * 50)

    # Rango de fechas
    fecha_fin   = datetime.utcnow()
    fecha_inicio = fecha_fin - timedelta(days=DIAS_ATRAS)
    fecha_inicio_str = fecha_inicio.strftime("%Y-%m-%dT00:00:00Z")
    fecha_fin_str    = fecha_fin.strftime("%Y-%m-%dT23:59:59Z")
    log(f"📅 Rango: {fecha_inicio_str[:10]} → {fecha_fin_str[:10]}")
    log("─" * 50)

    # Conectar Sheets
    log("🔑 Conectando a Google Sheets...")
    gc    = conectar_sheets()
    sheet = gc.open_by_key(SHEET_ID)
    log("✅ Conectado")
    log("─" * 50)

    # Obtener token Shopify
    log("🔑 Autenticando con Shopify...")
    token = obtener_access_token()
    log("─" * 50)

    # Obtener pedidos
    log("📥 Obteniendo pedidos de Shopify...")
    pedidos = obtener_pedidos(fecha_inicio_str, fecha_fin_str, token)
    log(f"   {len(pedidos)} pedidos obtenidos")

    if not pedidos:
        log("⚠️  No hay pedidos en el rango. Finalizando.")
        return

    # Parsear pedidos
    filas = [parsear_pedido(p) for p in pedidos]
    df    = pd.DataFrame(filas)
    log(f"   {len(df)} filas procesadas")
    log("─" * 50)

    # Volcar al dashboard
    upsert_sheet(sheet, df, PESTANA, ["pedido_id"])

    log("─" * 50)
    log("🎉 Sincronización Shopify completada.")


if __name__ == "__main__":
    main()
