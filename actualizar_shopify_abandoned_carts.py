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
SHEET_ID      = "1j84VyucNRrRx7haLKm16ppRuaL28p5BaX75D4BHaqWs"
PESTANA       = "shopify_abandoned_checkouts"  # Asegúrate de crear esta pestaña
DIAS_ATRAS    = 440 

SHOPIFY_STORE  = os.environ.get("SHOPIFY_STORE", "koalabay")
SHOPIFY_KEY    = os.environ.get("SHOPIFY_API_KEY")
SHOPIFY_SECRET = os.environ.get("SHOPIFY_API_SECRET")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

# ── Autenticación ──────────────────────────────────────────

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
    url = f"https://{SHOPIFY_STORE}.myshopify.com/admin/api/2024-01/{endpoint}"
    response = requests.get(
        url,
        headers={"X-Shopify-Access-Token": token},
        params=params or {},
        timeout=30,
    )
    response.raise_for_status()
    return response

# ── Obtención de Checkouts Abandonados ──────────────────────

def obtener_abandonados(fecha_inicio, fecha_fin, token):
    todos_checkouts = []
    dt_inicio = datetime.strptime(fecha_inicio[:10], "%Y-%m-%d")
    dt_fin    = datetime.strptime(fecha_fin[:10], "%Y-%m-%d")

    cursor = dt_inicio
    while cursor <= dt_fin:
        mes_fin = min(
            datetime(cursor.year, cursor.month + 1 if cursor.month < 12 else 1, 1) - timedelta(days=1)
            if cursor.month < 12 else datetime(cursor.year + 1, 1, 1) - timedelta(days=1),
            dt_fin
        )

        rango_inicio = cursor.strftime("%Y-%m-%dT00:00:00Z")
        rango_fin    = mes_fin.strftime("%Y-%m-%dT23:59:59Z")
        log(f"   📅 Mes: {rango_inicio[:7]}...")

        params = {
            "status":         "abandoned",
            "created_at_min": rango_inicio,
            "created_at_max": rango_fin,
            "limit":          250,
            "fields": ("id,token,created_at,total_price,subtotal_price,total_discounts,"
                       "abandoned_checkout_url,email,shipping_address,billing_address,"
                       "line_items,discount_codes,referring_site,source_name,completed_at")
        }

        page = 1
        while True:
            log(f"      Página {page}...")
            resp = shopify_get("checkouts.json", params, token=token)
            data = resp.json().get("checkouts", [])
            todos_checkouts.extend(data)

            link_header = resp.headers.get("Link", "")
            if 'rel="next"' not in link_header:
                break

            match = re.search(r'<[^>]*page_info=([^&>]+)[^>]*>;\s*rel="next"', link_header)
            if not match: break
            params = {"limit": 250, "page_info": match.group(1)}
            page += 1
            time.sleep(0.5)

        if cursor.month == 12: cursor = datetime(cursor.year + 1, 1, 1)
        else: cursor = datetime(cursor.year, cursor.month + 1, 1)
        time.sleep(1)

    return todos_checkouts

# ── Parseo de Datos ──────────────────────────────────────────

def parsear_checkout(ck):
    shipping = ck.get("shipping_address") or ck.get("billing_address") or {}
    line_items = ck.get("line_items", [])
    
    # Productos y SKUs
    productos = " | ".join([f"{li.get('title','?')} x{li.get('quantity',1)}" for li in line_items])
    skus = " | ".join([str(li.get('sku', '')) for li in line_items if li.get('sku')])
    
    # Códigos de descuento
    discounts = ck.get("discount_codes", [])
    codigos = ", ".join([d.get("code", "") for d in discounts if d.get("code")])

    return {
        "fecha":              ck.get("created_at", "")[:10],
        "checkout_id":        str(ck.get("id")),
        "token":              ck.get("token"),
        "email":              ck.get("email", ""),
        "importe_total":      float(ck.get("total_price", 0) or 0),
        "subtotal":           float(ck.get("subtotal_price", 0) or 0),
        "total_discounts":    float(ck.get("total_discounts", 0) or 0),
        "codigos_descuento":  codigos,
        "url_recuperacion":   ck.get("abandoned_checkout_url", ""),
        "pais":               shipping.get("country", ""),
        "ciudad":             shipping.get("city", ""),
        "zip":                shipping.get("zip", ""),
        "num_productos":      sum(li.get("quantity", 1) for li in line_items),
        "productos":          productos,
        "skus":               skus,
        "referring_site":     ck.get("referring_site", "") or "",
        "source_name":        ck.get("source_name", ""),
        "recuperado":         "Sí" if ck.get("completed_at") else "No"
    }

# ── Escritura en Google Sheets ───────────────────────────────

def upsert_sheet(sheet, df, nombre_pestana, claves):
    log(f"   Preparando upsert en '{nombre_pestana}'...")
    claves_validas = [c for c in claves if c in df.columns]

    for intento in range(3):
        try:
            ws = sheet.worksheet(nombre_pestana)
            valores_exist = ws.get_all_values()

            if not valores_exist or len(valores_exist) < 1:
                ws.clear()
                _write_in_chunks(ws, [df.columns.tolist()] + df.fillna("").values.tolist())
                return

            h_exist     = valores_exist[0]
            filas_exist = valores_exist[1:]
            df_exist    = pd.DataFrame(filas_exist, columns=h_exist)

            # Unir y eliminar duplicados (el nuevo dato pisa al viejo)
            df_merged = (
                pd.concat([df_exist, df], ignore_index=True)
                  .drop_duplicates(subset=claves_validas, keep="last")
                  .sort_values(by="fecha", ascending=False)
            )

            ws.clear()
            _write_in_chunks(ws, [df_merged.columns.tolist()] + df_merged.fillna("").values.tolist())
            log(f"  ✅ Actualizado: {len(df_merged)} filas totales.")
            return

        except Exception as e:
            log(f"  ⚠️ Intento {intento+1} fallido: {e}")
            time.sleep(5)

def _write_in_chunks(ws, data, chunk_rows=5000):
    headers = data[0]
    rows    = data[1:]
    ws.update(range_name="A1", values=[headers], value_input_option="USER_ENTERED")
    for start in range(0, len(rows), chunk_rows):
        chunk = rows[start:start + chunk_rows]
        ws.update(range_name=f"A{start + 2}", values=chunk, value_input_option="USER_ENTERED")
        time.sleep(1)

# ── Main ─────────────────────────────────────────────────────

def main():
    log("🚀 Iniciando Extracción de Abandonados")
    
    token = obtener_access_token()
    
    fecha_fin = datetime.utcnow()
    fecha_inicio = fecha_fin - timedelta(days=DIAS_ATRAS)
    
    log(f"📅 Rango: {fecha_inicio.strftime('%Y-%m-%d')} a {fecha_fin.strftime('%Y-%m-%d')}")
    
    checkouts = obtener_abandonados(fecha_inicio.strftime("%Y-%m-%dT00:00:00Z"), 
                                    fecha_fin.strftime("%Y-%m-%dT23:59:59Z"), token)
    
    if not checkouts:
        log("⚠️ No se encontraron checkouts abandonados.")
        return

    filas = [parsear_checkout(c) for c in checkouts]
    df = pd.DataFrame(filas)
    
    log("🔑 Conectando a Sheets...")
    gc = conectar_sheets()
    sheet = gc.open_by_key(SHEET_ID)
    
    upsert_sheet(sheet, df, PESTANA, ["checkout_id"])
    log("🎉 Proceso finalizado con éxito.")

if __name__ == "__main__":
    main()
