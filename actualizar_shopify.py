"""
actualizar_shopify.py
---------------------
- Trae TODOS los pedidos independientemente del canal (web, API, middleware, etc.)
- Vuelca devoluciones como columnas en shopify_orders Y como filas en shopify_refunds
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
SHEET_ID   = "1j84VyucNRrRx7haLKm16ppRuaL28p5BaX75D4BHaqWs"
PESTANA_ORDERS  = "shopify_orders"
PESTANA_REFUNDS = "shopify_refunds"
DIAS_ATRAS = 460

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


# ── Obtención de pedidos (todos los canales) ─────────────────

def obtener_pedidos(fecha_inicio, fecha_fin, token):
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

        # Sin source_name → todos los canales
        params = {
            "status":         "any",
            "created_at_min": rango_inicio,
            "created_at_max": rango_fin,
            "limit":          250,
        }

        page = 1
        while True:
            log(f"      Página {page}...")
            resp = shopify_get("orders.json", params, token=token)
            data = resp.json().get("orders", [])
            todos.extend(data)

            link_header = resp.headers.get("Link", "")
            if 'rel="next"' not in link_header:
                break

            match = re.search(r'<[^>]*page_info=([^&>]+)[^>]*>;\s*rel="next"', link_header)
            if not match:
                break
            params = {"limit": 250, "page_info": match.group(1)}
            page += 1
            time.sleep(0.5)

        if cursor.month == 12:
            cursor = datetime(cursor.year + 1, 1, 1)
        else:
            cursor = datetime(cursor.year, cursor.month + 1, 1)
        time.sleep(1)

    return todos


# ── Obtención de refunds independientes ──────────────────────

def obtener_refunds_de_pedido(pedido_id, token):
    """Llama a /orders/{id}/refunds.json para obtener el detalle completo."""
    try:
        resp = shopify_get(f"orders/{pedido_id}/refunds.json", token=token)
        return resp.json().get("refunds", [])
    except Exception as e:
        log(f"   ⚠️  Error obteniendo refunds de pedido {pedido_id}: {e}")
        return []


# ── Helpers ──────────────────────────────────────────────────

def safe_float(val):
    try:
        return float(val or 0)
    except (ValueError, TypeError):
        return 0.0


def safe_str(val):
    return str(val).strip() if val is not None else ""


def extraer_utm(landing_site):
    if not landing_site:
        return {}, None
    try:
        parsed = urlparse(
            landing_site if landing_site.startswith("http") else f"https://x.com{landing_site}"
        )
        qs = parse_qs(parsed.query)
        utms = {
            "utm_source":   qs.get("utm_source",   [None])[0],
            "utm_medium":   qs.get("utm_medium",   [None])[0],
            "utm_campaign": qs.get("utm_campaign", [None])[0],
            "utm_content":  qs.get("utm_content",  [None])[0],
            "utm_term":     qs.get("utm_term",     [None])[0],
        }
        match = re.search(r"/([a-z]{2}-[a-z]{2})/", parsed.path)
        idioma = match.group(1) if match else None
        return utms, idioma
    except Exception:
        return {}, None


def extraer_mercado(idioma, pais):
    if idioma:
        prefijo = idioma.split("-")[1].upper() if "-" in idioma else idioma.upper()
        if prefijo == "ES": return "España"
        if prefijo == "DE": return "Alemania"
        if prefijo == "FR": return "Francia"
        if prefijo == "EU": return "Europa"
    if pais:
        p = pais.upper()
        if p in ["ES", "SPAIN", "ESPAÑA"]:    return "España"
        if p in ["DE", "GERMANY", "ALEMANIA"]: return "Alemania"
        if p in ["FR", "FRANCE", "FRANCIA"]:   return "Francia"
    return "Otro"


# ── Parseo de pedidos ────────────────────────────────────────

def parsear_pedido(pedido):
    shipping    = pedido.get("shipping_address") or pedido.get("billing_address") or {}
    pais_codigo = shipping.get("country_code", "")
    pais_nombre = shipping.get("country", "")
    ciudad      = shipping.get("city", "")
    provincia   = shipping.get("province", "")
    zip_code    = shipping.get("zip", "")

    landing      = pedido.get("landing_site", "")
    utms, idioma = extraer_utm(landing)
    mercado      = extraer_mercado(idioma, pais_codigo or pais_nombre)

    cliente         = pedido.get("customer") or {}
    cliente_id      = cliente.get("id", "")
    cliente_email   = cliente.get("email", "") or pedido.get("email", "")
    cliente_nombre  = f"{cliente.get('first_name','')} {cliente.get('last_name','')}".strip()
    cliente_pedidos = cliente.get("orders_count", 0)
    cliente_gasto   = safe_float(cliente.get("total_spent"))
    cliente_tags    = cliente.get("tags", "")
    cliente_nuevo   = "Sí" if cliente_pedidos == 1 else "No"

    moneda            = safe_str(pedido.get("currency", "EUR"))
    importe_total     = safe_float(pedido.get("total_price"))
    subtotal          = safe_float(pedido.get("subtotal_price"))
    descuentos        = safe_float(pedido.get("total_discounts"))
    total_impuestos   = safe_float(pedido.get("total_tax"))
    total_envio       = safe_float(
        sum(safe_float(s.get("price")) for s in pedido.get("shipping_lines", []))
    )
    total_reembolsado = safe_float(
        pedido.get("total_refunds") or
        sum(safe_float(r.get("transactions", [{}])[0].get("amount", 0))
            for r in pedido.get("refunds", []) if r.get("transactions"))
    )

    discount_codes = pedido.get("discount_codes", [])
    codigos_dcto   = ", ".join([safe_str(d.get("code")) for d in discount_codes if d.get("code")])
    tipo_dcto      = ", ".join([safe_str(d.get("type")) for d in discount_codes if d.get("type")])

    line_items  = pedido.get("line_items", [])
    num_items   = sum(li.get("quantity", 1) for li in line_items)
    productos   = " | ".join([
        f"{safe_str(li.get('name')) or '?'} x{li.get('quantity', 1)}" for li in line_items
    ])
    skus        = " | ".join([safe_str(li.get("sku")) for li in line_items if li.get("sku")])
    product_ids = " | ".join([str(li.get("product_id", "")) for li in line_items if li.get("product_id")])
    vendors     = " | ".join(list(dict.fromkeys([
        safe_str(li.get("vendor")) for li in line_items if li.get("vendor")
    ])))

    shipping_lines = pedido.get("shipping_lines", [])
    metodo_envio   = " | ".join([safe_str(s.get("title"))  for s in shipping_lines])
    carrier_envio  = " | ".join([safe_str(s.get("source")) for s in shipping_lines])

    fulfillments     = pedido.get("fulfillments", [])
    tracking_numbers = " | ".join([safe_str(f.get("tracking_number")) for f in fulfillments if f.get("tracking_number")])
    tracking_urls    = " | ".join([safe_str(f.get("tracking_url"))    for f in fulfillments if f.get("tracking_url")])
    fecha_envio      = fulfillments[0].get("created_at", "")[:10] if fulfillments else ""

    refunds         = pedido.get("refunds", [])
    tiene_reembolso = "Sí" if refunds else "No"
    fecha_reembolso = refunds[0].get("created_at", "")[:10] if refunds else ""

    nota            = safe_str(pedido.get("note"))
    note_attributes = "; ".join([
        f"{na.get('name')}={na.get('value')}"
        for na in pedido.get("note_attributes", [])
        if na.get("name")
    ])
    impuestos_incluidos = "Sí" if pedido.get("taxes_included") else "No"

    # ── Canal de venta (NUEVO) ───────────────────────────────
    source_name = safe_str(pedido.get("source_name", ""))
    canal = {
        "web":         "Online Store",
        "pos":         "POS",
        "shopify_draft_order": "Draft Order",
        "api":         "API",
        "":            "Desconocido",
    }.get(source_name, source_name)  # si no está en el mapa, muestra el valor raw

    return {
        # Identificación
        "fecha":                pedido.get("created_at", "")[:10],
        "fecha_hora":           pedido.get("created_at", ""),
        "pedido_id":            str(pedido.get("id", "")),
        "order_number":         pedido.get("order_number", ""),
        "order_status_url":     pedido.get("order_status_url", ""),
        "name":                 pedido.get("name", ""),
        # Canal (NUEVO)
        "canal":                canal,
        "source_name_raw":      source_name,
        # Estado
        "estado_pago":          pedido.get("financial_status", ""),
        "estado_envio":         pedido.get("fulfillment_status", "") or "unfulfilled",
        "cancelado":            "Sí" if pedido.get("cancelled_at") else "No",
        "fecha_cancelacion":    (pedido.get("cancelled_at") or "")[:10],
        "motivo_cancelacion":   pedido.get("cancel_reason", "") or "",
        "cerrado_en":           (pedido.get("closed_at") or "")[:10],
        # Importes
        "moneda":               moneda,
        "importe_total":        importe_total,
        "subtotal":             subtotal,
        "descuentos":           descuentos,
        "total_impuestos":      total_impuestos,
        "total_envio":          total_envio,
        "total_reembolsado":    total_reembolsado,
        "impuestos_incluidos":  impuestos_incluidos,
        # Descuentos
        "codigos_descuento":    codigos_dcto,
        "tipo_descuento":       tipo_dcto,
        # Cliente
        "cliente_id":           str(cliente_id),
        "cliente_email":        cliente_email,
        "cliente_nombre":       cliente_nombre,
        "cliente_nuevo":        cliente_nuevo,
        "cliente_num_pedidos":  cliente_pedidos,
        "cliente_gasto_total":  cliente_gasto,
        "cliente_tags":         cliente_tags,
        # Geo
        "pais":                 pais_nombre,
        "pais_codigo":          pais_codigo,
        "provincia":            provincia,
        "ciudad":               ciudad,
        "zip":                  zip_code,
        "idioma":               idioma or "",
        "mercado":              mercado,
        # Tráfico / UTM
        "utm_source":           utms.get("utm_source")   or source_name or "",
        "utm_medium":           utms.get("utm_medium")   or "",
        "utm_campaign":         utms.get("utm_campaign") or "",
        "utm_content":          utms.get("utm_content")  or "",
        "utm_term":             utms.get("utm_term")     or "",
        "referring_site":       pedido.get("referring_site", "") or "",
        "landing_site":         landing or "",
        # Productos
        "num_productos":        num_items,
        "productos":            productos,
        "skus":                 skus,
        "product_ids":          product_ids,
        "vendors":              vendors,
        # Envío
        "metodo_envio":         metodo_envio,
        "carrier_envio":        carrier_envio,
        "fecha_envio":          fecha_envio,
        "tracking_number":      tracking_numbers,
        "tracking_url":         tracking_urls,
        # Reembolsos (resumen en pedido)
        "tiene_reembolso":      tiene_reembolso,
        "fecha_reembolso":      fecha_reembolso,
        # Notas
        "nota":                 nota,
        "note_attributes":      note_attributes,
        "tags":                 pedido.get("tags", "") or "",
        # Otros
        "gateway":              pedido.get("gateway", "") or "",
        "procesado_en":         (pedido.get("processed_at") or "")[:10],
        "test":                 "Sí" if pedido.get("test") else "No",
    }


# ── Parseo de devoluciones (pestaña independiente) ───────────

def parsear_refunds(pedido, refunds_detalle):
    """Genera una fila por cada devolución con contexto del pedido."""
    filas = []
    pedido_id    = str(pedido.get("id", ""))
    order_number = pedido.get("order_number", "")
    mercado      = parsear_pedido(pedido).get("mercado", "")
    cliente      = pedido.get("customer") or {}
    cliente_email = cliente.get("email", "") or pedido.get("email", "")

    for refund in refunds_detalle:
        refund_id   = str(refund.get("id", ""))
        fecha       = (refund.get("created_at") or "")[:10]
        nota        = safe_str(refund.get("note"))
        restock     = "Sí" if refund.get("restock") else "No"

        # Importe devuelto (suma de transactions)
        transactions = refund.get("transactions", [])
        importe_devuelto = sum(
            safe_float(t.get("amount")) for t in transactions
            if t.get("kind") in ("refund", "void")
        )
        gateway = " | ".join(list(dict.fromkeys([
            safe_str(t.get("gateway")) for t in transactions if t.get("gateway")
        ])))

        # Productos devueltos
        refund_items = refund.get("refund_line_items", [])
        productos_devueltos = " | ".join([
            f"{safe_str(li.get('line_item', {}).get('name'))} x{li.get('quantity', 1)}"
            for li in refund_items
        ])
        skus_devueltos = " | ".join([
            safe_str(li.get("line_item", {}).get("sku"))
            for li in refund_items if li.get("line_item", {}).get("sku")
        ])
        subtotal_devuelto = sum(
            safe_float(li.get("subtotal")) for li in refund_items
        )
        impuesto_devuelto = sum(
            safe_float(li.get("total_tax")) for li in refund_items
        )

        filas.append({
            "fecha":               fecha,
            "refund_id":           refund_id,
            "pedido_id":           pedido_id,
            "order_number":        order_number,
            "cliente_email":       cliente_email,
            "mercado":             mercado,
            "importe_devuelto":    importe_devuelto,
            "subtotal_devuelto":   subtotal_devuelto,
            "impuesto_devuelto":   impuesto_devuelto,
            "productos_devueltos": productos_devueltos,
            "skus_devueltos":      skus_devueltos,
            "restock":             restock,
            "gateway":             gateway,
            "nota":                nota,
        })

    return filas


# ── Upsert en Google Sheets ──────────────────────────────────

def upsert_sheet(sheet, df, nombre_pestana, claves):
    log(f"   Columnas en df ({len(df.columns)}): {list(df.columns)}")
    claves_validas = [c for c in claves if c in df.columns]

    if not claves_validas:
        log(f"  ⚠️  Ninguna clave válida — abortando upsert.")
        return

    for intento in range(3):
        try:
            ws            = sheet.worksheet(nombre_pestana)
            valores_exist = ws.get_all_values()

            if not valores_exist or len(valores_exist) < 2:
                ws.clear()
                _write_in_chunks(ws, [df.columns.tolist()] + df.fillna("").values.tolist())
                log(f"  ✅ '{nombre_pestana}': {len(df)} filas escritas (primera vez)")
                return

            h_exist     = valores_exist[0]
            idx_validos = [i for i, h in enumerate(h_exist) if h.strip() != ""]
            h_limpios   = [h_exist[i] for i in idx_validos]
            filas_exist = [
                [fila[i] if i < len(fila) else "" for i in idx_validos]
                for fila in valores_exist[1:]
            ]
            filas_exist = [f for f in filas_exist if any(v.strip() != "" for v in f)]
            df_exist    = pd.DataFrame(filas_exist, columns=h_limpios)

            for c in claves_validas:
                if c in df_exist.columns:
                    df_exist[c] = df_exist[c].astype(str).str.strip()
                if c in df.columns:
                    df[c] = df[c].astype(str).str.strip()

            claves_merge = [c for c in claves_validas if c in df_exist.columns]
            if not claves_merge:
                ws.clear()
                _write_in_chunks(ws, [df.columns.tolist()] + df.fillna("").values.tolist())
                log(f"  ✅ '{nombre_pestana}': {len(df)} filas escritas (reescritura completa)")
                return

            n_antes   = len(df_exist)
            df_merged = (
                pd.concat([df_exist, df], ignore_index=True)
                  .drop_duplicates(subset=claves_merge, keep="last")
                  .sort_values(by=claves_merge[0], ascending=False, key=lambda x: x.astype(str))
            )

            ws.clear()
            _write_in_chunks(ws, [df_merged.columns.tolist()] + df_merged.fillna("").values.tolist())

            nuevas = max(len(df_merged) - n_antes, 0)
            log(f"  ✅ '{nombre_pestana}': {len(df_merged)} filas totales "
                f"({nuevas} nuevas / {len(df)} procesadas)")
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

    fecha_fin    = datetime.utcnow()
    fecha_inicio = fecha_fin - timedelta(days=DIAS_ATRAS)
    log(f"📅 Rango: {fecha_inicio.strftime('%Y-%m-%d')} → {fecha_fin.strftime('%Y-%m-%d')}")
    log("─" * 50)

    log("🔑 Conectando a Google Sheets...")
    gc    = conectar_sheets()
    sheet = gc.open_by_key(SHEET_ID)
    log("✅ Conectado")
    log("─" * 50)

    log("🔑 Autenticando con Shopify...")
    token = obtener_access_token()
    log("─" * 50)

    log("📥 Obteniendo pedidos de Shopify (todos los canales)...")
    pedidos = obtener_pedidos(
        fecha_inicio.strftime("%Y-%m-%dT00:00:00Z"),
        fecha_fin.strftime("%Y-%m-%dT23:59:59Z"),
        token,
    )
    log(f"   {len(pedidos)} pedidos obtenidos")

    if not pedidos:
        log("⚠️  No hay pedidos en el rango. Finalizando.")
        return

    # ── Parsear pedidos ──────────────────────────────────────
    filas_orders  = []
    filas_refunds = []

    for i, pedido in enumerate(pedidos):
        filas_orders.append(parsear_pedido(pedido))

        # Solo procesar refunds si el pedido tiene alguna
        if pedido.get("refunds"):
            refunds_detalle = obtener_refunds_de_pedido(pedido["id"], token)
            filas_refunds.extend(parsear_refunds(pedido, refunds_detalle))
            time.sleep(0.3)  # respetar rate limit

        if (i + 1) % 100 == 0:
            log(f"   Procesados {i + 1}/{len(pedidos)} pedidos...")

    log(f"   {len(filas_orders)} pedidos procesados")
    log(f"   {len(filas_refunds)} devoluciones encontradas")
    log("─" * 50)

    # ── Volcar pedidos ───────────────────────────────────────
    df_orders = pd.DataFrame(filas_orders)
    upsert_sheet(sheet, df_orders, PESTANA_ORDERS, claves=["pedido_id"])

    # ── Volcar devoluciones ──────────────────────────────────
    if filas_refunds:
        df_refunds = pd.DataFrame(filas_refunds)
        upsert_sheet(sheet, df_refunds, PESTANA_REFUNDS, claves=["refund_id"])
    else:
        log("ℹ️  Sin devoluciones en el período.")

    log("─" * 50)
    log("🎉 Sincronización Shopify completada.")


if __name__ == "__main__":
    main()
