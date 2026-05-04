import os
import time
import json
import requests
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials

# --- CONFIGURACIÓN ---
SHOP = os.getenv("SHOPIFY_STORE")
CLIENT_ID = os.getenv("SHOPIFY_API_KEY")
CLIENT_SECRET = os.getenv("SHOPIFY_API_SECRET")
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")  # ← AJUSTA al nombre que uses en tus otros scripts
API_VERSION = "2025-01"

SHEET_ID = "1j84VyucNRrRx7haLKm16ppRuaL28p5BaX75D4BHaqWs"
TAB_NAME = "Inventario"

token = None
token_expires_at = 0.0


def get_token():
    """Obtiene un token temporal usando Client ID y Client Secret"""
    global token, token_expires_at
    if token and time.time() < token_expires_at - 60:
        return token

    url = f"https://{SHOP}.myshopify.com/admin/oauth/access_token"
    print(f"Solicitando acceso para la tienda: {SHOP}...")

    try:
        response = requests.post(
            url,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "client_credentials",
                "client_id": CLIENT_ID,
                "client_secret": CLIENT_SECRET,
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        token = data["access_token"]
        token_expires_at = time.time() + data.get("expires_in", 3600)
        print("¡Token obtenido con éxito!")
        return token
    except Exception as e:
        print(f"ERROR EN AUTENTICACIÓN: {e}")
        if "response" in locals():
            print(f"Detalle del error: {response.text}")
        return None


def get_all_inventory_graphql():
    """Descarga inventario completo vía GraphQL paginado por productVariants"""
    current_token = get_token()
    if not current_token:
        return pd.DataFrame()

    url = f"https://{SHOP}.myshopify.com/admin/api/{API_VERSION}/graphql.json"
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": current_token,
    }

    query = """
    query getVariants($cursor: String) {
      productVariants(first: 250, after: $cursor) {
        edges {
          cursor
          node {
            id
            sku
            title
            barcode
            price
            inventoryQuantity
            product {
              id
              title
              vendor
              productType
              status
              handle
            }
            inventoryItem {
              id
              tracked
              inventoryLevels(first: 20) {
                edges {
                  node {
                    location {
                      id
                      name
                    }
                    quantities(names: ["available", "on_hand", "committed"]) {
                      name
                      quantity
                    }
                  }
                }
              }
            }
          }
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    """

    all_rows = []
    cursor = None
    page = 0

    while True:
        page += 1
        payload = {"query": query, "variables": {"cursor": cursor}}

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=60)
            response.raise_for_status()
            data = response.json()

            if "errors" in data:
                print(f"Error GraphQL: {data['errors']}")
                break

            variants_data = data["data"]["productVariants"]
            edges = variants_data["edges"]

            for edge in edges:
                node = edge["node"]
                product = node.get("product") or {}
                inv_item = node.get("inventoryItem") or {}
                inv_levels = (inv_item.get("inventoryLevels") or {}).get("edges", [])

                if not inv_levels:
                    all_rows.append({
                        "variant_id": node["id"],
                        "sku": node.get("sku"),
                        "barcode": node.get("barcode"),
                        "variant_title": node.get("title"),
                        "price": node.get("price"),
                        "inventory_quantity_total": node.get("inventoryQuantity"),
                        "tracked": inv_item.get("tracked"),
                        "product_id": product.get("id"),
                        "product_title": product.get("title"),
                        "product_handle": product.get("handle"),
                        "vendor": product.get("vendor"),
                        "product_type": product.get("productType"),
                        "status": product.get("status"),
                        "location_id": None,
                        "location_name": None,
                        "available": None,
                        "on_hand": None,
                        "committed": None,
                    })
                    continue

                for lvl_edge in inv_levels:
                    lvl = lvl_edge["node"]
                    loc = lvl.get("location") or {}
                    qty_map = {q["name"]: q["quantity"] for q in (lvl.get("quantities") or [])}

                    all_rows.append({
                        "variant_id": node["id"],
                        "sku": node.get("sku"),
                        "barcode": node.get("barcode"),
                        "variant_title": node.get("title"),
                        "price": node.get("price"),
                        "inventory_quantity_total": node.get("inventoryQuantity"),
                        "tracked": inv_item.get("tracked"),
                        "product_id": product.get("id"),
                        "product_title": product.get("title"),
                        "product_handle": product.get("handle"),
                        "vendor": product.get("vendor"),
                        "product_type": product.get("productType"),
                        "status": product.get("status"),
                        "location_id": loc.get("id"),
                        "location_name": loc.get("name"),
                        "available": qty_map.get("available"),
                        "on_hand": qty_map.get("on_hand"),
                        "committed": qty_map.get("committed"),
                    })

            print(f"Página {page}: {len(edges)} variantes (acumulado filas: {len(all_rows)})")

            page_info = variants_data["pageInfo"]
            if page_info["hasNextPage"]:
                cursor = page_info["endCursor"]
                time.sleep(0.5)
            else:
                break

        except requests.exceptions.HTTPError as e:
            print(f"Error HTTP: {e} - {response.text}")
            break
        except Exception as e:
            print(f"Error: {e}")
            break

    return pd.DataFrame(all_rows)


def subir_a_sheets(df):
    """Sube el DataFrame a la Sheet de Shopify, pestaña Inventario"""
    if not GOOGLE_CREDS_JSON:
        print("ERROR: Falta el secret de credenciales de Google.")
        return False

    try:
        creds_dict = json.loads(GOOGLE_CREDS_JSON)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        client = gspread.authorize(creds)

        spreadsheet = client.open_by_key(SHEET_ID)

        # Crear la pestaña si no existe, o limpiarla si existe
        try:
            worksheet = spreadsheet.worksheet(TAB_NAME)
            worksheet.clear()
            print(f"Pestaña '{TAB_NAME}' encontrada y limpiada.")
        except gspread.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=TAB_NAME,
                rows=str(len(df) + 100),
                cols=str(len(df.columns) + 5),
            )
            print(f"Pestaña '{TAB_NAME}' creada.")

        # Convertir todo a string para evitar problemas con tipos mixtos / NaN
        df_clean = df.fillna("").astype(str)

        # Subir cabecera + datos
        data_to_upload = [df_clean.columns.tolist()] + df_clean.values.tolist()
        worksheet.update(values=data_to_upload, range_name="A1")

        print(f"✓ Subidas {len(df)} filas a la pestaña '{TAB_NAME}'.")
        return True

    except Exception as e:
        print(f"ERROR subiendo a Sheets: {e}")
        return False


if __name__ == "__main__":
    if not all([SHOP, CLIENT_ID, CLIENT_SECRET]):
        print("ERROR: Faltan variables de entorno de Shopify. Revisa los Secrets.")
    else:
        df_inventory = get_all_inventory_graphql()

        if not df_inventory.empty:
            # 1. Guardar CSV local (para el artifact del workflow)
            filename = "inventario_total_koalabay.csv"
            df_inventory.to_csv(filename, index=False)
            print(f"CSV generado: {filename} con {len(df_inventory)} filas.")

            # 2. Subir a Google Sheets
            subir_a_sheets(df_inventory)

            print("--- PROCESO FINALIZADO ---")
        else:
            print("No se extrajeron datos. Revisa los scopes de la App.")
