import os
import time
import requests
import pandas as pd

# --- CONFIGURACIÓN DE VARIABLES (Vienen del YAML) ---
SHOP = os.getenv("SHOPIFY_STORE")
CLIENT_ID = os.getenv("SHOPIFY_API_KEY")
CLIENT_SECRET = os.getenv("SHOPIFY_API_SECRET")
API_VERSION = "2024-04"

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
        # Algunos flujos no devuelven expires_in, ponemos 1h por defecto
        token_expires_at = time.time() + data.get("expires_in", 3600)
        print("¡Token obtenido con éxito!")
        return token
    except Exception as e:
        print(f"ERROR EN AUTENTICACIÓN: {e}")
        if 'response' in locals():
            print(f"Detalle del error: {response.text}")
        return None

def get_all_inventory():
    """Descarga datos de inventario usando el endpoint de Inventory Items"""
    all_data = []
    current_token = get_token()
    if not current_token:
        return pd.DataFrame()

    # Cambiamos a inventory_items que es más robusto para lecturas totales
    # Nota: Este endpoint devuelve información sobre los productos/variantes
    url = f"https://{SHOP}.myshopify.com/admin/api/{API_VERSION}/inventory_items.json?limit=50"
    
    print("Cambiando estrategia: Consultando Inventory Items...")
    
    while url:
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": current_token
        }
        
        try:
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                # El campo aquí se llama inventory_items
                items = data.get('inventory_items', [])
                all_data.extend(items)
                print(f"Total acumulado: {len(all_data)} items...")
                
                # Paginación
                link_header = response.headers.get('Link')
                if link_header and 'rel="next"' in link_header:
                    url = link_header.split('<')[1].split('>')[0]
                else:
                    url = None
            elif response.status_code == 422:
                print("Error 422 persistente: Intentando acceder vía GraphQL...")
                # Si falla aquí, el problema es que Shopify Plus exige GraphQL para esta cuenta
                return get_inventory_via_graphql(current_token)
            else:
                print(f"Error en la descarga: {response.status_code} - {response.text}")
                break
        except Exception as e:
            print(f"Error de conexión: {e}")
            break
            
    return pd.DataFrame(all_data)
if __name__ == "__main__":
    if not all([SHOP, CLIENT_ID, CLIENT_SECRET]):
        print("ERROR: Faltan variables de entorno. Revisa los Secrets en GitHub.")
    else:
        df_inventory = get_all_inventory()
        
        if not df_inventory.empty:
            filename = "inventario_total_koalabay.csv"
            df_inventory.to_csv(filename, index=False)
            print(f"--- PROCESO FINALIZADO ---")
            print(f"Archivo generado: {filename} con {len(df_inventory)} filas.")
        else:
            print("No se extrajeron datos. Revisa los permisos (scopes) de la App.")
