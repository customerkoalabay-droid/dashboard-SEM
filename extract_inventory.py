import os
import time
import requests
import pandas as pd

# 1. Configuración de variables (las que ya tienes en GitHub Actions)
SHOP = os.getenv("SHOPIFY_STORE", "koalabay")
CLIENT_ID = os.getenv("SHOPIFY_API_KEY")      # Aquí va tu API Key / Client ID
CLIENT_SECRET = os.getenv("SHOPIFY_API_SECRET") # Aquí va tu Client Secret
API_VERSION = "2024-04"

token = None
token_expires_at = 0.0

def get_token():
    global token, token_expires_at
    if token and time.time() < token_expires_at - 60:
        return token

    print(f"Solicitando nuevo token para {SHOP}...")
    response = requests.post(
        f"https://{SHOP}.myshopify.com/admin/oauth/access_token",
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
    # Si la respuesta no trae expires_in (algunas apps privadas), ponemos 1 hora por defecto
    token_expires_at = time.time() + data.get("expires_in", 3600)
    return token

def get_all_inventory():
    all_levels = []
    url = f"https://{SHOP}.myshopify.com/admin/api/{API_VERSION}/inventory_levels.json?limit=250"
    
    print("Iniciando descarga de inventario...")
    
    while url:
        headers = {
            "Content-Type": "application/json",
            "X-Shopify-Access-Token": get_token()
        }
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            levels = data.get('inventory_levels', [])
            all_levels.extend(levels)
            print(f"Descargados {len(all_levels)} niveles...")
            
            # Paginación
            link_header = response.headers.get('Link')
            if link_header and 'rel="next"' in link_header:
                url = link_header.split('<')[1].split('>')[0]
            else:
                url = None
        elif response.status_code == 429:
            time.sleep(2)
        else:
            print(f"Error: {response.status_code} - {response.text}")
            break
            
    return pd.DataFrame(all_levels)

if __name__ == "__main__":
    df = get_all_inventory()
    if not df.empty:
        df.to_csv("inventario_total_koalabay.csv", index=False)
        print("Archivo guardado con éxito.")
