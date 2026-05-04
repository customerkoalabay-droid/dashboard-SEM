import os
import requests
import pandas as pd
import time

SHOP_NAME = os.environ.get("SHOPIFY_STORE", "koalabay")
ACCESS_TOKEN = os.environ.get("SHOPIFY_API_KEY") 
API_VERSION = "2024-04"

def get_all_inventory():
    all_levels = []
    # Usamos el endpoint de inventory_levels
    url = f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/inventory_levels.json?limit=250"
    headers = {
        "X-Shopify-Access-Token": ACCESS_TOKEN,
        "Content-Type": "application/json"
    }
    
    print(f"Iniciando descarga de inventario para: {SHOP_NAME}...")

    while url:
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            levels = data.get('inventory_levels', [])
            all_levels.extend(levels)
            
            # Gestión de paginación por Link Header
            link_header = response.headers.get('Link')
            if link_header and 'rel="next"' in link_header:
                url = link_header.split('<')[1].split('>')[0]
            else:
                url = None
                
        elif response.status_code == 429:
            # Si llegamos al límite de Plus (raro con 250 items), esperamos
            retry_after = int(response.headers.get("Retry-After", 2))
            print(f"Límite de API alcanzado. Esperando {retry_after} segundos...")
            time.sleep(retry_after)
        else:
            print(f"Error en la API: {response.status_code} - {response.text}")
            break

    return pd.DataFrame(all_levels)

if __name__ == "__main__":
    df_inv = get_all_inventory()
    
    if not df_inv.empty:
        # Guardamos en CSV o Excel según prefieras para GitHub Artifacts
        output_file = "inventario_total_koalabay.csv"
        df_inv.to_csv(output_file, index=False)
        print(f"Proceso finalizado. Se han extraído {len(df_inv)} líneas de inventario.")
    else:
        print("No se extrajeron datos. Revisa las credenciales.")
