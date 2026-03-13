"""
actualizar_google_ads.py
------------------------
Lee los informes de Google Ads exportados a Google Sheets,
los limpia y normaliza, y los vuelca al Sheet principal del dashboard.

Hojas de origen (Google Ads → Sheets):
  - Campañas:          1saU4aeiEn9I9bcxg60rkhqJZKYKfpdqz6q7fO2NWGWA  (pestaña: "Campañas Gads a Sheets")
  - Grupos de anuncios: 1hXnrcmfXww-fuf-W1bD2_nvamKjx0kiQNt6JVgdct1U  (pestaña: "GRUPOS DE ANUNCIOS A SHEETS")
  - Anuncios:          1ROJL13zPjZJOJOBt5oTfTUU9hscZS5ill3gX7fZXbF4  (pestaña: "anuncios a sheets")

Hoja destino (dashboard principal):
  - SHEET_ID definido abajo → pestañas: gads_campaigns, gads_adgroups, gads_ads

Uso:
    python actualizar_google_ads.py

Requisitos:
    pip install gspread google-auth pandas
"""

import json
import os
import time
from datetime import datetime

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

# ============================================================
# CONFIGURACIÓN
# ============================================================
SHEET_ID = "1evv-YemzQfKFUr4mZyLEqne2ALqPD6v8rzFUlp68fcE"  # Dashboard principal

FUENTES = {
    "gads_campaigns": {
        "sheet_id":  "1saU4aeiEn9I9bcxg60rkhqJZKYKfpdqz6q7fO2NWGWA",
        "pestaña":   "Campañas Gads a Sheets",
        "claves":    ["fecha", "campaña"],
    },
    "gads_adgroups": {
        "sheet_id":  "19x-ds9L2_kOYZ_pYilii43KJu64GEwh-e4k5WtjorwQ",
        "pestaña":   "GRUPOS DE ANUNCIOS A SHEETS",
        "claves":    ["fecha", "campaña", "grupo_anuncios"],
    },
    "gads_ads": {
        "sheet_id":  "1ROJL13zPjZJOJOBt5oTfTUU9hscZS5ill3gX7fZXbF4",
        "pestaña":   "anuncios a sheets",
        "claves":    ["fecha", "campaña", "grupo_anuncios", "anuncio"],
    },
}

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


# ── Limpieza y normalización ─────────────────────────────────

def normalizar_columna(col):
    """Normaliza nombres de columnas: minúsculas, sin espacios ni caracteres raros."""
    return (
        col.strip()
           .lower()
           .replace(" ", "_")
           .replace("á", "a").replace("é", "e").replace("í", "i")
           .replace("ó", "o").replace("ú", "u").replace("ñ", "n")
           .replace(".", "").replace("/", "_").replace("%", "pct")
           .replace("(", "").replace(")", "")
    )


def limpiar_numero(valor):
    """Limpia valores numéricos que vienen como strings de Google Ads."""
    if pd.isna(valor) or valor == "--" or valor == "":
        return 0
    if isinstance(valor, str):
        valor = valor.replace(".", "").replace(",", ".").replace("%", "").replace("€", "").strip()
    try:
        return float(valor)
    except (ValueError, TypeError):
        return valor


def limpiar_df(df):
    """Limpia y normaliza un dataframe de Google Ads."""
    # Normalizar nombres de columnas
    df.columns = [normalizar_columna(c) for c in df.columns]

    # Eliminar filas vacías o de totales que mete Google Ads al exportar
    if "dia" in df.columns:
        df = df.rename(columns={"dia": "fecha"})
    if "fecha" in df.columns:
        df = df[df["fecha"].notna()]
        df = df[~df["fecha"].astype(str).str.lower().isin(["total", "totales", "fecha", ""])]

    # Renombrar columnas comunes de Google Ads a nombres consistentes
    renombres = {
        "campana":                          "campaña",
        "grupo_de_anuncios":                "grupo_anuncios",
        "anuncio_adaptable_de_busqueda":    "anuncio",
        "anuncio":                          "anuncio",
        "clics":                            "clics",
        "impr":                             "impresiones",
        "impresiones":                      "impresiones",
        "ctr":                              "ctr",
        "coste":                            "gasto",
        "cpc_medio":                        "cpc",
        "conv":                             "conversiones",
        "conversiones":                     "conversiones",
        "valor_conv_/_coste":               "roas",
        "valor_de_conversion":              "valor_conversiones",
        "coste_/_conv":                     "coste_conversion",
        "tasa_de_conversion":               "tasa_conversion",
        "todas_las_conv":                   "todas_conversiones",
        "conv_multidispositivo":            "conv_multidispositivo",
        "impr_cuota_busqueda":              "cuota_impresiones_busqueda",
        "impr_cuota_de_busqueda":           "cuota_impresiones_busqueda",
    }
    df = df.rename(columns={k: v for k, v in renombres.items() if k in df.columns})

    # Limpiar valores numéricos
    cols_texto = ["fecha", "campaña", "grupo_anuncios", "anuncio", "tipo_de_campana",
                  "tipo_de_red", "estado", "estado_del_anuncio"]
    for col in df.columns:
        if col not in cols_texto:
            df[col] = df[col].apply(limpiar_numero)
            try:
                df[col] = pd.to_numeric(df[col])
            except (ValueError, TypeError):
                pass

    # Añadir columna mercado y tipo desde naming convention
    if "campaña" in df.columns:
        df["mercado"]      = df["campaña"].apply(extraer_mercado)
        df["tipo_campaña"] = df["campaña"].apply(extraer_tipo)

    return df


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
    if "PROSPECTING"  in n: return "Prospecting"
    if "REMARKETING"  in n: return "Remarketing"
    if "BRAND"        in n: return "Brand"
    if "SHOPPING"     in n: return "Shopping"
    if "PMAX"         in n: return "Performance Max"
    return "Otro"


# ── Upsert en Google Sheets ──────────────────────────────────

def upsert_sheet(sheet, df, nombre_pestaña, claves):
    for intento in range(3):
        try:
            ws = sheet.worksheet(nombre_pestaña)

            # Usar get_all_values para evitar error con headers duplicados/vacíos
            valores_exist = ws.get_all_values()

            if not valores_exist or len(valores_exist) < 2:
                ws.clear()
                ws.update([df.columns.tolist()] + df.fillna("").values.tolist())
                log(f"  ✅ '{nombre_pestaña}': {len(df)} filas escritas (primera vez)")
                return

            # Reconstruir df existente limpiando columnas vacías
            h_exist = valores_exist[0]
            idx_validos = [i for i, h in enumerate(h_exist) if h.strip() != ""]
            h_limpios = [h_exist[i] for i in idx_validos]
            filas_exist = [[fila[i] if i < len(fila) else "" for i in idx_validos] for fila in valores_exist[1:]]
            filas_exist = [f for f in filas_exist if any(v.strip() != "" for v in f)]

            df_exist = pd.DataFrame(filas_exist, columns=h_limpios)

            # Filtrar claves que existen en ambos dataframes
            claves_validas = [c for c in claves if c in df.columns and c in df_exist.columns]

            df_merged = (
                pd.concat([df_exist, df], ignore_index=True)
                .drop_duplicates(subset=claves_validas, keep="last")
                .sort_values(by=claves_validas[0], ascending=False)
            )

            ws.clear()
            ws.update([df_merged.columns.tolist()] + df_merged.fillna("").values.tolist())

            nuevas = len(df_merged) - len(df_exist)
            log(f"  ✅ '{nombre_pestaña}': {len(df_merged)} filas totales "
                f"({max(nuevas, 0)} nuevas / {len(df)} procesadas)")
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
    log("🚀 Iniciando sincronización Google Ads → Dashboard")
    log("─" * 50)

    log("🔑 Conectando a Google Sheets...")
    gc    = conectar_sheets()
    sheet = gc.open_by_key(SHEET_ID)
    log("✅ Conectado")
    log("─" * 50)

    for nombre_destino, config in FUENTES.items():
        log(f"📥 Leyendo {nombre_destino}...")
        try:
            # Abrir sheet de origen
            sheet_origen = gc.open_by_key(config["sheet_id"])
            ws_origen    = sheet_origen.worksheet(config["pestaña"])

            # Leer con get_all_values para evitar error de headers duplicados (columnas vacias de Google Ads)
            valores = ws_origen.get_all_values()
            if not valores or len(valores) < 2:
                log(f"  ⚠️  '{config['pestaña']}' está vacía, saltando...")
                continue

            # Google Ads exporta filas de metadata antes de los headers reales:
            # Fila 1: nombre del informe
            # Fila 2: rango de fechas
            # Fila 3: headers reales (contienen "Día", "Campaña", etc.)
            # Detectar la fila de headers reales buscando la que tenga más columnas no vacías
            # y que contenga palabras clave típicas de Google Ads
            PALABRAS_CLAVE_HEADER = {"día", "dia", "campaña", "campana", "grupo", "anuncio",
                                      "clics", "impresiones", "coste", "conversiones", "ctr"}
            fila_header_idx = 0
            for i, fila in enumerate(valores):
                celdas_no_vacias = [c.strip().lower() for c in fila if c.strip() != ""]
                if any(p in celdas_no_vacias for p in PALABRAS_CLAVE_HEADER):
                    fila_header_idx = i
                    break

            log(f"   Headers reales detectados en fila {fila_header_idx + 1}")

            # Limpiar columnas vacías
            headers = valores[fila_header_idx]
            indices_validos = [i for i, h in enumerate(headers) if h.strip() != ""]
            headers_limpios = [headers[i] for i in indices_validos]
            filas_limpias   = [[fila[i] if i < len(fila) else "" for i in indices_validos]
                                for fila in valores[fila_header_idx + 1:]]
            filas_limpias   = [f for f in filas_limpias if any(v.strip() != "" for v in f)]

            df = pd.DataFrame(filas_limpias, columns=headers_limpios)
            log(f"   {len(df)} filas leídas (columnas: {list(df.columns[:5])}...)")

            # Limpiar y normalizar
            df = limpiar_df(df)
            log(f"   {len(df)} filas tras limpieza")

            # Volcar al dashboard
            upsert_sheet(sheet, df, nombre_destino, config["claves"])
            time.sleep(2)

        except Exception as e:
            log(f"  ❌ Error procesando {nombre_destino}: {e}")

        log("─" * 50)

    log("🎉 Sincronización completada.")


if __name__ == "__main__":
    main()
