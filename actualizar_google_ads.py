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
SHEET_ID = "1QV-qOoxjdgBNAwxlqYcKyj-EJ_KAEIS8J7TjHsPm0go"  # Dashboard Google Ads

FUENTES = {
    "gads_campaigns": {
        "sheet_id":  "12DF3xp3p_1jh4NKfWCivARkYp5XCs65wj8zQam7W_lw",
        "pestana":   "Gads_campaigns",
        "destino":   "Gads_campaigns",
        "claves":    ["fecha", "campana", "hora_del_dia"],
    },
    "gads_adgroups": {
        "sheet_id":  "19U2XU1C_vhUfANkSt_zBizLqb1JGvk7_44eMH0QIwhQ",
        "pestana":   "Gads_adgroups",
        "destino":   "Gads_adgroups",
        "claves":    ["fecha", "grupo_de_anuncios", "ciudad_ubicacion_de_usuario"],
    },
    "gads_ads": {
        "sheet_id":  "1b0Qb4koO_0ouW9Oqn4a6sM0HXed5QihSDmmjoaf_bvE",
        "pestana":   "Gads_ads",
        "destino":   "Gads_ads",
        "claves":    ["fecha", "campana", "grupo_de_anuncios", "sexo", "edad"],
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

    # "Día" normaliza a "dia" → renombrar a "fecha"
    if "dia" in df.columns:
        df = df.rename(columns={"dia": "fecha"})

    # "Impr." normaliza a "impr_" (el punto se elimina dejando trailing _) → renombrar
    col_map_extra = {}
    for c in df.columns:
        if c.startswith("impr") and c != "impresiones":
            col_map_extra[c] = "impresiones"
        # "Coste/conv." → "coste_conv_" — normalizamos aquí antes del rename principal
        if c == "coste_conv_":
            col_map_extra[c] = "coste_conversion"
        if c == "valor_conv__coste":
            col_map_extra[c] = "roas"
        if c == "tasa_de_conv_":
            col_map_extra[c] = "tasa_conversion"
        if c == "valor_de_conv_":
            col_map_extra[c] = "valor_conversiones"
    if col_map_extra:
        df = df.rename(columns=col_map_extra)

    # Eliminar filas de totales o vacías
    if "fecha" in df.columns:
        df = df[df["fecha"].notna()]
        df = df[~df["fecha"].astype(str).str.lower().isin(["total", "totales", "fecha", ""])]
        # Filtrar filas que no sean fechas válidas (e.g. filas de sub-total)
        df = df[df["fecha"].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}$")]

    # Renombrar columnas de Google Ads a nombres internos consistentes
    # (los nombres están ya normalizados por normalizar_columna())
    renombres = {
        # Fecha
        "dia":                                      "fecha",
        # Campana
        "campana":                                  "campana",    # ya normalizado, lo dejamos como campana (sin tilde para compatibilidad con claves)
        # Grupo de anuncios
        "grupo_de_anuncios":                        "grupo_de_anuncios",
        # Metricas de trafico
        "impr":                                     "impresiones",
        "impr_":                                    "impresiones",   # Google Ads exporta "Impr." → normaliza a "impr_"... a veces con punto
        "clics":                                    "clics",
        "ctr":                                      "ctr",
        "cpc_medio":                                "cpc",
        # Costes
        "coste":                                    "gasto",
        "coste_conv":                               "coste_conversion",
        "coste_todas_las_conversiones":             "coste_todas_conversiones",
        # Conversiones
        "conversiones":                             "conversiones",
        "todas_las_conversiones":                   "todas_conversiones",
        "conversiones_multidispositivo":            "conv_multidispositivo",
        "tasa_de_conv":                             "tasa_conversion",
        # Valor
        "valor_de_conv":                            "valor_conversiones",
        "valor_conv_coste":                         "roas",
        "valor_de_todas_las_conversiones":          "valor_todas_conversiones",
        "valor_de_todas_las_conversiones_coste":    "roas_todas",
        # Otros
        "tipo_de_estrategia_de_puja_de_la_campana": "tipo_estrategia_puja",
        "dia_de_la_semana":                         "dia_semana",
        "hora_del_dia":                             "hora_del_dia",
        "codigo_de_moneda":                         "moneda",
        "ciudad_ubicacion_de_usuario":              "ciudad_ubicacion_de_usuario",
        "pais_territorio_ubicacion_de_usuario":     "pais",
        # Video
        "visualizaciones_de_trueview":              "visualizaciones",
        "cpv_medio_de_trueview":                    "cpv",
        # Demografico
        "sexo":                                     "sexo",
        "edad":                                     "edad",
        "hijos":                                    "hijos",
    }
    df = df.rename(columns={k: v for k, v in renombres.items() if k in df.columns})

    # Limpiar valores numéricos (todo excepto columnas de texto)
    cols_texto = ["fecha", "campana", "grupo_de_anuncios", "tipo_estrategia_puja",
                  "dia_semana", "moneda", "ciudad_ubicacion_de_usuario", "pais",
                  "sexo", "edad", "hijos", "estado"]
    for col in df.columns:
        if col not in cols_texto:
            df[col] = df[col].apply(limpiar_numero)
            try:
                df[col] = pd.to_numeric(df[col])
            except (ValueError, TypeError):
                pass

    # Añadir columna mercado y tipo desde naming convention
    if "campana" in df.columns:
        df["mercado"]      = df["campana"].apply(extraer_mercado)
        df["tipo_campana"] = df["campana"].apply(extraer_tipo)

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
    # Log columnas reales del df para diagnóstico
    log(f"   Columnas en df ({len(df.columns)}): {list(df.columns)}")
    log(f"   Claves buscadas: {claves}")

    # Verificar qué claves existen realmente
    if claves is None:
        log(f"   Deduplicando por todas las columnas")
        claves_validas = None
    else:
        claves_validas = [c for c in claves if c in df.columns]
        if not claves_validas:
            log(f"  ⚠️  Ninguna clave {claves} encontrada en columnas del df. "
                f"Volcando todo sin deduplicar.")

    for intento in range(3):
        try:
            ws = sheet.worksheet(nombre_pestaña)
            valores_exist = ws.get_all_values()

            # Si la hoja destino está vacía o solo tiene headers → escribir directo
            if not valores_exist or len(valores_exist) < 2 or claves_validas == []:
                ws.clear()
                data = [df.columns.tolist()] + df.fillna("").values.tolist()
                _write_in_chunks(ws, data)
                log(f"  ✅ '{nombre_pestaña}': {len(df)} filas escritas")
                return

            # Reconstruir df existente limpiando columnas vacías
            h_exist = valores_exist[0]
            idx_validos  = [i for i, h in enumerate(h_exist) if h.strip() != ""]
            h_limpios    = [h_exist[i] for i in idx_validos]
            filas_exist  = [
                [fila[i] if i < len(fila) else "" for i in idx_validos]
                for fila in valores_exist[1:]
            ]
            filas_exist  = [f for f in filas_exist if any(v.strip() != "" for v in f)]
            df_exist     = pd.DataFrame(filas_exist, columns=h_limpios)

            # Claves que existen en AMBOS dataframes (o todas las columnas si claves=None)
            if claves_validas is None:
                claves_merge = None  # deduplicar por todas las columnas
            else:
                claves_merge = [c for c in claves_validas if c in df_exist.columns]
                if not claves_merge:
                    log(f"  ⚠️  Claves no coinciden con hoja destino existente. Reescribiendo todo.")
                    ws.clear()
                    data = [df.columns.tolist()] + df.fillna("").values.tolist()
                    _write_in_chunks(ws, data)
                    log(f"  ✅ '{nombre_pestaña}': {len(df)} filas escritas (reescritura completa)")
                    return

            df_merged = (
                pd.concat([df_exist, df], ignore_index=True)
                  .drop_duplicates(subset=claves_merge, keep="last")
                  .sort_values(by=(claves_merge[0] if claves_merge else df.columns[0]), ascending=False)
            )

            ws.clear()
            data = [df_merged.columns.tolist()] + df_merged.fillna("").values.tolist()
            _write_in_chunks(ws, data)

            nuevas = len(df_merged) - len(df_exist)
            log(f"  ✅ '{nombre_pestaña}': {len(df_merged)} filas totales "
                f"({max(nuevas, 0)} nuevas / {len(df)} procesadas)")
            return

        except Exception as e:
            import traceback
            log(f"  ⚠️  Intento {intento+1} fallido en '{nombre_pestaña}': {e}")
            log(f"      {traceback.format_exc().splitlines()[-1]}")
            if intento < 2:
                log("  🔄 Reintentando en 5s...")
                time.sleep(5)
            else:
                log(f"  ❌ No se pudo actualizar '{nombre_pestaña}' tras 3 intentos")


def _write_in_chunks(ws, data, chunk_rows=5000):
    """Escribe data en la hoja en chunks para no superar límites de la API de Sheets."""
    if not data:
        return
    headers = data[0]
    rows    = data[1:]
    total_filas_necesarias = len(rows) + 1  # +1 por header

    # Expandir la hoja si no tiene suficientes filas
    filas_actuales = ws.row_count
    if filas_actuales < total_filas_necesarias:
        ws.add_rows(total_filas_necesarias - filas_actuales + 1000)  # margen extra
        time.sleep(1)

    # Escribir headers primero
    ws.update(range_name="A1", values=[headers], value_input_option="USER_ENTERED")
    # Escribir filas en chunks
    for start in range(0, len(rows), chunk_rows):
        chunk = rows[start:start + chunk_rows]
        row_start = start + 2  # +1 por header, +1 porque Sheets es 1-indexed
        ws.update(range_name=f"A{row_start}", values=chunk, value_input_option="USER_ENTERED")
        time.sleep(1)  # pausa para no saturar la API


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
            ws_origen    = sheet_origen.worksheet(config["pestana"])

            # Leer con get_all_values para evitar error de headers duplicados (columnas vacias de Google Ads)
            valores = ws_origen.get_all_values()
            if not valores or len(valores) < 2:
                log(f"  ⚠️  '{config['pestana']}' está vacía, saltando...")
                continue

            # Google Ads exporta filas de metadata antes de los headers reales:
            # Fila 1: nombre del informe, Fila 2: rango de fechas, Fila 3+: headers reales
            PALABRAS_CLAVE_HEADER = {"día", "dia", "campaña", "campana", "grupo", "anuncio",
                                      "clics", "impresiones", "impr", "coste", "conversiones",
                                      "ctr", "sexo", "edad"}
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
            upsert_sheet(sheet, df, config.get("destino", nombre_destino), config["claves"])
            time.sleep(2)

        except Exception as e:
            import traceback
            log(f"  ❌ Error procesando {nombre_destino}: {e}")
            log(f"      {traceback.format_exc()}")

        log("─" * 50)

    log("🎉 Sincronización completada.")


if __name__ == "__main__":
    main()
