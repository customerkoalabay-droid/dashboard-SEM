"""
actualizar_google_ads.py
------------------------
Lee los informes de Google Ads exportados a Google Sheets,
los limpia y normaliza, y los vuelca al Sheet principal del dashboard.

Hojas de origen (Google Ads → Sheets):
  - Campañas:           sheet_id en FUENTES
  - Grupos de anuncios: sheet_id en FUENTES
  - Anuncios:           sheet_id en FUENTES

Hoja destino (dashboard principal):
  - SHEET_ID definido abajo → pestañas: Gads_campaigns, Gads_adgroups, Gads_ads

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
        "sheet_id":  "1-C-nMfK8_eZIAjuSTqTz2fwGNtJS3U8s502EAGvXoII",
        "pestana":   "Hoja 1",
        "destino":   "Gads_campaigns",
        # FIX: hora_del_dia no existe en el informe de campañas → clave correcta
        "claves":    ["fecha", "campana"],
    },
    "gads_adgroups": {
        "sheet_id":  "1KDV801Y5hYt5DI9UxK2OSq7atCb7s8qiEp9jTOshz1E",
        "pestana":   "Hoja 1",
        "destino":   "Gads_adgroups",
        "claves":    ["fecha", "grupo_de_anuncios", "ciudad_ubicacion_de_usuario"],
    },
    "gads_ads": {
        "sheet_id":  "1ZsI9_wdCm6Svt5rjjkTG-xMetQKCaZ7XE1Af-QHKr5A",
        "pestana":   "Hoja 1",
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
    if pd.isna(valor) or valor == "--" or valor == "":
        return 0
    if isinstance(valor, str):
        valor = valor.replace(".", "").replace(",", ".").replace("%", "").replace("€", "").strip()
    try:
        return float(valor)
    except (ValueError, TypeError):
        return valor


def limpiar_df(df):
    df.columns = [normalizar_columna(c) for c in df.columns]

    if "dia" in df.columns:
        df = df.rename(columns={"dia": "fecha"})

    col_map_extra = {}
    for c in df.columns:
        if c.startswith("impr") and c != "impresiones":
            col_map_extra[c] = "impresiones"
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

    if "fecha" in df.columns:
        df = df[df["fecha"].notna()]
        df = df[~df["fecha"].astype(str).str.lower().isin(["total", "totales", "fecha", ""])]
        df = df[df["fecha"].astype(str).str.match(r"^\d{4}-\d{2}-\d{2}$")]

    renombres = {
        "dia":                                      "fecha",
        "campana":                                  "campana",
        "grupo_de_anuncios":                        "grupo_de_anuncios",
        "impr":                                     "impresiones",
        "impr_":                                    "impresiones",
        "clics":                                    "clics",
        "ctr":                                      "ctr",
        "cpc_medio":                                "cpc",
        "coste":                                    "gasto",
        "coste_conv":                               "coste_conversion",
        "coste_todas_las_conversiones":             "coste_todas_conversiones",
        "conversiones":                             "conversiones",
        "todas_las_conversiones":                   "todas_conversiones",
        "conversiones_multidispositivo":            "conv_multidispositivo",
        "tasa_de_conv":                             "tasa_conversion",
        "valor_de_conv":                            "valor_conversiones",
        "valor_conv_coste":                         "roas",
        "valor_de_todas_las_conversiones":          "valor_todas_conversiones",
        "valor_de_todas_las_conversiones_coste":    "roas_todas",
        "tipo_de_estrategia_de_puja_de_la_campana": "tipo_estrategia_puja",
        "dia_de_la_semana":                         "dia_semana",
        "hora_del_dia":                             "hora_del_dia",
        "codigo_de_moneda":                         "moneda",
        "ciudad_ubicacion_de_usuario":              "ciudad_ubicacion_de_usuario",
        "pais_territorio_ubicacion_de_usuario":     "pais",
        "visualizaciones_de_trueview":              "visualizaciones",
        "cpv_medio_de_trueview":                    "cpv",
        "sexo":                                     "sexo",
        "edad":                                     "edad",
        "hijos":                                    "hijos",
    }
    df = df.rename(columns={k: v for k, v in renombres.items() if k in df.columns})

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


# ── Normalizar tipos para merge correcto ─────────────────────

def normalizar_tipos_para_merge(df_exist, df_nuevo, claves):
    """
    FIX: df_exist viene todo como strings desde get_all_values().
    Convertimos las columnas numéricas de df_exist al mismo tipo que df_nuevo
    para que drop_duplicates funcione correctamente.
    """
    for col in df_nuevo.columns:
        if col in df_exist.columns and col not in claves:
            try:
                if pd.api.types.is_numeric_dtype(df_nuevo[col]):
                    df_exist[col] = pd.to_numeric(df_exist[col], errors="coerce").fillna(0)
            except Exception:
                pass

    # Las claves también deben ser strings en ambos para comparar bien
    for clave in claves:
        if clave in df_exist.columns:
            df_exist[clave] = df_exist[clave].astype(str).str.strip()
        if clave in df_nuevo.columns:
            df_nuevo[clave] = df_nuevo[clave].astype(str).str.strip()

    return df_exist, df_nuevo


# ── Upsert en Google Sheets ──────────────────────────────────

def upsert_sheet(sheet, df, nombre_pestaña, claves):
    log(f"   Columnas en df ({len(df.columns)}): {list(df.columns)}")
    log(f"   Claves buscadas: {claves}")

    # Verificar qué claves existen realmente en el df
    claves_validas = [c for c in claves if c in df.columns]
    claves_faltantes = [c for c in claves if c not in df.columns]

    if claves_faltantes:
        log(f"   ⚠️  Claves no encontradas en df (se ignorarán): {claves_faltantes}")

    if not claves_validas:
        log(f"  ⚠️  Ninguna clave válida — volcando todo sin deduplicar.")

    for intento in range(3):
        try:
            ws = sheet.worksheet(nombre_pestaña)
            valores_exist = ws.get_all_values()

            # Hoja vacía o sin datos → escribir directo
            if not valores_exist or len(valores_exist) < 2:
                ws.clear()
                data = [df.columns.tolist()] + df.fillna("").values.tolist()
                _write_in_chunks(ws, data)
                log(f"  ✅ '{nombre_pestaña}': {len(df)} filas escritas (primera vez)")
                return

            # Reconstruir df existente
            h_exist      = valores_exist[0]
            idx_validos  = [i for i, h in enumerate(h_exist) if h.strip() != ""]
            h_limpios    = [h_exist[i] for i in idx_validos]
            filas_exist  = [
                [fila[i] if i < len(fila) else "" for i in idx_validos]
                for fila in valores_exist[1:]
            ]
            filas_exist  = [f for f in filas_exist if any(v.strip() != "" for v in f)]
            df_exist     = pd.DataFrame(filas_exist, columns=h_limpios)

            # Claves que existen en AMBOS dataframes
            claves_merge = [c for c in claves_validas if c in df_exist.columns]

            if not claves_merge:
                # Las claves no coinciden con la hoja destino → reescribir todo
                log(f"  ⚠️  Claves no coinciden con hoja destino. Reescribiendo todo.")
                ws.clear()
                data = [df.columns.tolist()] + df.fillna("").values.tolist()
                _write_in_chunks(ws, data)
                log(f"  ✅ '{nombre_pestaña}': {len(df)} filas escritas (reescritura completa)")
                return

            # FIX: normalizar tipos antes del merge para que drop_duplicates funcione
            df_exist, df = normalizar_tipos_para_merge(df_exist, df, claves_merge)

            n_antes = len(df_exist)
            df_merged = (
                pd.concat([df_exist, df], ignore_index=True)
                  .drop_duplicates(subset=claves_merge, keep="last")
                  .sort_values(by=claves_merge[0], ascending=False)
            )

            ws.clear()
            data = [df_merged.columns.tolist()] + df_merged.fillna("").values.tolist()
            _write_in_chunks(ws, data)

            nuevas = len(df_merged) - n_antes
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
            sheet_origen = gc.open_by_key(config["sheet_id"])
            ws_origen    = sheet_origen.worksheet(config["pestana"])

            valores = ws_origen.get_all_values()
            if not valores or len(valores) < 2:
                log(f"  ⚠️  '{config['pestana']}' está vacía, saltando...")
                continue

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

            headers      = valores[fila_header_idx]
            idx_validos  = [i for i, h in enumerate(headers) if h.strip() != ""]
            headers_limpios = [headers[i] for i in idx_validos]
            filas_limpias   = [[fila[i] if i < len(fila) else "" for i in idx_validos]
                                for fila in valores[fila_header_idx + 1:]]
            filas_limpias   = [f for f in filas_limpias if any(v.strip() != "" for v in f)]

            df = pd.DataFrame(filas_limpias, columns=headers_limpios)
            log(f"   {len(df)} filas leídas (columnas: {list(df.columns[:5])}...)")

            df = limpiar_df(df)
            log(f"   {len(df)} filas tras limpieza")

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
