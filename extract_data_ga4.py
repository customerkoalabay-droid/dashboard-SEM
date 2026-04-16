import os
import json
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from googleapiclient.discovery import build
from google.oauth2 import service_account

# Cargar credenciales
credentials_info = json.loads(os.environ['GOOGLE_SERVICE_ACCOUNT'])
property_id = os.environ['GA_PROPERTY_ID']
spreadsheet_id = os.environ['SPREADSHEET_ID']

def get_analytics_data():
    client = BetaAnalyticsDataClient.from_service_account_info(credentials_info)
    
    print(f"DEBUG: Iniciando consulta para la propiedad {property_id}")
    
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[
            Dimension(name="date"),
            Dimension(name="country")
        ],
        metrics=[
            Metric(name="activeUsers"),
            Metric(name="sessions")
        ],
        # Rango histórico forzado
        date_ranges=[DateRange(start_date="yesterday", end_date="yesterday")],
    )
    
    response = client.run_report(request)
    
    values = []
    if not response.rows:
        print("DEBUG: No se encontraron filas en GA4.")
        return []

    for row in response.rows:
        # EXTRACCIÓN Y FORMATEO DE FECHA
        fecha_cruda = row.dimension_values[0].value  # "20260415"
        
        # Transformación manual a "2026-04-15"
        fecha_f = f"{fecha_cruda[:4]}-{fecha_cruda[4:6]}-{fecha_cruda[6:]}"
        
        pais = row.dimension_values[1].value
        usuarios = row.metric_values[0].value
        sesiones = row.metric_values[1].value
        
        values.append([fecha_f, pais, usuarios, sesiones])
    
    # Ordenar por fecha cronológicamente
    values.sort(key=lambda x: x[0])
    print(f"DEBUG: Se han procesado {len(values)} filas correctamente.")
    return values

def append_to_sheets(data):
    if not data:
        return

    creds = service_account.Credentials.from_service_account_info(credentials_info)
    service = build('sheets', 'v4', credentials=creds)
    
    # IMPORTANTE: Asegúrate de que tu pestaña se llame Hoja1
    # Usamos 'append' para que no borre lo anterior
    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range="Hoja1!A1", 
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={'values': data}
    ).execute()
    print("DEBUG: Datos enviados a Google Sheets.")

if __name__ == "__main__":
    datos = get_analytics_data()
    append_to_sheets(datos)
