import os
import json
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from googleapiclient.discovery import build
from google.oauth2 import service_account

# Cargar credenciales desde variables de entorno
credentials_info = json.loads(os.environ['GA_SERVICE_ACCOUNT'])
property_id = os.environ['GA_PROPERTY_ID']
spreadsheet_id = os.environ['SPREADSHEET_ID']

def get_analytics_data():
    client = BetaAnalyticsDataClient.from_service_account_info(credentials_info)
    
    # Hemos añadido la dimensión 'country' para el desglose
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
        # Extraemos lo de ayer para ir acumulando sin duplicar
        date_ranges=[DateRange(start_date="yesterday", end_date="yesterday")],
    )
    
    response = client.run_report(request)
    
    values = []
    for row in response.rows:
        # row.dimension_values[0] es la fecha
        # row.dimension_values[1] es el país
        values.append([
            row.dimension_values[0].value, 
            row.dimension_values[1].value,
            row.metric_values[0].value, 
            row.metric_values[1].value
        ])
    return values

def append_to_sheets(data):
    if not data:
        print("No hay datos para añadir.")
        return

    creds = service_account.Credentials.from_service_account_info(credentials_info)
    service = build('sheets', 'v4', credentials=creds)
    
    # 'Hoja1!A1' busca la tabla y añade al final
    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range="Hoja1!A1", 
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={'values': data}
    ).execute()
    print(f"Se han añadido {len(data)} fila(s) con desglose por país.")

if __name__ == "__main__":
    datos = get_analytics_data()
    append_to_sheets(datos)
