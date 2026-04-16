import os
import json
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from googleapiclient.discovery import build
from google.oauth2 import service_account

# Cargar credenciales desde variables de entorno
credentials_info = json.loads(os.environ['GA_SERVICE_ACCOUNT_JSON'])
property_id = os.environ['GA_PROPERTY_ID']
spreadsheet_id = os.environ['SPREADSHEET_ID']

def get_analytics_data():
    client = BetaAnalyticsDataClient.from_service_account_info(credentials_info)
    
    # IMPORTANTE: Cambiamos el rango a "yesterday" para no duplicar datos del día en curso
    # cada vez que se ejecute la acción.
    request = RunReportRequest(
        property=f"properties/{property_id}",
        dimensions=[Dimension(name="date")],
        metrics=[Metric(name="activeUsers"), Metric(name="sessions")],
        date_ranges=[DateRange(start_date="yesterday", end_date="yesterday")],
    )
    response = client.run_report(request)
    
    values = []
    for row in response.rows:
        values.append([
            row.dimension_values[0].value, 
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
    
    # Usamos 'append' en lugar de 'update'
    # 'Hoja1!A1' le dice a Google que busque la tabla que empieza en A1 y añada al final
    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range="Hoja1!A1", 
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={'values': data}
    ).execute()
    print(f"Se han añadido {len(data)} fila(s) nuevas a Google Sheets.")

if __name__ == "__main__":
    datos = get_analytics_data()
    append_to_sheets(datos)
