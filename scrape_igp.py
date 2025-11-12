import requests
import boto3
import uuid
import json
from datetime import datetime

def lambda_handler(event, context):
    # URL oficial del servicio ArcGIS del IGP
    url = "https://ide.igp.gob.pe/arcgis/rest/services/monitoreocensis/SismosReportados/MapServer/0/query"

    # Parámetros para obtener los 10 últimos sismos
    params = {
        "where": "1=1",
        "outFields": "objectid,fechaevento,hora,magnitud,lat,lon,prof,ref,departamento",
        "orderByFields": "fechaevento DESC",
        "resultRecordCount": 10,
        "f": "json"
    }

    # Realizar la solicitud
    response = requests.get(url, params=params)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder al servicio del IGP'
        }

    data = response.json()

    if 'features' not in data:
        return {
            'statusCode': 500,
            'body': 'Respuesta inesperada del IGP'
        }

    # Procesar los datos
    sismos = []
    for feature in data['features']:
        a = feature['attributes']

        # Si no hay fecha, saltar este registro
        if not a.get('fechaevento'):
            continue

        # Convertir fecha (epoch ms → ISO 8601)
        try:
            fecha = datetime.utcfromtimestamp(a['fechaevento'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            fecha = "N/A"

        sismos.append({
            'id': str(uuid.uuid4()),
            'objectid': str(a.get('objectid', '')),
            'fechaevento': fecha,
            'hora': a.get('hora', ''),
            'magnitud': str(a.get('magnitud', '')),
            'lat': a.get('lat', 0),
            'lon': a.get('lon', 0),
            'profundidad_km': a.get('prof', 0),
            'referencia': a.get('ref', ''),
            'departamento': a.get('departamento', '')
        })


    # Guardar en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('UltimosSismos')

    # Eliminar datos anteriores
    scan = table.scan()
    with table.batch_writer() as batch:
        for item in scan.get('Items', []):
            batch.delete_item(Key={'id': item['id']})

    # Insertar nuevos registros
    with table.batch_writer() as batch:
        for s in sismos:
            batch.put_item(Item=s)

    return {
        'statusCode': 200,
        'body': json.dumps({
            'mensaje': 'Últimos 10 sismos guardados correctamente',
            'cantidad': len(sismos),
            'datos': sismos
        }, ensure_ascii=False)
    }

