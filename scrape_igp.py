from datetime import datetime
from decimal import Decimal
import requests
import boto3
import uuid

def lambda_handler(event, context):
    url = "https://ultimosismo.igp.gob.pe/ultimosismo/sismos-reportados"
    response = requests.get(url)
    if response.status_code != 200:
        return {'statusCode': response.status_code, 'body': 'Error al acceder al servicio del IGP'}

    data = response.json()
    features = data.get('features', [])

    sismos = []
    for feature in features[:10]:  # Solo los 10 últimos
        a = feature.get('attributes', {})
        if not a.get('fechaevento'):
            continue

        try:
            fecha = datetime.utcfromtimestamp(a['fechaevento'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            fecha = "N/A"

        # Conversión segura de floats a Decimal
        def safe_decimal(x):
            try:
                return Decimal(str(x))
            except:
                return Decimal('0')

        sismos.append({
            'id': str(uuid.uuid4()),
            'objectid': str(a.get('objectid', '')),
            'fechaevento': fecha,
            'hora': a.get('hora', ''),
            'magnitud': safe_decimal(a.get('magnitud', 0)),
            'lat': safe_decimal(a.get('lat', 0)),
            'lon': safe_decimal(a.get('lon', 0)),
            'profundidad_km': safe_decimal(a.get('prof', 0)),
            'referencia': a.get('ref', ''),
            'departamento': a.get('departamento', '')
        })

    # Guardar los datos en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaWebScrappingIGP')

    with table.batch_writer() as batch:
        for sismo in sismos:
            batch.put_item(Item=sismo)

    return {
        'statusCode': 200,
        'body': sismos
    }
