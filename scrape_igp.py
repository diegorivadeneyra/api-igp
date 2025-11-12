import requests
from bs4 import BeautifulSoup
import boto3
import uuid
from decimal import Decimal

def lambda_handler(event, context):
    url = "https://ultimosismo.igp.gob.pe/ultimosismo/sismos-reportados"
    response = requests.get(url)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder al sitio del IGP'
        }

    soup = BeautifulSoup(response.text, 'html.parser')

    # Buscar la tabla principal de sismos
    table = soup.find('table')
    if not table:
        return {
            'statusCode': 404,
            'body': 'No se encontró la tabla de sismos en el sitio del IGP'
        }

    # Obtener encabezados
    headers = [th.text.strip() for th in table.find_all('th')]

    # Obtener filas
    rows = []
    for tr in table.find_all('tr')[1:11]:  # Solo los 10 primeros
        tds = tr.find_all('td')
        if len(tds) < len(headers):
            continue

        row_data = {}
        for i, td in enumerate(tds):
            header = headers[i]
            value = td.text.strip()

            # Intentar convertir valores numéricos a Decimal
            try:
                if header.lower() in ['latitud', 'longitud', 'magnitud', 'profundidad (km)']:
                    value = Decimal(str(value))
            except:
                pass

            row_data[header] = value

        row_data['id'] = str(uuid.uuid4())
        rows.append(row_data)

    # Guardar en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('TablaWebScrappingIGP')

    # Vaciar antes de insertar (opcional)
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan.get('Items', []):
            batch.delete_item(Key={'id': each['id']})

    # Insertar los nuevos
    with table.batch_writer() as batch:
        for row in rows:
            batch.put_item(Item=row)

    return {
        'statusCode': 200,
        'body': rows
    }
