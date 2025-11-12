import json
import requests
import boto3
from datetime import datetime
from decimal import Decimal

# Inicializar el cliente de DynamoDB
dynamodb = boto3.resource('dynamodb')
TABLE_NAME = "SismosIGP"

def safe_decimal(value):
    """Convierte a Decimal o devuelve None"""
    try:
        if value is None:
            return None
        return Decimal(str(value))
    except:
        return None

def format_fecha(ms):
    """Convierte milisegundos a fecha legible"""
    try:
        if ms:
            return datetime.utcfromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        pass
    return None

def lambda_handler(event, context):
    try:
        # Endpoint ArcGIS del IGP
        url = "https://ide.igp.gob.pe/arcgis/rest/services/monitoreocensis/SismosReportados/MapServer/0/query"

        params = {
            "where": "1=1",
            "outFields": "objectid,fechaevento,hora,magnitud,lat,lon,prof,ref,departamento",
            "orderByFields": "fechaevento DESC",
            "resultRecordCount": 10,
            "f": "json"
        }

        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()

        if "features" not in data:
            return {
                "statusCode": 404,
                "body": json.dumps({"message": "No se encontraron datos de sismos en el servicio del IGP"})
            }

        table = dynamodb.Table(TABLE_NAME)
        sismos = []

        # Guardar los sismos
        with table.batch_writer() as batch:
            for f in data["features"]:
                attrs = f.get("attributes", {})

                sismo = {
                    "id": str(attrs.get("objectid")),
                    "fecha": format_fecha(attrs.get("fechaevento")),
                    "hora": attrs.get("hora"),
                    "magnitud": safe_decimal(attrs.get("magnitud")),
                    "latitud": safe_decimal(attrs.get("lat")),
                    "longitud": safe_decimal(attrs.get("lon")),
                    "profundidad_km": safe_decimal(attrs.get("prof")),
                    "referencia": attrs.get("ref"),
                    "departamento": attrs.get("departamento")
                }

                batch.put_item(Item=sismo)
                sismos.append(sismo)

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Últimos sismos guardados en DynamoDB correctamente",
                "cantidad": len(sismos),
                "data": sismos
            }, ensure_ascii=False)
        }

    except requests.RequestException as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Error al conectar con el IGP: {str(e)}"})
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Error inesperado: {str(e)}"})
        }
