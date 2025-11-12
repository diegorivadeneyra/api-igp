import json
import requests
from decimal import Decimal

def lambda_handler(event, context):
    try:
        # Endpoint ArcGIS oficial del IGP
        url = "https://ide.igp.gob.pe/arcgis/rest/services/monitoreocensis/SismosReportados/MapServer/0/query"
        
        # Parámetros del query (últimos 10 sismos)
        params = {
            "where": "1=1",
            "outFields": "objectid,fechaevento,hora,magnitud,lat,lon,prof,ref,departamento",
            "orderByFields": "fechaevento DESC",
            "resultRecordCount": 10,
            "f": "json"
        }

        # Petición al servicio
        response = requests.get(url, params=params)
        response.raise_for_status()

        data = response.json()

        # Validar si hay datos
        if "features" not in data:
            return {
                "statusCode": 404,
                "body": json.dumps({"message": "No se encontraron datos de sismos en el servicio del IGP"})
            }

        # Extraer los sismos
        sismos = []
        for f in data["features"]:
            attrs = f["attributes"]
            sismo = {
                "id": attrs.get("objectid"),
                "fecha": attrs.get("fechaevento"),
                "hora": attrs.get("hora"),
                "magnitud": float(attrs.get("magnitud", 0)),
                "latitud": float(attrs.get("lat", 0)),
                "longitud": float(attrs.get("lon", 0)),
                "profundidad_km": float(attrs.get("prof", 0)),
                "referencia": attrs.get("ref"),
                "departamento": attrs.get("departamento")
            }
            sismos.append(sismo)

        # Respuesta exitosa
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Últimos sismos reportados por el IGP",
                "cantidad": len(sismos),
                "data": sismos
            }, default=str)
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
