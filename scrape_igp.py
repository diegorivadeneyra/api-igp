import json
import requests

def safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None

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
            attrs = f.get("attributes", {})
            sismo = {
                "id": attrs.get("objectid"),
                "fecha": attrs.get("fechaevento"),
                "hora": attrs.get("hora"),
                "magnitud": safe_float(attrs.get("magnitud")),
                "latitud": safe_float(attrs.get("lat")),
                "longitud": safe_float(attrs.get("lon")),
                "profundidad_km": safe_float(attrs.get("prof")),
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
