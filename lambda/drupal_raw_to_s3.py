import os
import boto3
import requests
import datetime
import logging

# Config de logs
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

def lambda_handler(event, context):
    url = os.environ.get("API_URL")
    bucket = os.environ.get("RAW_BUCKET")
    
    # Validar env vars
    if not url or not bucket:
        logger.error("Faltan variables de entorno API_URL o RAW_BUCKET")
        return {"status": "error", "message": "Configuración incompleta"}

    try:
        # stream para no saturar RAM
        logger.info(f"Iniciando descarga desde {url}")
        
        # ruta temporal
        temp_file_path = "/tmp/data_download.json"
        
        with requests.get(url, stream=True, timeout=300) as r:
            r.raise_for_status()
            # Escribimos en disco x chunks
            with open(temp_file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
                    
        # Construimos la Key con Particionamiento Hive (Year/Month/Day)
        now = datetime.datetime.now(datetime.timezone.utc)
        partition_path = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
        file_name = f"tramites_{now.strftime('%H%M%S')}.json"
        
        # Ej: drupal/raw/tramites/year=2025/month=11/day=26/tramites_123000.json
        s3_key = f"drupal/raw/tramites/{partition_path}/{file_name}"

        # Upload
        logger.info(f"Subiendo a S3: s3://{bucket}/{s3_key}")
        s3.upload_file(
            Filename=temp_file_path,
            Bucket=bucket,
            Key=s3_key,
            ExtraArgs={"ContentType": "application/json"}
        )

        return {
            "status": "ok", 
            "bucket": bucket,
            "key": s3_key,
            "partition": partition_path
        }

    except requests.exceptions.Timeout:
        logger.error("Timeout en la API externa")
        return {"status": "error", "message": "API Timeout 5min"}
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error en request: {str(e)}")
        return {"status": "error", "message": str(e)}
        
    except Exception as e:
        logger.error(f"Error inesperado: {str(e)}")
        return {"status": "error", "message": f"Error interno: {str(e)}"}
