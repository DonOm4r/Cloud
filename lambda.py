import json
import boto3
import uuid
s3 = boto3.client('s3')
def lambda_handler(event, context):
    bucket_name = 'equipo4' # Reemplazcon tu nombre de bucket
    file_name = f'data-stream-{uuid.uuid4()}.json'
    # Extraer datos del evento HTTP POST
    content = json.loads(event['body']) # Asumimos que los datosson enviados como JSON en el body
    # Guardar los datos en S3
    s3.put_object(Bucket=bucket_name, Key=file_name,
    Body=json.dumps(content))
    return {
    'statusCode': 200,
    'body': json.dumps(f'Datos recibidos y guardados en{file_name}')
}