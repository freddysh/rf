import boto3
import tempfile

def downloadIpmFileFromAwsS3(fileName, bucket, region):
    print("Iniciando descarga del archivo ", fileName)
    file = None
    tmp_file = None
    try:
        s3 = boto3.client('s3', region_name=region)
        bcClai = bucket.split("/")
        bucket = bcClai.pop(0)
        tmp_file = tempfile.NamedTemporaryFile(delete=False)
        s3.download_fileobj(bucket, "/".join(bcClai) + "/" + fileName,
                            tmp_file)
        file = tmp_file.name
        print("Proceso de descarga finalizado de manera exitosa. ", fileName)
    except Exception as e:
        print("Error", e)
    finally:
        if (tmp_file is not None):
            tmp_file.close()
    return file