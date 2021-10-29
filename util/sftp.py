import json
import paramiko
import time
import util.properties as properties
from util.date_time_zone import get_date

folder_sftp = "IN/Rchzo/"

def connection_sftp(host, user, file):
    print("Iniciando conexión con sftp")
    
    try:

        external_t = paramiko.SSHClient()
        rsa_key = paramiko.RSAKey.from_private_key_file(file)
        external_t.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        time.sleep(0)
        external_t.connect(hostname=host,port=properties.SFTP_PORT_CLAI,username=user, password=None, pkey=rsa_key,timeout=3)
        sftp = external_t.open_sftp()
        return {
            "status": "success",
            "message": "message.sftp.connection",
            "messageDetail": "Éxito en conexión con el sftp",
            "sftp": sftp
        }
    except Exception as e:
        print(e)
        raise e
        external_t.close()
        print('Finalizando conexión')
        return {"status": "error", "message": "error.sftp.connection", "messageDetail":"Error en conexión con el sftp"}
        
def file_list(sftp):
    date = get_date()
    #date = "20210705"
    file_name = "ALG_ListRchzo_CC.ADQMC_" +str(date)

    print("Inicio de búsqueda de archivos para el día "+date)
    lista = sftp.listdir(folder_sftp)
    archivos_encontrados = []
    print("Lista de archivos ")
    for x in lista:
        if file_name in x:
            archivos_encontrados.append(x)
            print("Archivo encontrado :", x)
    print("Lista de archivos encontrados para el día ", archivos_encontrados)
    return archivos_encontrados
    

def read_file_sftp(sftp, file_name):
    print("Lectura de archivo")
    try:
        path = folder_sftp+file_name
        print("Path :", path)
        sftp_file = sftp.open(path, "r", bufsize=-1)
        
        file = sftp_file.read().decode('latin-1').splitlines(True)
        
    except Exception as e:
        print("Error ", e)
        file = None
    print("Fin de lectura de archivo")
    return file
        
        