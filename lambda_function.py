import json
import csv
from typing import Any
import psycopg2
from util.database import connect_database
from util.database import insert_devolution
from util.database import select_transactions
from util.database import update_pendingpayment_error
from util.database import clai_configuration
from util.sftp import connection_sftp
from util.sftp import read_file_sftp
from util.sftp import file_list
from util.s3_client import downloadIpmFileFromAwsS3
 

def lambda_handler(event, context):
    # TODO implement
    
    Headers_CSV = ["Tipo Entidad ","Razon Social Entidad ","Nombre Red: ","Arch.Pres ","Arch.Comp ",
                    "Nombre del Facilitador ","N. Documento Facilitador ","Nombre Comercio ","RAZON_SOCIAL_COMERCIO ","TIPO_DOCUMENTO_COMERCIO ",
                    "NRO_DOCUMENTO_COMERCIO ","ID_COMERCIO ", "Codigo Comercio ","Canal ","MONEDA ",
                    "Metodo Pago ","LOTE ","Codigo Operación ","Tipo Operación ","Mensaje Operación ",
                    "Fecha Operación ","ID Trx ","NRO_PEDIDO ","COD_AUTORIZACION ","NRO_TARJETA ",
                    "Origen Tarjeta ","Banco ","Emisor CSI ","Plan CSI ","Cuota CSI ",
                    "Comision CSI ","Fecha Proceso ","IMPORTE_PROCESADO ","Fecha Procesamiento Pres.","Fecha Procesamiento Comp.",
                    "Importe Compensado ","Tipo Conciliación ","Desc T. Conciliación ","Fecha Conciliación ","Hora Conciliación "]


    connection, cursor = connect_database()

    if cursor is None or connection is None:
        return {
            "status": "error",
            "message": "error.postgres.connection",
            "messageDetail": "Error en conexión con postgres"
        }

    # Get CLAI configuration
    hostClai, pathClai, userClai, bucketClai, regionClai, keyClai = clai_configuration(
        connection, cursor)
    if hostClai is None or pathClai is None or userClai is None or bucketClai is None or regionClai is None or keyClai is None:
        return {
            "status": "error",
            "message": "error.clai.configuration",
            "messageDetail": "No se pudo obetener configuración CLAI"
        }
    
    print("""hostClai={0}, pathClai={1}, userClai={2}, bucketClai={3}, regionClai={4}, keyClai={5}""".format(hostClai, pathClai, userClai, bucketClai, regionClai, keyClai))
    # Connect S3 CLAI and download rsa key
    
    # rsaClai = downloadIpmFileFromAwsS3(keyClai, bucketClai, regionClai)
    # if rsaClai is None:
    #     return {
    #         "status": "error",
    #         "message": "error.s3.connection",
    #         "messageDetail": "Error en conexión con s3"
    #     }

    # Connect SFTP
    # response_remote = connection_sftp(hostClai,
    #                                   userClai, rsaClai)
    response_remote = connection_sftp("3.234.195.134",
                                      "clai", "sftp.dev.clai-id_rsa")
    json_response = {
        "status":False,
        "Archivos":[],
        "Mensaje": ""
    }
    
    print("Resultado sftp:", response_remote)
    
    if response_remote['status'] == "error":
        return response_remote
    
    sftp = get_response_item(response_remote,'sftp')
    # lista todod los archivos del dia
    daily_list = file_list(sftp)
    
    if len(daily_list) == 0:
        return {"status": "error", "message": "error.lambda_handler.file_list", "messageDetail":"No se encontró archivos a procesar"}

    transaction_list = select_transactions(connection, cursor)
    # print("litado:",transaction_list)
    # raise Exception("new breack")
    if len(transaction_list) == 0:
        print("No se encontro transacciones con estado REGISTRADO")
        return {"status": "error", "message": "error.transaction.status", "messageDetail":"No se encontró transacciones con estado liquidado en BD"}
    
    list_file = []

    # raise Exception("new breack")
    # Filter files with 0 transaction 
    for file in daily_list:

        data_original = None
        data_original = read_file_sftp(sftp, file) 
        NumberRow = 7 
        if len(data_original) > NumberRow :
            row = data_original[NumberRow]
            filas = row.split(",")       

            if len(filas[0]) > 1:
                list_file.append(file)

    if(len(list_file) == 0) :
        # for  item in transaction_list :
        #     insert_devolution(connection, cursor, item.vc_numeropedido, item.vc_idcomercio, item.dt_fecha_trx, item.idspecial_purchasereturn)
        return {"status": "error", "message": "error.file.connection", "messageDetail":"No se encontró registros dentro de los archivos(nro:"+str(len(list_file))+") a procesar"}

    count_devolution = 0
    count_rejected = 0
    count = 0

    for item_payment in transaction_list:
        readListFile = True
        IndexFile = 0
        while readListFile and IndexFile<len(list_file) :
            file = list_file[IndexFile]
            data_original = ""
            # Number row begins information 
            NumberRow = 7 
            filas = []
            data_original = None
            data_original = read_file_sftp(sftp, file)
            readFile = True

            #while rows in data_original:
            while readFile:

                row = data_original[NumberRow]
                filas = row.split(",")

                if len(filas[0]) > 1:                
                    
                    row_idecommerce = filas[22].replace("'","")
                    row_ordernumber = filas[28].replace("'","")

                    if item_payment.vc_numeropedido == row_ordernumber and str(item_payment.vc_idcomercio) == row_idecommerce :                                    
                        count = update_pendingpayment_error(connection, cursor,item_payment.idspecial_purchasereturn)
                        count_rejected += count
                        readFile = False
                        readListFile = False
                else:
                    readFile = False 

                NumberRow += 1
            
            IndexFile += 1
            # if ( readListFile and IndexFile == len(list_file) ):
            #     count = insert_devolution(connection, cursor, item_payment.ordernumber, item_payment.idecommerce, item_payment.date_trx, item_payment.idspecial_purchasereturn)
            #     count_devolution += count
            #     readListFile = False

            #json_response['Archivos'].append(json_file)
    
    json_response["Archivos"] = list_file

    if count_devolution + count_rejected == len(transaction_list) :
        json_response["status"] = True
        json_response["Mensaje"] = "Se procesaron " + str(count_devolution) + " registros para devoluciones y " + str(count_rejected) + " registros como rechazados de un total de " + str(len(transaction_list))
    else :
        json_response["status"] = False
        json_response["Mensaje"] = "Se procesaron " + str( count_devolution + count_rejected ) + " registros  de un total de " + str(len(transaction_list))


    print("Finaliza el proceso : ", json.dumps(json_response))
    return json_response

def get_response_item(response, item):
    for key , value in response.items():
        if key == item:
            x_item = value
    return x_item

if __name__ == "__main__":
    a = lambda_handler(None, None)
    print("\n")
    print(json.dumps(a, indent=4, sort_keys=True, ensure_ascii=False))