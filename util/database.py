import psycopg2
import util.properties as properties
from util.date_time_zone import get_date_time

class pendingpayment:
    def __init__(self, idpendingpayment, vc_ordernumber, in_idecommerce, date_trx):
        self.idpendingpayment = idpendingpayment
        self.ordernumber = vc_ordernumber
        self.idecommerce = in_idecommerce
        self.date_trx = date_trx


def connect_database():
    connection = None
    cursor = None
    try:
        connection = psycopg2.connect(
            user=properties.DATABASE_USER,
            password=properties.DATABASE_PASSWORD,
            host=properties.DATABASE_HOST,
            port=properties.DATABASE_PORT,
            database=properties.DATABASE_NAME,
            connect_timeout=properties.DATABASE_TIMEOUT)
        cursor = connection.cursor()
    except Exception as e:
        print("No se pudo conectar a postgres")
    return connection, cursor

def clai_configuration(connection, cursor):
    print("Recuperar variables de configuración de CLAI de base de datos")
    hostClai = pathClai = userClai = bucketClai = regionClai = keyClai = None
    try:
        hostClai = obtain_properties(connection, cursor,
                                     properties.SFTP_IP_CLAI)
        pathClai = obtain_properties(connection, cursor,
                                     properties.SFTP_PATH_REPORTS_CLAI)
        userClai = obtain_properties(connection, cursor,
                                     properties.SFTP_USER_CLAI)
        bucketClai = obtain_properties(connection, cursor,
                                       properties.S3_BUCKET_CLAI)
        regionClai = obtain_properties(connection, cursor,
                                       properties.S3_REGION_CLAI)
        keyClai = obtain_properties(connection, cursor,
                                    properties.S3_KEY_NAME_CLAI)
    except Exception as e:
        print("No se pudo recuperar variables de configuración de CLAI")
        print("Error", e)
    return hostClai, pathClai, userClai, bucketClai, regionClai, keyClai

def obtain_properties(connection, cursor, key):
    value = None
    try:
        cursor.execute(
            "select vc_value from sqmpp.tpp_resourcevalue tp where tp.vc_key = %s",
            (key, ))
        connection.commit()
        vl = cursor.fetchone()
        if vl is not None:
            value = vl[0]
    except Exception as e:
        connection.rollback()
        raise e
    return value

def select_transactions(connection, cursor):
    print("Inicio de búsqueda de todas las transacciones que se enviaron a Clai")
    query = "SELECT IN_IDPENDINGPAYMENT, VC_ORDERNUMBER, IN_IDECOMMERCE, DT_TRANSACTION  FROM SQMBOX.TBOX_PENDINGPAYMENT WHERE CH_ERRORREVERSE = '" + properties.ESTADO_PENDINGPAYMENT_REGISTTRADO + "'"
    pendingpayment_list = []
    cursor.execute(query)
    connection.commit()
    try:
        transaction_list = cursor.fetchall()
        for tx in transaction_list:
            pendingpayment_list.append( pendingpayment(tx[0],tx[1],tx[2],tx[3]))
    except:
        transaction_list = None
        connection.rollback()
        
    print("Fin de búsqueda de todas las transacciones que se enviaron a Clai lista=" , pendingpayment_list )
    return pendingpayment_list

def insert_devolution(connection, cursor, ordernumber, idecommerce, date, idpendingpayment):
    result = 0
    try:

        insert_query = " INSERT INTO sqmbox.tbox_devoluciones(vc_numeropedido,vc_idcomercio,in_tipo,dt_fecha_trx) "
        insert_query = insert_query + " VALUES('" + ordernumber + "','" + str(idecommerce) + "'," + properties.IN_TIPO_DEVOLUCION_TOTAL + ",'" + date.strftime("%Y-%m-%d %H:%M:%S") + "')"

        cursor.execute(insert_query)
        connection.commit()

        if not (cursor.statusmessage == "INSERT 0 1"):
            print("No se pudo insertar el registro")
            result = 0
        else:
            query = "UPDATE SQMBOX.TBOX_PENDINGPAYMENT SET CH_ERRORREVERSE = '"+ properties.ESTADO_PENDINGPAYMENT_PROCESADO +"' WHERE IN_IDPENDINGPAYMENT = "+ str(idpendingpayment) 
            cursor.execute(query)
            connection.commit()
            result = 1
        
    except Exception as e:
        connection.rollback()
        raise e
    
    return result

def update_pendingpayment_error(connection, cursor, idpendingpayment):
    result = 0
    try:
        query = "UPDATE SQMBOX.TBOX_PENDINGPAYMENT SET CH_ERRORREVERSE = '"+ properties.ESTADO_PENDINGPAYMENT_NO_COMPLETADO +"' WHERE IN_IDPENDINGPAYMENT = "+ str(idpendingpayment) 
        cursor.execute(query)
        connection.commit()
        if not (cursor.statusmessage=='UPDATE 1'):
            print("No se pudo actualizar el registro ")
            result = 0
        else :
            result = 1
    except Exception as e:
        connection.rollback()
        raise e
    
    return result