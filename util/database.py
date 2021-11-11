import psycopg2
# import properties as properties
# from date_time_zone import get_date_time
import util.properties as properties

class pendingpayment:
    def __init__(self, idspecial_purchasereturn, vc_numeropedido, vc_idcomercio, dt_fecha_trx):
        self.idspecial_purchasereturn = idspecial_purchasereturn
        self.vc_numeropedido = vc_numeropedido
        self.vc_idcomercio = vc_idcomercio
        self.dt_fecha_trx = dt_fecha_trx

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
    # query = "SELECT IN_IDPENDINGPAYMENT, VC_ORDERNUMBER, IN_IDECOMMERCE, DT_TRANSACTION  FROM SQMBOX.TBOX_PENDINGPAYMENT WHERE CH_ERRORREVERSE = '" + properties.ESTADO_PENDINGPAYMENT_REGISTTRADO + "'"
    # obtenemos el agrupador
    # query_agrupador = "select max(in_agrupador) from sqmbox.tbox_special_purchasereturn"
    # cursor.execute(query_agrupador)
    # agrupador=cursor.fetchone()
    query="""select
                    in_idspecial_purchasereturn,
                    vc_numeropedido,
                    vc_idcomercio,
                    dt_fecha_trx
                from
                    sqmbox.tbox_special_purchasereturn
                where
                    in_state = {0}
                    """.format(properties.ESTADO_LIQUIDADO)

    # query = "SELECT IN_IDPENDINGPAYMENT, VC_ORDERNUMBER, IN_IDECOMMERCE, DT_TRANSACTION  FROM SQMBOX.TBOX_PENDINGPAYMENT WHERE CH_ERRORREVERSE = '" + properties.ESTADO_PENDINGPAYMENT_REGISTTRADO + "'"
    pendingpayment_list = []
    cursor.execute(query)
    connection.commit()
    try:
        transaction_list = cursor.fetchall()
        print("registros {0}".format(transaction_list))
        for tx in transaction_list:
            pendingpayment_list.append( pendingpayment(tx[0],tx[1],tx[2],tx[3]))
    except:
        transaction_list = None
        connection.rollback()
        
    print("Fin de búsqueda de todas las transacciones que se enviaron a Clai lista=" , pendingpayment_list )
    return pendingpayment_list

def insert_devolution(connection, cursor, ordernumber, idecommerce, date, idspecial_purchasereturn):
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
            # query = "UPDATE SQMBOX.TBOX_PENDINGPAYMENT SET CH_ERRORREVERSE = '"+ properties.ESTADO_PENDINGPAYMENT_PROCESADO +"' WHERE IN_IDPENDINGPAYMENT = "+ str(idpendingpayment) 
            query = "sqmbox.tbox_special_purchasereturn set in_state = '"+ properties.ESTADO_DEVOLUCION +"' WHERE in_idspecial_purchasereturn = "+ str(idspecial_purchasereturn) 
            cursor.execute(query)
            connection.commit()
            result = 1
        
    except Exception as e:
        connection.rollback()
        raise e
    
    return result

def update_pendingpayment_error(connection, cursor, idspecial_purchasereturn):
    result = 0
    try:

        query="""update sqmbox.tbox_special_purchasereturn set 
                in_state = {0} 
                where 
                    in_idspecial_purchasereturn = {1}""".format(properties.ESTADO_DEVLUCION_ERROR,idspecial_purchasereturn)
        # query = "UPDATE SQMBOX.TBOX_PENDINGPAYMENT SET CH_ERRORREVERSE = '"+ properties.ESTADO_PENDINGPAYMENT_NO_COMPLETADO +"' WHERE IN_IDPENDINGPAYMENT = "+ str(idpendingpayment) 
        print(query)
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