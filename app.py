# -*- coding: utf-8 -*-
"""
Requires Python 3.0 or later
"""

__author__ = "Jorge Morfinez Mojica (jorgemorfinez@ofix.mx)"
__copyright__ = "Copyright 2019, Jorge Morfinez Mojica"
__license__ = "Ofix S.A. de C.V."
__history__ = """ """
__version__ = "1.20.D10.1.2.1 ($Rev: 10 $)"


from constants.constants import Constants as Const
# import paramiko
import fnmatch
import boto3
from ftplib import FTP_TLS
import argparse
from logger_controller.logger_control import *

logger = configure_logger()


# Conecta al FTP B2B de Tecnofin
def ftp_orders_b2b_tecnofin_connector():

    cfg = get_config_constant_file()

    remote_host = cfg['TECNOFIN_ACCESS_B2B']['HOST']
    remote_port = cfg['TECNOFIN_ACCESS_B2B']['PORT']
    remote_username = cfg['TECNOFIN_ACCESS_B2B']['USERNAME']
    remote_password = cfg['TECNOFIN_ACCESS_B2B']['PASSWORD']
    remote_timeout = cfg['TECNOFIN_ACCESS_B2B']['TIME_OUT']

    ftps = FTP_TLS(remote_host)

    ftps.set_debuglevel(2)
    ftps.set_pasv(True)
    ftps.connect(port=remote_port, timeout=remote_timeout)

    ftps.login(remote_username, remote_password)

    logger.info('FTP Connected to: %s', remote_username+'@'+str(remote_host))

    ftps.prot_p()

    return ftps


# Conecta al FTP B2C de Tecnofin
def ftp_orders_b2c_tecnofin_connector():

    cfg = get_config_constant_file()

    remote_host = cfg['TECNOFIN_ACCESS_B2C']['HOST']
    remote_port = cfg['TECNOFIN_ACCESS_B2C']['PORT']
    remote_username = cfg['TECNOFIN_ACCESS_B2C']['USERNAME']
    remote_password = cfg['TECNOFIN_ACCESS_B2C']['PASSWORD']
    remote_timeout = cfg['TECNOFIN_ACCESS_B2C']['TIME_OUT']

    ftps = FTP_TLS(remote_host)

    ftps.set_debuglevel(2)
    ftps.set_pasv(True)
    ftps.connect(port=remote_port, timeout=remote_timeout)

    ftps.login(remote_username, remote_password)

    logger.info('FTP Connected to: %s', remote_username+'@'+str(remote_host))

    ftps.prot_p()

    return ftps


# Parsea todos los XML de pedidos pendientes
# para insertarlos mediante API de Pedidos
def parse_xml_pedidos_b2c_tv(order_type):

    import os

    cfg = get_config_constant_file()

    # remote_backup_path = '/ofix/tecnofin/pedidosBk/'

    if 'B2C' in order_type:

        remote_path = cfg['PATH_ORDERS_B2C']
        local_temp_path = cfg['PATH_LOCAL']
        pattern = cfg['EXT_ORDERS_TV']

        ftps = ftp_orders_b2c_tecnofin_connector()

        ftps.cwd(remote_path)

        pedidos = ftps.nlst()

        # ssh = connect_to_ftp_pedidos()

        # sftp = ssh.open_sftp()

        # pedidos = sftp.listdir(remote_path)

        file_remote = None
        file_local = None

        for pedido in pedidos:
            # print all entries that are files
            if fnmatch.fnmatch(pedido, pattern):
                file_remote = remote_path + '/' + pedido
                file_local = local_temp_path + '/' + pedido

                pedido_s3_exists = validate_order_exists_s3(pedido)

                logger.info('Pedido Order Exists in S3: %s',
                            'Pedido: {0} ¿exists?: {1}'.format(pedido,
                                                               pedido_s3_exists))

                if pedido_s3_exists is False:

                    logger.info('Server File >>> ' + file_remote + ' : ' + file_local + ' <<< Local File')

                    ftps.retrbinary('RETR %s' % file_remote, open(file_local, 'wb').write)

                    # sftp.get(file_remote, file_local) # NOT USE IT

                    logger.info('Local File Pedido was created: %s', str(file_local))

                    copy_order_to_aws_s3(pedido)

                    # If file exists, delete it ##
                    if os.path.isfile(file_local):
                        os.remove(file_local)
                        logger.info('Local File Pedido was deleted: %s', str(file_local))
                    else:    # Show an error ##
                        logger.error("Error: %s file not found" % file_local)

                else:
                    logger.info('Pedido File: %s', '{0} already exists in Bucket S3!'.format(pedido))

                ftps.delete(pedido)

        ftps.close()

    elif 'B2B' in order_type:

        remote_path = cfg['PATH_ORDERS_B2B']
        local_temp_path = cfg['PATH_LOCAL']
        pattern = cfg['EXT_ORDERS_TV']

        ftps = ftp_orders_b2b_tecnofin_connector()

        ftps.cwd(remote_path)

        pedidos = ftps.nlst()

        for pedido in pedidos:
            # print all entries that are files
            if fnmatch.fnmatch(pedido, pattern):
                file_remote = remote_path + '/' + pedido
                file_local = local_temp_path + '/' + pedido

                pedido_s3_exists = validate_order_exists_s3(pedido)

                logger.info('Pedido Order Exists in S3: %s',
                            'Pedido: {0} ¿exists?: {1}'.format(pedido,
                                                               pedido_s3_exists))

                if pedido_s3_exists is False:

                    logger.info('Server File >>> ' + file_remote + ' : ' + file_local + ' <<< Local File')

                    ftps.retrbinary('RETR %s' % file_remote, open(file_local, 'wb').write)

                    # sftp.get(file_remote, file_local) # NOT USE IT

                    logger.info('Local File Pedido was created: %s', str(file_local))

                    copy_order_to_aws_s3(pedido)

                    # If file exists, delete it ##
                    if os.path.isfile(file_local):
                        os.remove(file_local)
                        logger.info('Local File Pedido was deleted: %s', str(file_local))
                    else:    # Show an error ##
                        logger.error("Error: %s file not found" % file_local)

                else:
                    logger.info('Pedido File: %s', '{0} already exists in Bucket S3!'.format(pedido))

        ftps.close()


# Contiene el codigo para conectar Bucket AWS de S3
# y subir el archivo del pedido:
def copy_order_to_aws_s3(pedido):

    cfg = get_config_constant_file()

    bucket_s3_name = cfg['BUCKET_AWS_S3']['S3_NAME']

    s3_access_key = cfg['BUCKET_AWS_S3']['ACCESS_KEY']
    s3_secret_key = cfg['BUCKET_AWS_S3']['SECRET_KEY']

    bucketname = bucket_s3_name

    logger.info('Bucket S3 to Upload Order file: %s', bucketname)
    logger.info('Order Pedido file to upload: %s', pedido)

    s3 = boto3.resource('s3', aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key)
    # s3.Bucket(bucketname).upload_file(filename, '/home/ubuntu/environment/ordersS3Uploader/Order-12630.xml')
    s3.Object(bucketname, pedido).upload_file(Filename=pedido)

    logger.info('Order Pedido file uploaded: %s', pedido)


# Conecta a AWS S3 para descargar y leer cada pedido XML
def connect_aws_s3():

    cfg = get_config_constant_file()

    bucket_s3_name = cfg['BUCKET_AWS_S3']['S3_NAME']

    s3_access_key = cfg['BUCKET_AWS_S3']['ACCESS_KEY']
    s3_secret_key = cfg['BUCKET_AWS_S3']['SECRET_KEY']

    bucketname = bucket_s3_name

    s3 = boto3.resource('s3', aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key)
    # s3.Bucket(bucketname).upload_file(filename, '/home/ubuntu/environment/ordersS3Uploader/Order-12630.xml')

    bucket_pedidos = s3.Bucket(bucketname)

    # s3.Object(bucketname, pedido).upload_file(Filename=pedido)

    return bucket_pedidos


def validate_order_exists_s3(pedido_order):

    pedido_s3_exists = False

    bucket_pedidos = connect_aws_s3()

    logger.info('Pedido to validate in S3: %s', str(pedido_order))

    for pedido in bucket_pedidos.objects.all():
        order_name = pedido.key

        logger.info('File Pedido into S3 Bucket: %s', str(order_name))

        if str(pedido_order) in str(order_name):
            pedido_s3_exists = True
        else:
            pedido_s3_exists = False

    return pedido_s3_exists


# Define y obtiene el configurador para las constantes del sistema:
def get_config_constant_file():
    """Contiene la obtencion del objeto config
        para setear datos de constantes en archivo
        configurador

    :rtype: object
    """
    # TEST
    _constants_file = "constants/constants.yml"

    # PROD
    # _constants_file = '/home/jorge/Documents/Projects/tecnofinLayouts/projects/PaginaB2COFIXNORMAL/' \
    #                  'ordersTVParser/constants/constants.yml'
    cfg = Const.get_constants_file(_constants_file)

    return cfg


def main():
    pass

    parser = argparse.ArgumentParser()

    parser.add_argument('--order_type', required=True, type=str,
                        help="Parametro Tipo de Orden B2C o B2B entre comillas")

    args = parser.parse_args()

    order_type = args.order_type

    logger.info('ORDER_TYPE ARG: %s', str(order_type))

    parse_xml_pedidos_b2c_tv(order_type)
    # ftp_orders_tecnofin_connector()


if __name__ == "__main__":
    pass

    main()


'''
# NO USE IT - Connect to SFTP not FTP over TLS
# noinspection PyGlobalUndefined
def connect_to_ftp_pedidos():

    ssh = None

    cfg = get_config_constant_file()

    remote_host = cfg['WEBAPPS_ACCESS']['HOST']
    remote_port = cfg['WEBAPPS_ACCESS']['PORT']
    remote_username = cfg['WEBAPPS_ACCESS']['USERNAME']
    remote_password = cfg['WEBAPPS_ACCESS']['PASSWORD']

    ssh = paramiko.SSHClient()

    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:

        ssh.connect(hostname=remote_host,
                    port=remote_port,
                    username=remote_username,
                    password=remote_password,
                    timeout=300,
                    banner_timeout=300,
                    allow_agent=False,
                    look_for_keys=False)

        chan = ssh.invoke_shell()
        resp = chan.recv(9999)
        print(resp)

    except paramiko.ssh_exception.SSHException as sshe:
        # socket is open, but not SSH service responded
        # if e.message == 'Error reading SSH protocol banner':

        print('SSH transport is available! ', sshe)

    except paramiko.ssh_exception.NoValidConnectionsError as nvce:
        print('SSH transport is not ready... ', nvce)

    return ssh
'''