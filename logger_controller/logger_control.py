# -*- coding: utf-8 -*-
"""
    Requires Python 3.x or later
"""

__author__ = "Jorge Morfinez Mojica (jorgemorfinez@ofix.mx)"
__copyright__ = "Copyright 2019, Jorge Morfinez Mojica"
__license__ = "Ofix S.A. de C.V."
__history__ = """ """
__version__ = "1.20.D10.1.1.0 ($Rev: 5 $)"

import datetime
import errno
import logging
import os
import sys
from constants.constants import Constants as Const
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))


# Possible values here are DEBUG, INFO, WARN, ERROR and CRITICAL
# LOG_LEVEL = logging.DEBUG


# Para LOG de cliente BD:
def configure_db_logging(log_name, path_to_log_directory):
    """
    Configure logger
    :param log_name:
    :param path_to_log_directory:  path to directory to write log file in
    :return:
    """

    cfg = get_config_constant_file()

    # log_filename = log_name + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '.log'
    _date_name = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M')
    log_filename = log_name + _date_name + 'hr' + cfg['LOG_RESOURCE']['FILE_EXTENSION']
    _importer_logger = logging.getLogger('db')
    _importer_logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - Module: %(module)s - Line No: %(lineno)s : %(name)s : %(levelname)s - '
                                  '%(message)s')

    fh = logging.FileHandler(filename=os.path.join(path_to_log_directory, log_filename), mode='w+',
                             encoding='utf-8', delay=False)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    # fh.addFilter(type('', (logging.Filter,), {'filter': staticmethod(lambda r: r.levelno <= logging.INFO)}))
    _importer_logger.addHandler(fh)

    # _importer_logger = logging.basicConfig(filename=os.path.join(path_to_log_directory, log_filename),
    #                                        filemode='w+', format=formatter, level=logging.DEBUG)

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.DEBUG)
    sh.setFormatter(formatter)
    # sh.addFilter(type('', (logging.Filter,), {'filter': staticmethod(lambda r: r.levelno <= logging.INFO)}))
    _importer_logger.addHandler(sh)

    create_directory_if_not_exists(_importer_logger, path_to_log_directory)

    return _importer_logger


# Para LOG del WS:
def configure_ws_logging(log_name, path_to_log_directory):
    """
    Configure logger
    :param log_name:
    :param path_to_log_directory:  path to directory to write log file in
    :return:
    """

    cfg = get_config_constant_file()

    # log_filename = log_name + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '.log'
    _date_name = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M')
    log_filename = log_name + _date_name + 'hr' + cfg['LOG_RESOURCE']['FILE_EXTENSION']

    _importer_logger = logging.getLogger('ws')
    _importer_logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - Module: %(module)s - Line No: %(lineno)s : %(name)s : %(levelname)s - '
                                  '%(message)s')

    fh = logging.FileHandler(filename=os.path.join(path_to_log_directory, log_filename), mode='w+',
                             encoding='utf-8', delay=False)

    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    # fh.addFilter(type('', (logging.Filter,), {'filter': staticmethod(lambda r: r.levelno <= logging.INFO)}))
    _importer_logger.addHandler(fh)

    # _importer_logger = logging.basicConfig(filename=os.path.join(path_to_log_directory, log_filename),
    #                                        filemode='w+', format=formatter, level=logging.DEBUG)

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.DEBUG)
    sh.setFormatter(formatter)
    # sh.addFilter(type('', (logging.Filter,), {'filter': staticmethod(lambda r: r.levelno <= logging.INFO)}))
    _importer_logger.addHandler(sh)

    create_directory_if_not_exists(_importer_logger, path_to_log_directory)

    return _importer_logger


# Para App Principal
def configure_logging(log_name, path_to_log_directory):
    """
    Configure logger
    :param log_name:
    :param path_to_log_directory:  path to directory to write log file in
    :return:
    """

    cfg = get_config_constant_file()

    # log_filename = log_name + datetime.datetime.now().strftime('%Y-%m-%d') + '.log'
    _date_name = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M')
    log_filename = log_name + _date_name + 'hr' + cfg['LOG_RESOURCE']['FILE_EXTENSION']

    _importer_logger = logging.getLogger('root')
    _importer_logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - Module: %(module)s - Line No: %(lineno)s : %(name)s : %(levelname)s - '
                                  '%(message)s')

    fh = logging.FileHandler(filename=os.path.join(path_to_log_directory, log_filename), mode='w+', encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    # fh.addFilter(type('', (logging.Filter,), {'filter': staticmethod(lambda r: r.levelno <= logging.INFO)}))
    _importer_logger.addHandler(fh)

    # _importer_logger = logging.basicConfig(filename=os.path.join(path_to_log_directory, log_filename),
    #                                        filemode='w+', format=formatter, level=logging.DEBUG)

    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.DEBUG)
    sh.setFormatter(formatter)
    # sh.addFilter(type('', (logging.Filter,), {'filter': staticmethod(lambda r: r.levelno <= logging.INFO)}))
    _importer_logger.addHandler(sh)

    create_directory_if_not_exists(_importer_logger, path_to_log_directory)

    return _importer_logger


def log_critical_error(logger, ex, message):
    """
    Logs the exception at 'CRITICAL' log level
    :param logger:  the logger
    :param ex:      exception to log
    :param message: descriptive message to log details of where/why ex occurred
    """
    if logger is not None:
        logger.critical(message)
        logger.critical(ex)


def create_directory_if_not_exists(logger, path):
    """
    Creates 'path' if it does not exist
    If creation fails, an exception will be thrown
    :param logger:  the logger
    :param path:    the path to ensure it exists
    """
    try:
        os.makedirs(path)
    except OSError as ex:
        if ex.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            log_critical_error(logger, ex, 'An error happened trying to create ' + path)
            raise


def configure_logger():
    """
    Declare and validate existence of log directory; create and configure logger object
    :return:  instance of configured logger object
    """

    cfg = get_config_constant_file()

    # log_dir = os.path.join(os.getcwd(), 'log')
    log_name = cfg['LOG_RESOURCE']['APP_FILE_LOG_NAME']
    log_dir = cfg['LOG_RESOURCE']['DIRECTORY_LOG_FILES']

    logger = configure_logging(log_name, log_dir)

    # logger = logging.getLogger('root')
    if logger is not None:

        return logger


def configure_ws_logger():
    """
    Declare and validate existence of log directory; create and configure logger object
    :return:  instance of configured logger object
    """

    cfg = get_config_constant_file()

    log_name = cfg['LOG_RESOURCE']['WS_FILE_LOG_NAME']
    # log_dir = os.path.join(os.getcwd(), 'log')
    log_dir = cfg['LOG_RESOURCE']['DIRECTORY_LOG_FILES']

    logger = configure_ws_logging(log_name, log_dir)

    # create_directory_if_not_exists(logger, log_dir)

    # logger = logging.getLogger('root')
    if logger is not None:

        return logger


def configure_db_logger():

    """
    Declare and validate existence of log directory; create and configure logger object
    :return:  instance of configured logger object
    """

    cfg = get_config_constant_file()

    log_name = cfg['LOG_RESOURCE']['DB_FILE_LOG_NAME']
    # log_dir = os.path.join(os.getcwd(), 'log')
    log_dir = cfg['LOG_RESOURCE']['DIRECTORY_LOG_FILES']

    logger = configure_db_logging(log_name, log_dir)

    # create_directory_if_not_exists(logger, log_dir)

    # logger = logging.getLogger('root')
    if logger is not None:

        return logger


# Define y obtiene el configurador para las constantes del sistema:
def get_config_constant_file():
    """
        Contiene la obtencion del objeto config
        para setear datos de constantes en archivo
        configurador

    :rtype: object
    """

    # TEST
    _constants_file = "constants/constants.yml"

    # PROD
    # _constants_file = "/ofix/tienda_virtual/parserCt/constants/constants.yml"
    cfg = Const.get_constants_file(_constants_file)

    return cfg
