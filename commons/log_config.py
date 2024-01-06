"""
Inicializa la configuración de registro (logging) para Prefect en un script.

Este módulo proporciona funciones para configurar la configuración de registro
(logging) para Prefect, incluyendo la configuración de un TimedRotatingFileHandler
para la rotación de archivos de registro y la aplicación de un formato personalizado
para los mensajes de registro.

Funciones:
    - obtener_nombre_script: Devuelve el nombre del script que realiza la llamada.
    - inicializar_logger_prefect: Inicializa el registro para Prefect, incluyendo
                                  la configuración de manejadores de archivos y formateadores.
"""

import os
import logging
# from typing import Union
from logging.handlers import TimedRotatingFileHandler

from prefect import runtime
from prefect import logging as prefect_logging
from prefect.logging.formatters import PrefectFormatter

import lib_programname
# import __main__

CALLER_SCRIPT_PATH = str(lib_programname.get_path_executed_script())
FILE_NAME = os.path.splitext(os.path.basename(CALLER_SCRIPT_PATH))[0]
FILE_PATH = os.path.join(os.path.dirname(CALLER_SCRIPT_PATH))
LOG_PATH = os.path.join(os.path.dirname(
    CALLER_SCRIPT_PATH), 'logs', f"{FILE_NAME}.log")


class RemoveSpecificLogs(logging.Filter):
    def filter(self, record):
        # Agrega otras cadenas que deseas filtrar
        # strings_to_exclude = ['Created task run', 'Created flow run', 'Executing', 'Finished in state Completed']
        strings_to_exclude = ['Created task run',
                              'Created flow run', 'Executing']

        # Devuelve False si alguno de los records tiene el string en el mensaje
        return not any(exclude_str in record.msg for exclude_str in strings_to_exclude)

# class PrefectLogger(Union[logging.Logger, logging.LoggerAdapter]):
#     def __init__(self):
#         self.the_file = None


class PrefectLogger(object):
    """
    Clase para manejar el registro de logs para Prefect.

    Atributos:
        DEFAULT_LOG_PATH (str): Ruta predeterminada para los logs.
        DEFAULT_WHEN (str): Tipo de intervalo para la rotación del archivo (por defecto, 'W0' para semanal).
        DEFAULT_INTERVAL (int): Intervalo entre rotaciones en semanas.
        DEFAULT_BACKUP_COUNT (int): Número de archivos de log de respaldo a retener.

    Métodos:
        __init__(self, log_path: str = None):
            Inicializa la instancia de PrefectLogger.

        __initialize_logger(self):
            Inicializa el logger con los parámetros configurados.

        obtener_logger_prefect(self):
            Obtiene el logger de Prefect.

        cambiar_rotfile_handler_params(self, log_path: str = None, when: str = DEFAULT_WHEN, interval: int = DEFAULT_INTERVAL, backup_count: int = DEFAULT_BACKUP_COUNT):
            Cambia los parámetros del manejador de archivos rotativos.

    """

    DEFAULT_LOG_PATH = LOG_PATH
    DEFAULT_WHEN = 'W0'
    DEFAULT_INTERVAL = 1
    DEFAULT_BACKUP_COUNT = 12

    def __init__(self, log_path: str = None):
        self.__log_path = log_path or self.DEFAULT_LOG_PATH
        self.__when = self.DEFAULT_WHEN
        self.__interval = self.DEFAULT_INTERVAL
        self.__backup_count = self.DEFAULT_BACKUP_COUNT
        self.handler = None
        self.__logger_prefect = None

    def __initialize_logger(self):
        if runtime.flow_run.name is not None or runtime.task_run.name is not None:
            if self.__log_path is None:
                self.__log_path = LOG_PATH

            self.handler = TimedRotatingFileHandler(
                filename=self.__log_path,
                when=self.__when,
                interval=self.__interval,
                backupCount=self.__backup_count
            )

            formatter = PrefectFormatter(
                format="%(asctime)s | %(levelname)-7s | %(name)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
                flow_run_fmt="%(asctime)s | %(levelname)-7s | Flow %(flow_name)r - %(message)s",
                task_run_fmt="%(asctime)s | %(levelname)-7s | Task %(task_name)r - %(message)s"
            )
            self.handler.setFormatter(formatter)

            root_logger = logging.getLogger()
            prefect_logger = prefect_logging.get_run_logger()
            prefect_logger.logger.addHandler(self.handler)

            # Check if the handler is already present in root_logger.handlers
            rotfile_handler_present = any(isinstance(
                h, type(self.handler)) for h in root_logger.handlers)

            if rotfile_handler_present:
                # Replace the existing handler with self.handler
                for i, h in enumerate(root_logger.handlers):
                    if isinstance(h, type(self.handler)):
                        root_logger.handlers[i] = self.handler
                        break
            else:
                # Add the handler if not present
                root_logger.addHandler(self.handler)

            return prefect_logger
        # else:
        #     raise error

    def obtener_logger_prefect(self):
        """
        Obtiene el logger de Prefect.

        Retorna:
            prefect_logger: Instancia del logger de Prefect.
        """
        if self.__logger_prefect is None:
            self.__logger_prefect = self.__initialize_logger()
        return self.__logger_prefect

    def cambiar_rotfile_handler_params(self, log_path: str = LOG_PATH, when: str = DEFAULT_WHEN, interval: int = DEFAULT_INTERVAL, backup_count: int = DEFAULT_BACKUP_COUNT):
        """
        Cambia los parámetros del manejador de archivos rotativos.

        Parámetros:
            log_path (str): Ruta personalizada para los logs. Si es None, se utiliza DEFAULT_LOG_PATH.
            when (str): Tipo de intervalo para la rotación del archivo.
            interval (int): Intervalo entre rotaciones en semanas.
            backup_count (int): Número de archivos de log de respaldo a retener.

        Retorna:
            prefect_logger: Instancia actualizada del logger de Prefect.
        """
        self.__log_path = log_path or self.DEFAULT_LOG_PATH
        self.__when = when or self.DEFAULT_WHEN
        self.__interval = interval or self.DEFAULT_INTERVAL
        self.__backup_count = backup_count or self.DEFAULT_BACKUP_COUNT
        # Reinitialize the logger with updated parameters
        self.__logger_prefect = self.__initialize_logger()

        return self.__logger_prefect


def obtener_nombre_script():
    # print("__main__.__file__: " + __main__.__file__) # Otra opcion es obtener importando __main__
    # print("programname: " + str(lib_programname.get_path_executed_script()))
    return FILE_NAME


def obtener_logger_prefect():
    global LOGGER_PREFECT
    if LOGGER_PREFECT is None:
        LOGGER_PREFECT = inicializar_logger_prefect(LOG_PATH)
    return LOGGER_PREFECT


def inicializar_logger_prefect(log_path: str = None):
    # Se configura el logger para que imprima en el log_path
    # que incluye el nombre del script que llama al modulo
    if log_path is None:
        log_path = LOG_PATH

    handler = TimedRotatingFileHandler(
        filename=log_path,
        when='W0',
        interval=1,
        backupCount=12
    )

    formatter = PrefectFormatter(
        format="%(asctime)s | %(levelname)-7s | %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        flow_run_fmt="%(asctime)s | %(levelname)-7s | Flow %(flow_name)r - %(message)s",
        task_run_fmt="%(asctime)s | %(levelname)-7s | Task %(task_name)r - %(message)s"
    )
    handler.setFormatter(formatter)

    prefect_logger = None
    root_logger = logging.getLogger()  # Obtener el logger root por defecto

    # Chequear si se llamo desde un flujo o tarea para detectar si se esta inicializando o se quiere obtener solo el logger
    if runtime.flow_run.name is not None or runtime.task_run.name is not None:
        prefect_logger = prefect_logging.get_run_logger()
        plogger = prefect_logger.logger
        plogger.addHandler(handler)

    # Verificar si el handler ya está presente en cualquier logger
    handler_present = any(isinstance(h, type(handler))
                          for h in root_logger.handlers)

    if not handler_present:
        root_logger.addHandler(handler)

    # if handler not in root_logger.handlers:
    #     root_logger.addHandler(handler)

    return prefect_logger
