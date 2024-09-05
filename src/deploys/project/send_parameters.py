from prefect import task, flow
from datetime import datetime
import time

from consulterscommons.log_tools import PrefectLogger

logger_prefect = PrefectLogger(__file__)

@task
def print_message(numero):
    logger = logger_prefect.obtener_logger_prefect()
    logger.info("Iniciando tarea...")
    time.sleep(1)

    message = "La respuesta es: " + str(numero)
    logger.info(message)

    return message

@task
def sumar_numeros(a: int, b: int) -> str:
    time.sleep(1)
    return str(int(a) + int(b))

@task
def duplicar_numero(a: str) -> str:
    time.sleep(1)
    return str(int(a) * 2)

@flow
def mano_derecha(numero):
    # time.sleep(1)
    message = print_message(numero)
    return message

@flow
def mano_izquierda(numero):
    # time.sleep(1)
    message = mano_derecha(numero)
    return message

@flow
def basic_flow(num1, num2):
    logger = logger_prefect.obtener_logger_prefect()
    logger.info("Iniciando flujo...")

    numero_str = sumar_numeros(num1, num2)
    numero_dup_str = duplicar_numero(numero_str)

    time.sleep(1)

    for _ in range(10):
        numero_str = sumar_numeros(numero_str, numero_dup_str)
        message = mano_izquierda(numero_str)
        logger.info(message)

    message = mano_izquierda(numero_str)
    logger.info(message)

    message = mano_derecha(numero_dup_str)
    logger.info(message)
    return message

if __name__ == "__main__":
    basic_flow(5, 3)
