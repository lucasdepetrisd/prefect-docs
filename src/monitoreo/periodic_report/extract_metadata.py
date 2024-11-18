"""
    hola soy el script
"""

import re
import yaml

from prefect import task

from consulterscommons.log_tools import PrefectLogger

logger_global = PrefectLogger(__file__)


@task
def extract_metadata(docstring: str) -> dict:
    """
    Extrae metadatos de un docstring en formato YAML.
    """
    logger = logger_global.obtener_logger_prefect()

    # Verificar si el docstring es None o no es un string
    if not isinstance(docstring, str):
        logger.debug("El docstring proporcionado es None o no es de tipo str.")
        return {}

    pattern = r"---\n(.*)"  # Busca el bloque de YAML tras tres guiones seguidos de un salto de línea

    # Intentar extraer el bloque YAML
    yaml_block = re.search(pattern, docstring, re.DOTALL)

    if yaml_block:
        try:
            # Intentar parsear el bloque YAML encontrado
            metadata = yaml.safe_load(yaml_block.group(1))
            if isinstance(metadata, dict) and 'metadata' in metadata:
                logger.debug("Metadatos extraídos con éxito.")
                return metadata.get('metadata', {})
            else:
                logger.warning("El bloque YAML no contiene el campo 'metadata'.")
                return {}
        except yaml.YAMLError as e:
            logger.warning("Error al parsear YAML: %s", e)
        except Exception as e:
            logger.error("Error inesperado al procesar el docstring: %s", e)
    else:
        logger.debug("No se encontró un bloque de YAML en el docstring.")

    return {}


if __name__ == "__main__":

    # Ejemplo de uso
    _docstring = """
    Función principal que ejecuta el proceso de obtención de stock desde RGA.

    Esta función realiza las siguientes tareas:
    - Configura el registro de eventos.
    - Inicializa la conexión a SQL Server.
    - Ejecuta una consulta SQL para insertar el stock en la base de datos.
    - Envía un correo electrónico en caso de errores.

    ---
    metadata:
        nombre: Proceso de Obtención de Stock de RGA
        responsable: FR
        area: Producción
    """

    _metadata = extract_metadata(_docstring)
    print(_metadata)

    # import asyncio
    # from uuid import UUID
    # from dev.MONITOREO_PREFECT.get_prefect_info import get_deployment_info

    # deploy_id = UUID('64ceb90b-300b-4013-8474-c4b9314db290')
    # deploy_info = asyncio.run(get_deployment_info(deploy_id))
    # print(deploy_info["description"])
    # metadata = extract_metadata(deploy_info["description"])
    # print(metadata)
