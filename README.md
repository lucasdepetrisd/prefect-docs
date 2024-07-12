# 1. Documentación de Prefect
Documentación de Prefect.

## Tabla de contenidos
- [1. Documentación de Prefect](#1-documentación-de-prefect)
  - [Tabla de contenidos](#tabla-de-contenidos)
- [2. Sobre Prefect](#2-sobre-prefect)
- [3. Flows](#3-flows)
  - [3.1. Subflows](#31-subflows)
- [4. Deploys](#4-deploys)
- [5. Work Pools](#5-work-pools)
- [6. Logeo](#6-logeo)
  - [6.1. Logger de Prefect](#61-logger-de-prefect)
  - [6.2. Logger de Python](#62-logger-de-python)
  - [6.3. Logger Personalizado](#63-logger-personalizado)
- [[Prefect#7. Perfiles|7. Perfiles]]
- [8. Watchdog](#8-watchdog)

# 2. Sobre Prefect

Prefect es una plataforma para la automatización y orquestación de flujos de trabajo, diseñada para simplificar y optimizar la ejecución de tareas y procesos complejos. Su enfoque se centra en facilitar la creación, programación y monitoreo de flujos de datos.

A diferencia de otros orquestadores, Prefect se esfuerza por ser lo menos invasivo posible, reconociendo que el código escrito ya representa de manera óptima los objetivos del flujo de trabajo y cualquier adaptación a una plataforma supone un esfuerzo que innecesario.

Esta plataforma sigue la idea de "[code as workflows](https://www.prefect.io/blog/prefect-global-coordination-plane#:~:text=Code%20as%20Workflows%3A%20The%20Python%20API)". Esto significa que cualquier función puede convertirse fácilmente en un flujo Prefect simplemente agregando un decorador ```@flow```. Este enfoque nos brinda acceso instantáneo a características como la gestión de errores, los reintentos automáticos, programación de la ejecución y una interfaz de usuario intuitiva para monitorear y controlar nuestros flujos de trabajo.

![Funcionamiento de Prefect](funcionamiento_prefect.png)
*Funcionamiento de Prefect: Este diagrama muestra el flujo de trabajo en Prefect, la definición del código, el despliegue y la ejecución en el servidor.*

# 3. Flows

En Prefect, un flow (flujo) es una abstracción que representa un conjunto de tareas y sus dependencias. Cada tarea ```@task``` dentro de un flujo puede ser una operación de datos, una llamada a una función de Python o cualquier otro tipo de acción ejecutable.

Un ejemplo básico de un flujo en Prefect con logeo y parámetros, podría ser el siguiente:

```python
from prefect import flow, task, get_run_logger

@task
def mi_tarea(mensaje_tarea: str = ""):
    logger = get_run_logger()
    logger.info("Hola %s desde la tarea", mensaje_tarea)

@flow
def mi_flujo(mensaje_flujo: str = ""):
    logger = get_run_logger()
    logger.info("Hola %s desde el flujo", mensaje_flujo)
    mi_tarea(mensaje_flujo)

if __name__ == '__main__':
    mi_flujo("mundo")
```

Esto devolverá lo siguiente en terminal:

```sh
12:41:21.023 | INFO    | prefect.engine - Created flow run 'zippy-skunk' for flow 'mi-flujo'
12:41:21.089 | INFO    | Flow run 'zippy-skunk' - Hola mundo desde el flujo
12:41:23.244 | INFO    | Flow run 'zippy-skunk' - Created task run 'mi_tarea-0' for task 'mi_tarea'
12:41:23.255 | INFO    | Flow run 'zippy-skunk' - Executing 'mi_tarea-0' immediately...
12:41:23.368 | INFO    | Task run 'mi_tarea-0' - Hola mundo desde la tarea
12:41:23.435 | INFO    | Task run 'mi_tarea-0' - Finished in state Completed()
12:41:23.494 | INFO    | Flow run 'zippy-skunk' - Finished in state Completed('All states completed.')
```

Y en la IU de Prefect se mostrará así:

![Resultados](resultados_basico.png)

## 3.1. Subflows

También se puede crear un flujo que ejecute otros flujos creando así _subflujos_.

Para ejemplificar los sublujos y también mostrar como funciona la ejecución de flujos en paralelos con funciones ```async```, podemos combinar ambos en un mismo ejemplo:

> [!example]
> Se puede utilizar ```async``` no solo en sublujos sino también en flujos normales y tareas.

```python
import asyncio
from prefect import flow, task

@task
async def print_text(mensaje: str):
    print(mensaje)
    

@flow
async def subflow_1():
    await print_text("Subflow 1 started!")
    await asyncio.sleep(1)

@flow
async def subflow_2():
    await print_text("Subflow 2 started!")
    await asyncio.sleep(1)

@flow
async def main_flow():
    parallel_subflows = [subflow_1(), subflow_2()]
    await asyncio.gather(*parallel_subflows)

if __name__ == "__main__":
    main_flow_state = asyncio.run(main_flow())
```

Resultados:

![Resultados subflujo](resultados_subflujo.png)

> [!help] Sobre subflujos
> Si bien un flujo puede ejecutar subflujos, y estos a su vez ejecutar tareas, las tareas no pueden ejecutar flujos.  
> Por lo tanto las tareas son **unidades atómicas y representan la más mínima expresión de trabajo en una ejecución.** 
> 
> ***Esto cambio a partir de Prefect 2.18. Ahora las tareas pueden ejecutar otras tareas, flujos otros flujos y tareas ejecutar flujos.***

# 4. Deploys
Los deploys (despliegues) son conexiones del servidor local de prefect con nuestro código. Los deploys nos permiten establecer la ubicación del script (ya sea local o en git) y configurar como se ejecutará (de manera manual, programada, por intervalos, etc). Son el paralelo a las tareas que utilizábamos en el Programador de Tareas de Windows.

Los despliegues son configuraciones que definen cómo y dónde se ejecutan los flujos de trabajo de Prefect. Incluyen detalles como la ubicación del código y los parámetros requeridos. También indican que Worker ejecutará el flujo y el tipo de infraestructura necesaria.

Antes de crear un deploy se debe haber creado previamente una work pool que reciba ese deploy. Para más información visita la sección [Work Pools](#5.-Work-Pools).

Crear un despliegue es sencillo y se puede hacer con el comando:
```prefect deploy```.
Prefect buscará automáticamente, en los subdirectorios disponibles, scripts que posean la etiqueta ```@flow``` y que tengan un ```__main__``` configurado. Por ejemplo:
```python
@flow
def basic_flow(message="Hola mundo!"):
    print_message(message)

if __name__ == "__main__":
    basic_flow()
```

Y prefect nos mostrará:
```shell
> prefect deploy
? Select a flow to deploy [Use arrows to move; enter to select; n to select none]                                                                 
┏━━━┳━━━━━━━━━━━━┳━━━━━━━━━━┓                                                                                                                     
┃   ┃ Flow Name  ┃ Location ┃                                                                                                                     
┡━━━╇━━━━━━━━━━━━╇━━━━━━━━━━┩                                                                                                                     
│ > │ basic_flow │ basic.py │                                                                                                                     
└───┴────────────┴──────────┘
    Enter a flow entrypoint manually
```

Bastará con seleccionar el script. En caso que no se muestre para seleccionar se deberá ingresar manualmente utilizando el formato ```directorio/al/script.py:nombre_funcion```:
```shell
? Flow entrypoint (expected format path/to/file.py:function_name): ./project/basic.py:basic_flow
```

Luego se nos solicitarán diferentes configuraciones para el deploy:  
- **Nombre para el deploy:** Debe ser un nombre descriptivo. Es el equivalente al nombre de las tareas.
- **Ejecución programada:** se puede hacer por intervalos (cada cierto tiempo) o utilizando cron ([leer sintaxis de cron](https://marquesfernandes.com/es/tecnologia-es/crontab-what-and-and-how-to-use-no-ubuntu-debian/) y [generador de cron](https://crontab.guru/#30_1,13,17_*_*_*)).
    - Esto puede ser configurado luego y de manera mucho más sencilla desde la UI..
 
> [!TIP] 
> La sintaxis en cron para ejecutar en los horarios usuales (1:30, 13:30 y 17:30) es ```(30 1,13,17 * * *)```

> [!CAUTION]
> Tener en cuenta el huso horario **NO UTILIZAR "UTC".** Se debe setear en "America / Buenos Aires"

- Luego se puede elegir una Work pool para _deployar_ el flujo. Aquí aparecerán las pools disponibles para el servidor actual.

> [!TIP]
> Para cada deploy utilizar una pool coherente. Por ejemplo para un deploy de tipo **productivo** para el área **Compras** utiliza la pool ```compras-prod```.

<!-- ```shell
? Deployment name (default): printear-mensaje # Ingreso un nombre para el deploy.
? Would you like to configure a schedule for this deployment? [y/n] (y): n # No configuro la ejecución automática
``` -->
# 5. Work Pools

En Prefect, las Work Pools (grupos de trabajo) son conjuntos de _workers_ o trabajadores que se pueden configurar para ejecutar flujos de trabajo específicos. Permiten la gestión de recursos del sistema y la ejecución de los flujos.

Los grupos de trabajos son responsables de recibir la configuración y la información de los deploys y se encargan de monitorear y responder a nuevas ejecuciones de flujos, ya sean disparadas manualmente desde la interfaz de usuario (IU) de Prefect o mediante ejecuciones programadas previamente.

Para iniciar una nueva Work Pool se ejecuta el siguiente comando en terminal:
```bash
prefect worker start --pool my-new-pool
```

> [!NOTE]
> Las Work Pools deben ser ejecutadas constantemente para escuchar cambios en los deploys y poder ejecutar los scripts.
> Por lo tanto no deben manejarse desde una terminal local del usuario, sino desde un servicio de Windows. Este servicio debe arrancar al iniciarse el sistema y se crea utilizando nssm.  
> Para crear los servicios con nssm y configurarlos ver la carpeta [/servicios](servicios).

# 6. Logeo

En Prefect, el logeo se maneja de una manera especial para asegurar que los logs sean coherentes y se puedan recuperar y visualizar a través de la IU. Es por esto que, aunque Prefect usa el módulo estándar de Python ```logging``` también se incluye un "logger" configurado específicamente para interactuar con Prefect.

Si bien el logger de Prefect es fácil de utilizar, este no nos brinda las funcionalidades del logging estándar de Python que ya veníamos utilizando, como por ejemplo el poder guardar los registros en un archivo de log. Por lo tanto nos quedan dos opciones para el logeo:

## 6.1. Logger de Prefect

Este es el que ya veníamos utilizando en ejemplos anteriores y se utiliza así:

```python
from prefect import flow, task, get_run_logger

@task
def mi_tarea(mensaje_tarea: str = ""):
    logger = get_run_logger()
    logger.info("Hola %s desde la tarea", mensaje_tarea)

@flow
def mi_flujo(mensaje_flujo: str = ""):
    logger = get_run_logger()
    logger.info("Hola %s desde el flujo", mensaje_flujo)
    mi_tarea(mensaje_flujo)

if __name__ == '__main__':
    mi_flujo("mundo")
```

## 6.2. Logger de Python

Para utilizar la librería logging debemos indicarle a Prefect que también debe escuchar los logs que provienen de otros loggers además del propio.
Es por esto que hay que configurar en Prefect un extra logger y de esta manera cuando ejecute un script este será buscado y escuchado.

> [!NOTE]
> **De la documentación:** Cuando utilizas simplemente logging sin agregarlo como un "extra logger", puedes notar que los mensajes no aparecen en la terminal. Esto sucede porque Prefect redirige y gestiona los logs a través de su propio sistema de logeo, el cual está diseñado para capturar, etiquetar y enviar los logs de la ejecución de tus flujos para su posterior visualización en la IU.

Para agregar un extra logger se debe agregar una configuración al perfil actual de Prefect. Para eso se debe ejecutar en terminal lo siguiente:
```sh
prefect config set PREFECT_LOGGING_EXTRA_LOGGERS=custom,test
```

Esto agregará dos extra loggers "custom" y "test".

Para chequear que se hayan agregado correctamente ejecutaremos:

```sh
prefect config view
```

Esto nos mostrará todas las configuraciones para el perfil actual de Prefect.
Para más info sobre los perfiles ver [Perfiles](#7. Perfiles)

Ahora podemos usar logging normalmente:

```python
import logging

logging.basicConfig(
    format='%(asctime)s,%(msecs)03d %(name)-8s %(levelname)-8s : %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename='mislogs.log',
    encoding='utf-8',
    filemode='a',
    level=logging.INFO
)

# Esta linea asigna el nombre "custom" a la instancia de logger permitiendo que Prefect lo escuche 
logger = logging.getLogger('custom')
logger.setLevel(logging.INFO)

# Se debe importar prefect luego de instanciar mylogger. Si se hace después no se detectará
from prefect import task, flow 

@task
def mi_tarea(mensaje_tarea: str = ""):
    logger.info("Hola %s desde la tarea", mensaje_tarea)

@flow
def mi_flujo(mensaje_flujo: str = ""):
    logger.info("Hola %s desde el flujo", mensaje_flujo)
    mi_tarea(mensaje_flujo)

if __name__ == '__main__':
    mi_flujo("mundo")
```

Esta opción nos permite ver los logs de prefect y de logging combinados en la interfaz de Prefect y en la terminal, pero solo los de logging en el archivo de logs.

Las desventajas de este método son:
* Los logs propios de Prefect no se guardarán en el archivo de logs.
* Para que esto funcione se debe cuidar el orden de importación de las librerías de prefect. Se debe importar luego de instanciar el logger causando confusión en el uso.
* Tendremos que configurar el logger cada vez que se escribe un script agregando lineas repetidas al código. Esto puede provocar también que el archivo de logs se encuentre en un directorio diferente del script y que haya que buscarlo cada vez que se quiera leer.

---
Ojo que hay una tercera opción, que combina la facilidad del logger de Prefect con las funcionalidades del de Python:

## 6.3. Logger Personalizado

En esta tercera opción utilizamos la librería ElectraCommons que posee scripts comunes a los flujos que trabajamos en Electra.
La librería es privada y se encuentra en la organización [DesarrollosElectra](https://github.com/DesarrollosElectra/) en el siguiente link: [ElectraCommons](https://github.com/DesarrollosElectra/electracommons)  

Para instalar electracommons en Python se debe tener configurado git en la terminal con una cuenta que tenga acceso a la librería. Luego se debe ejecutar lo siguiente:

```sh
pip install git+https://github.com/DesarrollosElectra/electracommons.git
```

Una vez instalada la librería se llama como al logger de Prefect pero con la diferencia de que genera un archivo de logs con todos los registros y también logea correctamente en la IU de Prefect.

Ejemplo de uso:

```python
from prefect import flow, task

from electracommons.log_config import PrefectLogger

logger_global = PrefectLogger(__file__)

@task
def mi_tarea(mensaje_tarea: str = ""):
    logger = logger_global.obtener_logger_prefect()
    logger.info("Iniciando tarea...")
    logger.info("Hola %s desde la tarea", mensaje_tarea)
    
    # Cambio el archivo de salida
    logger = logger_global.cambiar_rotfile_handler_params(r"C:\src\logeo\logs\hola.log")
    logger.info("Tarea finalizada...") # Esto se mostrara solo en hola.log

@flow
def mi_flujo(mensaje_flujo: str = ""):
    logger = logger_global.obtener_logger_prefect()
    logger.info("Hola %s desde el flujo", mensaje_flujo)
    mi_tarea(mensaje_flujo)

if __name__ == '__main__':
    mi_flujo()
```

Resultado en terminal:

![Alt text](result_logging.png)

Resultado en archivo de log test.log:

![Alt text](result_logfile.png)

Resultado en IU:

![Alt text](result_iu.png)

La clase ```PrefectLogger``` tiene un método ```cambiar_rotfile_handler_params```, en el que podemos cambiarle parámetros del manejador de logs para una tarea o flujo especifico, como por ejemplo la ubicación del archivo de logeo o el formato. Para más info leer la documentación de [ElectraCommons](https://github.com/DesarrollosElectra/electracommons)  

Para que esto funcione correctamente el archivo de configuración del logeo de Prefect ```logging_new.yml``` que se encuentra en este repositorio debe estar correctamente configurado y seteado:

```sh
prefect config set PREFECT_LOGGING_SETTINGS_PATH=C:\Users\usuario\.prefect\logging_new.yml
```

# 7. Perfiles
Los perfiles en Prefect permiten a los usuarios configurar y almacenar ajustes específicos del entorno que se pueden activar o desactivar según sea necesario. Esto es útil para manejar diferentes configuraciones de Prefect, como puntos finales de API, configuraciones de seguridad y otras preferencias a nivel de usuario.

Entre las configuraciones principales de los perfiles se encuentran

* PREFECT_API_URL='http://127.0.0.2:5000/api' para indicar que las ejecuciones y deploys se dirijan a esa URL.
* PREFECT_LOGGING_SETTINGS_PATH='C:\Users\tareas\\.prefect\logging_new.yml' para configurar el loggeo con electracommons.log_config

Se pueden agregar variables para que en el momento de la ejecución Prefect las levante y utilice esos valores de manera global.

# 8. Watchdog

Las ejecuciones a veces pueden quedar atascadas, la conexión se puede caer y la computadora se puede apagar, imprevistos siempre pueden surgir y para manejarlos en Prefect tenemos el Watchdog. Watchdog es un script común y corriente que se ejecuta cada 30 minutos y que se encarga de cancelar esas ejecuciones atrasadas o congeladas.

Watchdog revisa ejecuciones en estado Late en las que la hora actual difiere de la hora de ejecución programada en por ejemplo 4 horas. O también ejecuciones en estado Running que estuvieron durante más de 2 horas corriendo. Este script se encarga de cambiar su estado a Canceladas para así no tener en el registro ejecuciones tardías o congeladas.

Para más info visitar el script [Watchdog](src/watchdog/watchdog.py).
