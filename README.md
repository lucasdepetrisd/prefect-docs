# Notas de Prefect
Para el repositorio o copiar a un documento compartido

## Perfiles


## Deploys
Los deploys son conexiones del servidor de prefect con nuestro código. Los deploys nos permiten establecer la ubicación del script (ya sea local o en git) y configurar como se ejecutará (de manera manual, programada, por intervalos, etc). Son paralelos a las tareas que utilizabamos en el Programador de Tareas de Windows.

Crear un deploy es sencillo y se puede hacer con el comando:
```prefect deploy```.
Prefect buscará automaticamente, en los subdirectorios disponibles, scripts que posean la etiqueta ```@flow``` y que tengan un ```__main__``` configurado. Por ejemplo:
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

Bastará con seleccionar el script o en caso que no se muestre para seleccionar ingresarlo manualmente:
```shell
? Flow entrypoint (expected format path/to/file.py:function_name): ./project/basic.py:basic_flow
```

Luego se nos solicitaran diferentes configuraciones para el deploy:  
- **Nombre para el deploy:** Debe ser un nombre descriptivo. Es el equivalente al nombre de las tareas.
- **Ejecución programada:** se puede hacer por intervalos (cada cierto tiempo) o utilizando cron ([leer sintáxis de cron](https://marquesfernandes.com/es/tecnologia-es/crontab-what-and-and-how-to-use-no-ubuntu-debian/) y [generador de cron](https://crontab.guru/#30_1,13,17_*_*_*)).
    - Esto puede ser configurado luego y de manera mucho más sencilla desde la UI.
    > [!TIP] 
    > La sintáxis en cron para ejecutar en los horarios usuales (1:30, 13:30 y 17:30) es ```(30 1,13,17 * * *)```

    > [!CAUTION]
    > Tener en cuenta el huso horario **NO UTILIZAR "UTC".** Se debe setear en "America / Buenos Aires"
- Luego podras elegir una Work pool para _deployar_ el flujo. Aquí apareceran las pools disponibles para el servidor actual. Para más información sobre Work Pools visita la sección [Sobre Work Pools](#sobre-work-pools).
    > [!NOTE]
    > Para cada deploy utilizar una pool coherente. Por ejemplo para un deploy de tipo **productivo** para el área **Compras** utiliza la pool ```compras-prod```.

<!-- ```shell
? Deployment name (default): printear-mensaje # Ingreso un nombre para el deploy.
? Would you like to configure a schedule for this deployment? [y/n] (y): n # No configuro la ejecución automática
``` -->

## Work Pools

Las Work Pools o Grupos de Trabajos

## Logeo

Archivo logging.yml