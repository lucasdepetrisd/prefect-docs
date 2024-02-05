#* --------------------------------------------------------------
#* Instalar servicio
#* --------------------------------------------------------------

C:\nssm-2.24\win64\nssm.exe stop prefpoolwd # Detener en caso que este funcionando

C:\nssm-2.24\win64\nssm.exe install prefpoolwd C:\Reportes_Power_BI\Python\virtualenvs\devenv\Scripts\prefect.exe worker start --pool watchdog-pool
C:\nssm-2.24\win64\nssm.exe set prefpoolwd DisplayName "Prefect Pool - Watchdog"
# nssm set "Prefect Watchdog Pool" AppDirectory "C:\service\path"

# --------------------------------------------------------------
# Set descripcion
# --------------------------------------------------------------

C:\nssm-2.24\win64\nssm.exe set prefpoolwd Description "Pool de ejecuciones para flujo Watchdog"

#* --------------------------------------------------------------
#* Set usuario tareas
#* --------------------------------------------------------------

C:\nssm-2.24\win64\nssm.exe set prefpoolwd ObjectName SRVPWBGW\tareas Tar34s23

# --------------------------------------------------------------
# Set directorios logeo
# --------------------------------------------------------------

C:\nssm-2.24\win64\nssm.exe set prefpoolwd AppStdout C:\Reportes_Power_BI\Python\logs_services\pools\pool-dev\stdout.log
C:\nssm-2.24\win64\nssm.exe set prefpoolwd AppStderr C:\Reportes_Power_BI\Python\logs_services\pools\pool-dev\stderr.log

# --------------------------------------------------------------
# Set tipo de creacion de archivos de logeo. 4 = OPEN_ALWAYS. Crear si no se encontro y append al final
#? Más info:
#? CREATE_NEW (1): Creates a new file. If the file already exists, the function fails.
#? CREATE_ALWAYS (2): Creates a new file. If the file already exists, it is overwritten and truncated to zero length.
#? OPEN_EXISTING (3): Opens the file. The function fails if the file does not exist.
#? OPEN_ALWAYS (4): Opens the file if it exists. If the file does not exist, the function creates the file as if CREATE_NEW were specified.
#? TRUNCATE_EXISTING (5): Opens the file and truncates it so that its size is zero bytes. The function fails if the file does not exist.
# --------------------------------------------------------------

C:\nssm-2.24\win64\nssm.exe set prefpoolwd AppStdoutCreationDisposition 4
C:\nssm-2.24\win64\nssm.exe set prefpoolwd AppStderrCreationDisposition 4

# --------------------------------------------------------------
# Set rotacion de archivos de logeo.
#? AppRotateFiles para rotar solo en el inicio. 
#? AppRotateOnline para rotar durante la ejecucion. 
#? Seconds y Bytes para rotar luego de alcanzar alguno de esos limites
# --------------------------------------------------------------

C:\nssm-2.24\win64\nssm.exe set prefpoolwd AppRotateFiles 1
C:\nssm-2.24\win64\nssm.exe set prefpoolwd AppRotateOnline 1
C:\nssm-2.24\win64\nssm.exe set prefpoolwd AppRotateSeconds 86400 # Rotar cada un dia
# C:\nssm-2.24\win64\nssm.exe set prefpoolwd AppRotateBytes 50000

#* --------------------------------------------------------------
#* Set iniciar al arrancar el sistema
#* --------------------------------------------------------------

C:\nssm-2.24\win64\nssm.exe set prefpoolwd Start SERVICE_AUTO_START
C:\nssm-2.24\win64\nssm.exe start prefpoolwd

#! --------------------------------------------------------------
#! Eliminar servicio.
#! --------------------------------------------------------------

# C:\nssm-2.24\win64\nssm.exe stop "Prefect Pool - Watchdog"
# C:\nssm-2.24\win64\nssm.exe remove "Prefect Pool - Watchdog"