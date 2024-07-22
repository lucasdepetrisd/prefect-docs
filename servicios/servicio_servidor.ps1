
#* --------------------------------------------------------------
#* Instalar servicio
#* --------------------------------------------------------------

C:\nssm-2.24\win64\nssm.exe stop prefserver # Detener en caso que este funcionando

# C:\nssm-2.24\win64\nssm.exe install prefserver C:\Reportes_Power_BI\Python\virtualenvs\devenv\Scripts\prefect.exe server start --host 127.0.0.2 --port 5000
C:\nssm-2.24\win64\nssm.exe install prefserver "C:\Users\Lucas\miniconda3\envs\electraenv\Scripts\prefect.exe" server start --host 127.0.0.2 --port 5000
C:\nssm-2.24\win64\nssm.exe set prefserver DisplayName "Prefect Server"
# nssm set "Prefect Server" AppDirectory "C:\service\path"

# --------------------------------------------------------------
# Set descripcion
# --------------------------------------------------------------

C:\nssm-2.24\win64\nssm.exe set prefserver Description "Servidor Prefect ejecutandose en http://127.0.0.2:5000/"

#* --------------------------------------------------------------
#* Set usuario tareas
#* --------------------------------------------------------------

# C:\nssm-2.24\win64\nssm.exe set prefserver ObjectName SRVPWBGW\tareas Tar34s23

# --------------------------------------------------------------
# Set directorios logeo
# --------------------------------------------------------------

# C:\nssm-2.24\win64\nssm.exe set prefserver AppStdout C:\Reportes_Power_BI\Python\logs_services\server\stdout.log
C:\nssm-2.24\win64\nssm.exe set prefserver AppStdout "C:\Users\Lucas\Documents\Consulters\Electra\Python\prefect-test\logs_services\server\stdout.log"
# C:\nssm-2.24\win64\nssm.exe set prefserver AppStderr C:\Reportes_Power_BI\Python\logs_services\server\stderr.log
C:\nssm-2.24\win64\nssm.exe set prefserver AppStderr "C:\Users\Lucas\Documents\Consulters\Electra\Python\prefect-test\logs_services\server\stderr.log"

# --------------------------------------------------------------
# Set tipo de creacion de archivos de logeo. 4 = OPEN_ALWAYS. Crear si no se encontro y append al final
#? Más info:
#? CREATE_NEW (1): Creates a new file. If the file already exists, the function fails.
#? CREATE_ALWAYS (2): Creates a new file. If the file already exists, it is overwritten and truncated to zero length.
#? OPEN_EXISTING (3): Opens the file. The function fails if the file does not exist.
#? OPEN_ALWAYS (4): Opens the file if it exists. If the file does not exist, the function creates the file as if CREATE_NEW were specified.
#? TRUNCATE_EXISTING (5): Opens the file and truncates it so that its size is zero bytes. The function fails if the file does not exist.
# --------------------------------------------------------------

C:\nssm-2.24\win64\nssm.exe set prefserver AppStdoutCreationDisposition 4
C:\nssm-2.24\win64\nssm.exe set prefserver AppStderrCreationDisposition 4

# --------------------------------------------------------------
# Set rotacion de archivos de logeo.
#? AppRotateFiles para rotar solo en el inicio. 
#? AppRotateOnline para rotar durante la ejecucion. 
#? Seconds y Bytes para rotar luego de alcanzar alguno de esos limites
# --------------------------------------------------------------

C:\nssm-2.24\win64\nssm.exe set prefserver AppRotateFiles 0
C:\nssm-2.24\win64\nssm.exe set prefserver AppRotateOnline 0
C:\nssm-2.24\win64\nssm.exe set prefserver AppRotateSeconds 86400 # Rotar cada un dia
# C:\nssm-2.24\win64\nssm.exe set prefserver AppRotateBytes 50000

#* --------------------------------------------------------------
#* Set iniciar al arrancar el sistema
#* --------------------------------------------------------------

C:\nssm-2.24\win64\nssm.exe set prefserver Start SERVICE_AUTO_START
C:\nssm-2.24\win64\nssm.exe start prefserver

#! --------------------------------------------------------------
#! Eliminar servicio.
#! --------------------------------------------------------------

# C:\nssm-2.24\win64\nssm.exe stop prefserver
# C:\nssm-2.24\win64\nssm.exe remove prefserver