C:\nssm-2.24\win64\nssm.exe install "Prefect Watchdog Pool" "C:\Users\Lucas\miniconda3\envs\electraenv\Scripts\prefect.exe" "worker start --pool watchdog-pool"
# C:\nssm-2.24\win64\nssm.exe remove "watchdog-pool"

# nssm set "Prefect Watchdog Pool" AppDirectory "C:\service\path"

C:\nssm-2.24\win64\nssm.exe set "Prefect Watchdog Pool" AppStdout "C:\Users\Lucas\OneDrive\Consulters\Electra\prefect-test\logging_service\pool_logs\watchdog-pool\stdout.log"
C:\nssm-2.24\win64\nssm.exe set "Prefect Watchdog Pool" AppStderr "C:\Users\Lucas\OneDrive\Consulters\Electra\prefect-test\logging_service\pool_logs\watchdog-pool\stdout.log"

C:\nssm-2.24\win64\nssm.exe set "Prefect Watchdog Pool" AppStdoutCreationDisposition 4
C:\nssm-2.24\win64\nssm.exe set "Prefect Watchdog Pool" AppStderrCreationDisposition 4

# C:\nssm-2.24\win64\nssm.exe set "Prefect Watchdog Pool" AppRotateFiles 1
# C:\nssm-2.24\win64\nssm.exe set "Prefect Watchdog Pool" AppRotateOnline 1
# C:\nssm-2.24\win64\nssm.exe set "Prefect Watchdog Pool" AppRotateSeconds 0
# C:\nssm-2.24\win64\nssm.exe set "Prefect Watchdog Pool" AppRotateBytes 50000

C:\nssm-2.24\win64\nssm.exe set "Prefect Watchdog Pool" Start SERVICE_AUTO_START
C:\nssm-2.24\win64\nssm.exe start "Prefect Watchdog Pool"