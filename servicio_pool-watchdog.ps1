C:\nssm-2.24\win64\nssm.exe install "watchdog-pool" "C:\Users\Lucas\miniconda3\envs\electraenv\Scripts\prefect.exe" "worker start --pool watchdog-pool"
# C:\nssm-2.24\win64\nssm.exe remove "watchdog-pool"

# nssm set "watchdog-pool" AppDirectory "C:\service\path"

C:\nssm-2.24\win64\nssm.exe set "watchdog-pool" AppStdout "C:\Users\Lucas\OneDrive\Consulters\Electra\ElectraTest\logging_service\pool_logs\watchdog-pool\stdout.log"
C:\nssm-2.24\win64\nssm.exe set "watchdog-pool" AppStderr "C:\Users\Lucas\OneDrive\Consulters\Electra\ElectraTest\logging_service\pool_logs\watchdog-pool\stdout.log"

C:\nssm-2.24\win64\nssm.exe set "watchdog-pool" AppStdoutCreationDisposition 4
C:\nssm-2.24\win64\nssm.exe set "watchdog-pool" AppStderrCreationDisposition 4

# C:\nssm-2.24\win64\nssm.exe set "watchdog-pool" AppRotateFiles 1
# C:\nssm-2.24\win64\nssm.exe set "watchdog-pool" AppRotateOnline 1
# C:\nssm-2.24\win64\nssm.exe set "watchdog-pool" AppRotateSeconds 0
# C:\nssm-2.24\win64\nssm.exe set "watchdog-pool" AppRotateBytes 50000

C:\nssm-2.24\win64\nssm.exe set "watchdog-pool" Start SERVICE_AUTO_START
C:\nssm-2.24\win64\nssm.exe start "watchdog-pool"