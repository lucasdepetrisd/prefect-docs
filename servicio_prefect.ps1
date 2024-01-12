# C:\nssm-2.24\win64\nssm.exe install "Prefect Server" "C:\Users\Lucas\miniconda3\envs\electraenv\Scripts\prefect.exe" "server start --host 127.0.0.2 --port 5000"
# C:\nssm-2.24\win64\nssm.exe remove "Prefect Server"

# nssm set "Prefect Server" AppDirectory "C:\service\path"

C:\nssm-2.24\win64\nssm.exe set "Prefect Server" AppStdout "C:\Users\Lucas\OneDrive\Consulters\Electra\ElectraTest\logging_service\stdout.log"
C:\nssm-2.24\win64\nssm.exe set "Prefect Server" AppStderr "C:\Users\Lucas\OneDrive\Consulters\Electra\ElectraTest\logging_service\stderr.log"

C:\nssm-2.24\win64\nssm.exe set "Prefect Server" AppStdoutCreationDisposition 4
C:\nssm-2.24\win64\nssm.exe set "Prefect Server" AppStderrCreationDisposition 4

C:\nssm-2.24\win64\nssm.exe set "Prefect Server" AppRotateFiles 1
C:\nssm-2.24\win64\nssm.exe set "Prefect Server" AppRotateOnline 1
C:\nssm-2.24\win64\nssm.exe set "Prefect Server" AppRotateSeconds 0
C:\nssm-2.24\win64\nssm.exe set "Prefect Server" AppRotateBytes 50000

C:\nssm-2.24\win64\nssm.exe set "Prefect Server" Start SERVICE_AUTO_START
C:\nssm-2.24\win64\nssm.exe start "Prefect Server"