C:\nssm-2.24\win64\nssm.exe install "Prefect My New Pool" "C:\Users\Lucas\miniconda3\envs\electraenv\Scripts\prefect.exe" "worker start --pool my-new-pool"
# C:\nssm-2.24\win64\nssm.exe remove "Prefect My New Pool"

# nssm set "Prefect My New Pool" AppDirectory "C:\service\path"

C:\nssm-2.24\win64\nssm.exe set "Prefect My New Pool" AppStdout "C:\Users\Lucas\OneDrive\Consulters\Electra\ElectraTest\logging_service\pool_logs\my-new-pool\stdout.log"
C:\nssm-2.24\win64\nssm.exe set "Prefect My New Pool" AppStderr "C:\Users\Lucas\OneDrive\Consulters\Electra\ElectraTest\logging_service\pool_logs\my-new-pool\stdout.log"

C:\nssm-2.24\win64\nssm.exe set "Prefect My New Pool" AppStdoutCreationDisposition 4
C:\nssm-2.24\win64\nssm.exe set "Prefect My New Pool" AppStderrCreationDisposition 4

# C:\nssm-2.24\win64\nssm.exe set "Prefect My New Pool" AppRotateFiles 1
# C:\nssm-2.24\win64\nssm.exe set "Prefect My New Pool" AppRotateOnline 1
# C:\nssm-2.24\win64\nssm.exe set "Prefect My New Pool" AppRotateSeconds 0
# C:\nssm-2.24\win64\nssm.exe set "Prefect My New Pool" AppRotateBytes 50000

C:\nssm-2.24\win64\nssm.exe set "Prefect My New Pool" Start SERVICE_AUTO_START
C:\nssm-2.24\win64\nssm.exe start "Prefect My New Pool"