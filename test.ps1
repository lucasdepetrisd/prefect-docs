$filePath = "C:\Users\Lucas\OneDrive\Consulters\Electra\ElectraTest\logging_service\stdout.log"

# Get the last modified date of the file
$lastModified = (Get-Item $filePath).LastWriteTime

# Read the file content
$content = Get-Content $filePath

# Define a regex pattern to identify lines without a timestamp
$pattern = "^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"

# Iterate through each line
foreach ($line in $content) {
    # Check if the line doesn't already have a timestamp
    if ($line -notmatch $pattern) {
        # Prepend the timestamp to the line
        $line = "$lastModified | $line"
    }

    # Output the modified line
    Write-Output $line
} | Set-Content $filePath