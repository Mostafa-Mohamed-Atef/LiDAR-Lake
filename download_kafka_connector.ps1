# Download Kafka connector JAR for Flink
$jarDir = ".\jars"
$jarFile = "$jarDir\flink-connector-kafka-1.19.1.jar"

# Create jars directory if it doesn't exist
if (-not (Test-Path $jarDir)) {
    New-Item -ItemType Directory -Path $jarDir -Force
    Write-Host "Created $jarDir directory"
}

# Download Kafka connector if not already present
if (-not (Test-Path $jarFile)) {
    Write-Host "Downloading Kafka connector JAR..."
    
    # Try primary URL first
    $url = "https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.19.1/flink-sql-connector-kafka-1.19.1.jar"
    
    try {
        [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
        Invoke-WebRequest -Uri $url -OutFile $jarFile -UseBasicParsing
        Write-Host "Downloaded Kafka connector JAR"
    }
    catch {
        Write-Host "Primary URL failed, trying alternative..."
        # Try alternative URL
        $url2 = "https://archive.apache.org/dist/flink/flink-1.19.1/flink-sql-connector-kafka-1.19.1.jar"
        try {
            Invoke-WebRequest -Uri $url2 -OutFile $jarFile -UseBasicParsing
            Write-Host "Downloaded Kafka connector JAR from alternative source"
        }
        catch {
            Write-Host "Failed to download: $_"
            Write-Host "You may need to download manually from: https://repo.maven.apache.org/maven2/org/apache/flink/"
            exit 1
        }
    }
}
else {
    Write-Host "Kafka connector JAR already exists"
}

Write-Host "Ready to run Flink!"
