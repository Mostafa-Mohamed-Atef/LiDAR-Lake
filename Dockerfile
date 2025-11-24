FROM flink:1.19.1-java11

# Install Python and dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install PyFlink and Kafka dependencies
RUN pip3 install --no-cache-dir \
    apache-flink==1.19.1 \
    confluent-kafka

# Link python3 to python so Flink can find it
RUN ln -s /usr/bin/python3 /usr/bin/python
