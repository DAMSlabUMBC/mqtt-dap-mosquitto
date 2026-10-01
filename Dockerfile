FROM ubuntu:22.04

# Need this so apt doesn't ask me questions during install
ENV DEBIAN_FRONTEND=noninteractive

# Get all the dependencies we need (added wget and tar for node exporter)
RUN apt-get update && apt-get install -y \
    cmake \
    build-essential \
    libssl-dev \
    ca-certificates \
    libcjson-dev \
    libargon2-dev \
    libsqlite3-dev \
    pkg-config \
    libcunit1-dev \
    xsltproc \
    docbook-xsl \
    iproute2 \
    iputils-ping \
    wget \
    tar \
 && rm -rf /var/lib/apt/lists/*

# Set up the working directory
WORKDIR /opt/mqtt_brokers/mqtt-dap-mosquitto

# Copy everything over
COPY . /opt/mqtt_brokers/mqtt-dap-mosquitto

# Build mosquitto (had to disable LTO and ENGINE_cleanup to get it working)
RUN cmake -DWITH_LTO=OFF \
          -DWITH_CLIENTS=OFF \
          -DWITH_BROKER=ON \
          -DWITH_TESTS=OFF \
          -DCMAKE_C_FLAGS="-DDISABLE_ENGINE_CLEANUP" . \
 && make

# Install the built binaries to the system paths
RUN cp src/mosquitto /usr/local/sbin/mosquitto && \
    cp lib/libmosquitto.so.1 /usr/local/lib/ && \
    ldconfig

# Set up mosquitto config
RUN mkdir -p /etc/mosquitto && \
    cp mosquitto.conf /etc/mosquitto/mosquitto.conf

# Grab node exporter for metrics collection
RUN wget https://github.com/prometheus/node_exporter/releases/download/v1.7.0/node_exporter-1.7.0.linux-amd64.tar.gz && \
    tar xvfz node_exporter-1.7.0.linux-amd64.tar.gz && \
    mv node_exporter-1.7.0.linux-amd64/node_exporter /usr/local/bin/ && \
    rm -rf node_exporter-1.7.0.linux-amd64*

# Create startup script that runs both node exporter and mosquitto
RUN echo '#!/bin/bash\n\
# Fire up node exporter in background for metrics\n\
/usr/local/bin/node_exporter &\n\
\n\
# Start mosquitto (this stays in foreground)\n\
exec /usr/local/sbin/mosquitto -c /etc/mosquitto/mosquitto.conf\n\
' > /entrypoint.sh && chmod +x /entrypoint.sh

# MQTT port and node exporter metrics port
EXPOSE 1883 9100

# Start everything up
CMD ["/entrypoint.sh"]
