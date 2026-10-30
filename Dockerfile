FROM ubuntu:22.04

# Need this so apt doesn't ask me questions during install
ENV DEBIAN_FRONTEND=noninteractive

# Get all the dependencies we need
RUN apt-get update && apt-get install -y \
    cmake \
    build-essential \
    libssl-dev \
    ca-certificates \
    libcjson-dev \
    libargon2-dev \
    sqlite3 \
    libsqlite3-dev \
    pkg-config \
    libcunit1-dev \
    xsltproc \
    docbook-xsl \
    iproute2 \
    iputils-ping \
 && rm -rf /var/lib/apt/lists/*

# Set up the working directory
WORKDIR /opt/mqtt_brokers/mqtt-dap-mosquitto

# Copy everything over
COPY . /opt/mqtt_brokers/mqtt-dap-mosquitto

# Build mosquitto (had to disable LTO and ENGINE_cleanup to get it working)
RUN cmake -DCMAKE_BUILD_TYPE=Release \
          -DWITH_LTO=OFF \
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

# Resource usage is read from the container (docker stats), so nothing else runs
# here: an in-container exporter would be billed to the broker.
EXPOSE 1883

CMD ["/usr/local/sbin/mosquitto", "-c", "/etc/mosquitto/mosquitto.conf"]
