# Start from the official Flink image
FROM flink:1.19.0-scala_2.12

# Install Python3 and pip
USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-dev && \
    ln -s /usr/bin/python3 /usr/bin/python

# Optional: set JAVA_HOME if PyFlink needs it
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install PyFlink matching version
RUN pip3 install apache-flink==1.19.0

# Set default user back if needed
USER flink
