# Copyright 2020 Inscope Metrics
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM openjdk:8u242-jdk-slim-buster

MAINTAINER inscopemetrics

EXPOSE 8080 4242
WORKDIR /opt/kairosdb

# Configuration
ENV JVM_XMS="64m"
ENV JVM_XMX="1024m"
ENV LOGBACK_CONFIG="-Dlogback.configurationFile=/opt/kairosdb/config/logback.xml"
ENV KAIROSDB_CONFIG="/opt/kairosdb/config/kairosdb.properties"
ENV JAVA_OPTS=""

# Build
RUN mkdir -p /opt/kairosdb/lib/ext && \
    mkdir -p /opt/kairosdb/logs && \
    mkdir -p /opt/kairosdb/config
ADD deps /opt/kairosdb/lib/
ADD bin /opt/kairosdb/bin/
ADD webroot /opt/kairosdb/webroot/
ADD config /opt/kairosdb/config/
ADD lib /opt/kairosdb/lib/

# Entry point
ENTRYPOINT [ \
    "/opt/kairosdb/bin/kairosdb", \
    "${LOGBACK_CONFIG}", \
    "-XX:+HeapDumpOnOutOfMemoryError", \
    "-XX:HeapDumpPath=/opt/kairosdb/logs/kairosdb.oom.hprof", \
    "-XX:+PrintGCDetails", \
    "-XX:+PrintGCDateStamps", \
    "-Xloggc:/opt/kairosdb/logs/kairosdb.gc.log", \
    "-XX:NumberOfGCLogFiles=2", \
    "-XX:GCLogFileSize=50M", \
    "-XX:+UseGCLogFileRotation", \
    "-Xms${JVM_XMS}", \
    "-Xmx${JVM_XMX}", \
    "-XX:+UseStringDeduplication", \
    "-XX:+UseG1GC", \
    "-Duser.timezone=UTC", \
    "--", \
    "-c", \
    "run", \
    "-p", \
    "${KAIROSDB_CONFIG}" ]
