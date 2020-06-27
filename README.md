KairosDb
========

<a href="https://raw.githubusercontent.com/InscopeMetrics/kairosdb/master/LICENSE">
    <img src="https://img.shields.io/hexpm/l/plug.svg"
         alt="License: Apache 2">
</a>
<a href="https://travis-ci.com/InscopeMetrics/kairodb">
    <img src="https://travis-ci.com/InscopeMetrics/kairosdb.svg?branch=master"
         alt="Travis Build">
</a>
<a href="http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.inscopemetrics.kairosdb%22%20a%3A%22kairosdb%22">
    <img src="https://img.shields.io/maven-central/v/io.inscopemetrics.kairosdb/kairosdb.svg"
         alt="Maven Artifact">
</a>
<a href="https://hub.docker.com/r/inscopemetrics/kairosdb">
    <img src="https://img.shields.io/docker/pulls/inscopemetrics/kairosdb.svg" alt="Docker">
</a>

KairosDB is a fast distributed scalable time series abstraction on top of [Cassandra](https://cassandra.apache.org/).

Usage
-----

### Installation

#### Manual
The artifacts from the build are in *kairosdb/target/appassembler* and should be copied to an
appropriate directory on your application host(s).

#### Docker
If you use Docker, we publish a base docker image that makes it easy for you to layer configuration on top of.  Create
a Docker image based on the image inscopmetrics/kairosdb.  Configuration files are typically located at /opt/kairosdb/config/.
In addition, you can specify CONFIG_FILE (defaults to /opt/kairosdb/config/kairosdb.properties), LOGBACK_CONFIG (defaults to
"-Dlogback.configurationFile=/opt/kairosdb/config/logback.xml"), and JAVA_OPTS (defaults to "") environment variables to
control startup.

### Execution

In the installation's *bin* directory there are scripts to start Kairosdb: *kairosdb* (Linux) and
*kairosdb.bat* (Windows).  One of these should be executed on system start with appropriate parameters; for example:

    /usr/local/lib/kairosdb/bin/kairosdb -c run -p /usr/local/lib/kairosdb/config/kairosdb.properties

### Configuration

#### Logging

To customize logging you may provide a [LogBack](http://logback.qos.ch/) configuration file. The project ships with
`logback.xml` which writes logs to rotated files and with `logback-console.xml` which writes logs to STDOUT.

Outside of Docker, set the `JAVA_OPTS` environment variable to configure logging:

    export JAVA_OPTS="-Dlogback.configurationFile=/usr/local/lib/kairosdb/config/logback-console.xml"

Where */usr/local/lib/kairosdb/config/logger-console.xml* is the path to your logging configuration file.

Under Docker, set the `LOGBACK_CONFIG` environment variable to configure logging:

    docker run -e LOGBACK_CONFIG=/opt/kairosdb/config/logack-console.xml inscopemetrics/kairosdb:latest

#### Service

Please refer to the original KairosDb documentation linked below for information on how to configure KairosDb.

Origin
-----

## Reference Material

* [Documentation](http://kairosdb.github.io/website/)
* [Frequently Asked Questions](https://github.com/kairosdb/kairosdb/wiki/Frequently-Asked-Questions)
* [KairosDB Releases](https://github.com/kairosdb/kairosdb/releases)
* [Installation Instructions](http://kairosdb.github.io/docs/build/html/GettingStarted.html)
* [KairosDB Discussion Group](https://groups.google.com/forum/#!forum/kairosdb-group)

Development
-----------

To build the service locally you must satisfy these prerequisites:
* [Docker](http://www.docker.com/) (for [Mac](https://docs.docker.com/docker-for-mac/))

__Note:__ Requires at least Docker for Mac Beta version _Version 1.12.0-rc4-beta19 (build: 10258)_

Next, fork the repository, clone and build.

Unit tests and integration tests can be run from IntelliJ. Integration tests
require that the service and its dependencies be running (see below).

Building:

    kairosdb> ./jdk-wrapper.sh ./mvnw verify

To launch the service and its dependencies in Docker:

    kairosdb> ./jdk-wrapper.sh ./mvnw docker:start

To launch the service with remote debugging on port 9005 and its dependencies in Docker:

    kairosdb> ./jdk-wrapper.sh ./mvnw -Ddebug=true docker:start

To execute performance tests:

    kairosdb> ./jdk-wrapper.sh ./mvnw -PperformanceTest test

To use the local version in your project you must first install it locally:

    kairosdb> ./jdk-wrapper.sh ./mvnw install

You can determine the version of the local build from the pom.xml file.  Using the local version is intended only for
testing or development.

You may also need to add the local repository to your build in order to pick-up the local version:

* Maven - Included by default.
* Gradle - Add *mavenLocal()* to *build.gradle* in the *repositories* block.
* SBT - Add *resolvers += Resolver.mavenLocal* into *project/plugins.sbt*.

License
-------

Published under Apache Software License 2.0, see LICENSE

&copy; Inscope Metrics, 2020
