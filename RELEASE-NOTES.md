KairosDb Release Notes
======================

1.2.4 - TBD
---
* Refuse to startup with reporter ttl set to 0 and align ttl.
* Remove support for H2 datastore.

1.2.3 - July 22, 2020
------------------------
* Fixed all compiler warnings. Converted TestNG to JUnit.
* Configured GitHub releases.
* Update data cache only after successful write to Cassandra.
* Use reporter ttl in MetricReporterService.

1.2.2 - June 27, 2020
------------------------
* Initial release as gruop:artifact `io.inscopemetrics.kairosdb:kairosdb`.
* Forked from [ddimensia/kairosdb](https://github.com/ddimensia/kairosdb).
* Dependency changes from original (different transitive closures under Maven vs Ivy):
  * Replaces: `slf4j-api-1.7.25.jar` instead of `slf4j-api-1.7.2.jar`
  * Adds: `dom4j-1.6.1.jar` (Maven transitive of `org.agileclick.genorm:genormous:jar:1.6.4`)
  * Adds: `jffi-1.2.10.jar` (Maven transitive of `com.datastax.cassandra:cassandra-driver-core:jar:3.3.2`)
* Packaging changes from original:
  * Jar: 
    * Adds:
      * `LICENSE`
      * `logback.xml`
      * `META-INF/maven/io.inscopemetrics.kairosdb/kairosdb/pom.properties`
      * `META-INF/maven/io.inscopemetrics.kairosdb/kairosdb/pom.xml`
  * Docker
      * Configuration is in `/opt/kairosdb/config` instead of `/opt/kairosdb/conf`
      * `logback.xml` is in `/opt/kairosdb/config` instead of `/opt/kairosdb/conf/logging` 
      * No longer ships with `check-log-errors.sh`, `create_hbase_table.sh`, `kairosdb-service.sh` or `kairosdb.bat` in `bin`
  * Tar.gz:
      * Top level directory is no longer `kairosdb`
      * Configuration is in `./config` instead of `./kairosdb/conf`
      * `logback.xml` is in `./config` instead of `./kairosdb/conf/logging`
      * No longer ships with `check-log-errors.sh`, `create_hbase_table.sh`, `kairosdb-service.sh` or `kairosdb.bat` in `bin`
   * RPM:
      * Packaged with the standard Inscope Metrics launch scripts.
* Other changes:
  * Extensions should be added to `./lib/ext` regardless of distribution
  * The packaged configuration files place H2DB files in `./h2db` instead of `build/h2db`
  * The configuration files on the classpath during test place H2DB files in `./target/h2db` instead of `./build/h2db`
  * Log files are written to `logs` and not `log`

&copy; Inscope Metrics, 2020
