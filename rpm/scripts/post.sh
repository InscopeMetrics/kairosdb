set -e
/sbin/chkconfig --add kairosdb
mkdir -p /opt/kairosdb/logs
chown kairosdb:kairosdb /opt/kairosdb/logs
mkdir -p /opt/kairosdb/config
mkdir -p /var/run/kairosdb
chown kairosdb:kairosdb /var/run/kairosdb
