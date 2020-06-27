set -e
getent group kairosdb >/dev/null || groupadd -r kairosdb
getent passwd kairosdb >/dev/null || \
    useradd -r -g kairosdb -d /opt/kairosdb -s /sbin/nologin \
    -c "Account used for isolation of kairosdb" kairosdb
