set -e
if [ "$1" = 0 ]; then
  /sbin/service kairosdb stop > /dev/null 2>&1
  /sbin/chkconfig --del kairosdb
fi
exit 0
