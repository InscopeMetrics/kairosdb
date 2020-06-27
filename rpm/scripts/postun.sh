set -e
if [ "$1" -ge 1 ]; then
  /sbin/service kairosdb condrestart > /dev/null 2>&1
fi
exit 0
