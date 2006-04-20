if [ -f /etc/rc.d/init.d/functions ]
then
  OLDPATH=$PATH
  . /etc/rc.d/init.d/functions
  PATH=$OLDPATH
else
echo_success()
{
    echo -n "                                       ok"
}
echo_failure()
{
    echo -n "                                       failed"
}
fi

#AliEnServices="Monitor SE FTD PackMan MonaLisa CE"
AliEnServices="Monitor SE PackMan MonaLisa CE"
AliEnCommand="$VO_ALICE_SW_DIR/alien/bin/alien"
ScriptsLocation="$VO_ALICE_SW_DIR/alien/scripts/lcg"
AliEnOptions="-silent"
Sleep=0


case "$1" in
    start)
	echo "Starting AliEn services: "
	function=Start
	Sleep=2
	;;
    stop)
	echo "Shutting down AliEn services: "
	function=Stop
	;;
    status)
	echo "Status of AliEn services: "
	function=Status
	;;
    restart)
	$0 stop
	$0 start
	exit 0
	;;
    *)
	echo "Usage: $0 {start|stop|restart|status}"
	exit 1
esac
shift

if [ -n "$1" ] ;
then
   AliEnServices="$*"
fi

for SERVICE in $AliEnServices
do
  echo -n "alien $function$SERVICE "
  $ScriptsLocation/lcgAliEn.sh $function$SERVICE $AliEnOptions >/dev/null
  error=$?
  sleep $Sleep
  if [ $error -ne 0 ] ;
  then
      echo  -n "ERROR $error"
      echo_failure
  else
      echo_success
  fi
  echo
done
