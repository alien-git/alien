use strict;
use AliEn::Database;
use Net::Domain qw(hostname hostfqdn hostdomain);

my $checkProcess=createCheckProcess(10);

my $host=Net::Domain::hostfqdn();
my $connect={DRIVER=>"mysql", HOST=>"$host:3307", DB=>"alien_system"};
my $cdirect={DRIVER=>"mysql", HOST=>"$host:3307", DB=>"alien_system",
	     USE_PROXY=>0, ROLE=>"admin",PASSWD=>"pass"};
print "Connecting\n";
my $proxy=AliEn::Database->new($connect) or exit;

print "Connecting\n";
my $direct=AliEn::Database->new($cdirect) or exit;

print "Connecting\n";

$proxy->{ll}="Proxy connection";

my $i=0;
my $db=$direct;
while ($i<10){
  my $j=0;
  my @processes;
  while ($j<10) {
    my $pid=fork();
    
    if (! $pid) {

      #if the query doesn't work, let's sleep so the timeout kills us
      #and the test fails
      doQuery($db, $j,$i)
	or sleep(100);
      $db->close();
#      print "Client $i closed\n";
      exit();
    }
    push @processes, $pid;
    $j++;
  }
#  sleep(1);
#  print "AFTER FORKING\n";
#  system("ps -Ao command |grep Proxy |grep -v grep");

  my $wait=1;
  my $pid;
  use AliEn::Authen::ClientVerifier;
  local $SIG{ALRM} =sub {
    print localtime() . " One of the processes ($wait of @processes) does not finish (let's kill them):(\n";
    system("ps -Ao command -w -w |grep Proxy |grep -v grep");
    system("ps -A --forest -f -w -w");
    my $mpid=($AliEn::Authen::ClientVerifier::SERVERPID || "");
    print "PLEASE, CHECK $processes[$wait-1] (server $mpid)\n";
    sleep 40;
    kill 9, @processes;
    die("nope  ");
  };
  alarm(50);
#  doQuery($db, "master",$i) or exit(-2);

  foreach $pid (@processes) {
    print localtime() . " Waiting for $pid...\n";
    waitpid($pid, 0);
    print localtime() . " Waiting for $pid succeeded.\n";
    $wait++;
  }
  $i++;
  print "DONE\n\n\n\n";
#  sleep 10;
}
kill 9, $checkProcess;

sub createCheckProcess {
  my $minutes=shift;
  my $Mpid=$$;
  my $check=fork();
  $check and return $check;
    #This process is just going to wait and kill everything if there is a timeout
  my $time=0;
  while ($time<$minutes) {
    sleep (60);
    print "Checking if $Mpid is still alive\n";
    kill( 0,$Mpid) or exit();
    
    $time++;
  }
 my $date=time;
  print "Let's kill $Mpid $date\n";
  kill( 9, $Mpid);
  exit();

}
sub doQuery{
  my $db=shift;
  my $i=shift;
  my $j=shift;
  my $time=time;
  
  my $total=$db->query("SELECT * from L0L, L0L as D1, L0L as D2 limit 10000") or return;
  my $after=time;
  print localtime() . " $$ Query $i of $j done in ". ($after-$time). " seconds (got $total and $#{$total})\n";
  if (  $#{$total} < 0) {
    print "**************************************the query did not work :( \n";
    my $time=time;
    my $total=$db->query("SELECT * from L0L, L0L as D1, L0L as D2 limit 10000") or return;
    my $after=time;
    print localtime() . " $$ Query $i of $j done in ". ($after-$time). " seconds (got $total and $#{$total})\n";
    return;
  }
  return 1;
}
