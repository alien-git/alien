use strict;

use AliEn::RPC;


my $s=AliEn::RPC->new();

$s or exit(-1);
print "let's stop the IS...\n";

my $alien="$ENV{ALIEN_ROOT}/bin/alien";
system("$alien StopIS") and print "Error stopping the IS\n";

system("$alien StatusIS") or print "Error: the IS is still alive!!\n" and exit(-2);


print "Now, let's try to connect to it\n";

my $fork=fork();
defined $fork or print "Error forking the process\n" and exit(-2);
if ($fork eq "0") {
  print "The child sleeps for a while\n";
  sleep (20);
  print "The child restarts the IS\n";
#  my $user=getpwuid($<);
  my $start="$alien StartIS";
  $< or $start="su - alienmaster -c '$start'";
  system($start) and  print "Error starting the IS (with $start)\n" and exit(-2);
  exit(0);

}
sub _timeout {
  alarm(0);
  print "ME MUERO\n";
  exit(1);
}
local $SIG{ALRM}=\&_timeout;
alarm(300);
my $return=$s->checkService("IS", "-retry");
print "AND GOT";
use Data::Dumper;
print Dumper($return);
 #or exit(2);
alarm(0);
print "Got $return\n";
exit(0);
