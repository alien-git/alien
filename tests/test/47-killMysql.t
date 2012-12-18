use strict;

use AliEn::UI::Catalogue;


$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;

includeTest("68-dbthreads") or exit(-2);


my $checkProcess=createCheckProcess(5);


my $c=AliEn::UI::Catalogue->new() or exit(-2);
my $during=`ps -Ao command |grep mysql |wc -l`;
chomp $during;
print "During the connection we have $during  instances\n";

my (@before)=$c->execute("ls");
$c->execute("debug", "Database");
system ("$ENV{ALIEN_ROOT}/etc/rc.d/init.d/alien-mysqld stop") and print "Error stopping mysql!!!\n" and exit(-2);

my $pid=fork();

if ($pid){
  my (@after)=$c->execute("ls");
  print "Tenemos @before and @after\n";
  sleep 3;
  kill 9, $checkProcess;
  while (@before) {
    my $b=shift @before;
    my $a=shift @after;
    $b eq $a or print "Error before there were @before, and now we have @after\n" and exit(-2);
  }

  exit(0);
}

sleep (30);
system ("$ENV{ALIEN_ROOT}/etc/rc.d/init.d/alien-mysqld start");
exit(0);
