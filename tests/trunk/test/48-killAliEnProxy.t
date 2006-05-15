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

system ("$ENV{ALIEN_ROOT}/bin/alien StopProxy");

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
my $command="$ENV{ALIEN_ROOT}/bin/alien StartProxy <\$HOME/.alien/.startup/.passwd.$c->{CONFIG}->{ORG_NAME}";
$< or $command="su - alienmaster -c '$command'";
print "Doing $command";
system($command);
exit(0);
