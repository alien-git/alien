use strict;
use AliEn::Service::ClusterMonitor;

my $options={};
Getopt::Long::Configure("pass_through");
#First, let's see if we have to redirect the output
Getopt::Long::GetOptions($options,"logfile=s"  )  or exit;

my $config = AliEn::Config->new({logfile=>$options->{logfile}});


my $c=AliEn::Service::ClusterMonitor->new() or exit(-2);

print "We have a service!!\n";
$c->{SERVICE}="CMreport";

my $id=$c->forkCheckProcInfo() or exit(-2);
print "And we have forked the process: $id\n ";
system ("echo $id > $config->{LOG_DIR}/CMreport.pid");


