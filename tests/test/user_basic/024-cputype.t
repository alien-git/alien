use strict;

use Data::Dumper;

use AliEn::Util;
use AliEn::Database::IS;
use AliEn::RPC;

$ENV{ALIEN_DATABASE_PASSWORD}="pass";
my $db=AliEn::Database::IS->new({ROLE=>'admin', 'DEBUG'=>5}) or exit(-2);


$db->do("truncate cpu_si2k");
print "Data inserted in the database\n";
my $self={CONFIG=>AliEn::Config->new(),
    RPC=>AliEn::RPC->new(),
};
AliEn::Util::setupApMon($self);

my $monit=$self->{MONITOR};
my $cpuType= $monit->getCpuType() or exit(-2);

$db->insert("cpu_si2k", {cpu_model_name=>'%', cpu_cache=>$cpuType->{cpu_cache}, 
                         cpu_Mhz=>$cpuType->{cpu_MHz}, si2k=>100}) or exit(-2);

$db->close();


$cpuType->{host}=$self->{CONFIG}->{HOST};

print Dumper($cpuType);


my ($done)=$self->{RPC}->CallRPC("IS","getCpuSI2k", $cpuType, $self->{CONFIG}->{HOST})
or exit(-2);

delete $ENV{ALIEN_DATABASE_PASSWORD};
print "SpecINT2k for this machine is ".$done."\n";
$done == 100 or print "This is not what we expected!!\n" and exit(-2);



