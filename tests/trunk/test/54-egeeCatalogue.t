use strict;
use AliEn::SOAP;
use AliEn::XML;

my $xml=AliEn::XML->new();
my $soap=AliEn::SOAP->new();

$soap or exit(-2);

my $config=AliEn::Config->new();
$config or exit(-2);

my $error_codes = {OK          => "0",
		   ERROR => "1",};

#$soap->{LOGGER}->debugOn();
my $catAddress="http://lxn5218.cern.ch:9001",;
my $replicaAddress="http://aliendev.cern.ch:9998",;

print "Connected!!\n";
my $catalogue=1;
my $metadata=0;
my $useXML=1;

my $user;
if (exists $ENV{ALIEN_USER}) {
	$user = $ENV{ALIEN_USER};
}
else {
	$user=getpwuid ($<);
}
print "User $user\n";
my $homeDir="$config->{USER_DIR}/". substr( $user, 0, 1 )."/$user";

$soap->Connect({uri=>"AliEn/EGEE/Service/Catalogue",
		address=>$catAddress,
                name=>"EGEE_CAT"
               }
              );

if ($catalogue){
  print "TENGO $soap->{EGEE_CAT}\n";

  my @calls=(
	     [{},"ls", "$homeDir"], 
	     [{},"mkdir", "$homeDir/newDir"],
	     [{},"rmdir", "$homeDir/newDir"],
	     [{},"touch", "$homeDir/emptyFile"],
	     [{status=>$error_codes->{ERROR}},
	      "touch", "$homeDir/emptyFile"],
	     [{},"rm", "$homeDir/emptyFile"],
	     [{}, "execute", "ls /;ls /private"]);

  my $s="mkdir  $homeDir/example";
  for (my $i=0; $i<70;$i++) {
    $s.=";mkdir $homeDir/example/test$i";
  }

  push @calls, ([{},"execute", $s],
		[{Entries=>70}, "ls","$homeDir/example",],
		[{Entries=>20}, "ls","$homeDir/example",undef, 20,],
		[{Entries=>40}, "ls","$homeDir/example",undef, undef,30,],
	       [{}, "rmdir","-r", "$homeDir/example"],);
  
  
  foreach my $call (@calls) {
    print "Doing @$call\n";
    CallFunction("EGEE_CAT", @$call) or exit(-1);

  }
}


if ($metadata){
  $soap->Connect({uri=>"AliEn/EGEE/Service/MetaCatalogue",
		    address=>$catAddress,
		    name=>"EGEE_META_CAT"
		   }
		  );

  my $taggedDir="$homeDir/tagged";
  my @calls=(
	     ["EGEE_CAT", {}, "mkdir", $taggedDir],
	     ["EGEE_META_CAT",{},"defineMetadataStructure", "test", "patient char(100)"],
	     ["EGEE_CAT", {}, "touch", "$taggedDir/file1"],
	     ["EGEE_META_CAT",{Entries=>0},"getMetadata", $taggedDir ],
	     ["EGEE_META_CAT", {}, "addMetadata", $taggedDir, "test"],
	     ["EGEE_META_CAT",{Entries=>1},"getMetadata", $taggedDir ],

	     ["EGEE_META_CAT",{},"addMetadataValues", "$taggedDir/file1", "test", "patient='pablo'" ],
	     ["EGEE_META_CAT",{},"getMetadataValues", "$taggedDir/file1", "test" ],
#	     ["EGEE_CAT", {}, "rm", "/private_egee/tags/test"],
#	     ["EGEE_CAT", {}, "rmdir", $taggedDir],
	    );

  foreach my $call (@calls) {
    CallFunction(@$call) or exit(-2);
  }

}




sub CallFunction{
  my $service=shift;
  my $options=shift or {};

  print "Calling @_ in $service\n";

  my $r=$soap->CallSOAP($service, @_);
#  use Data::Dumper;
#print Dumper($r);
  $soap->checkSOAPreturn($r) or return;
  my @s=$soap->GetOutput($r) or return;
  print "Got @s\n";
  my $status=($options->{status} or $error_codes->{OK});
  $options->{status} and print "This call was supposed to return $status\n";
  if ($useXML) {
    my $tree=$xml->parse($s[0]);
    if (!$tree) {
      print "ERROR $@";
      return;
    }
    my @list=@{$tree};
    my $value=$xml->getXMLFirstValue("ReturnValue", @list);
    if ($value ne $status ) {
      print "The status is not $status :( \n";
      return;
    }
    if (defined $options->{Entries}) {
      print "Checking that $options->{Entries} were returned...";
      my $v=$xml->getXMLFirstValue("Entries", @list);
      ($v eq $options->{Entries}) or print "NOPE!! (got $v)\n" and return;
      print "ok\n";
    }
    if (defined $options->{Body}) {
      print "Checking that the Body is $options->{Body}...";
      my $v=$xml->getXMLFirstValue("Body", @list);
      ($v eq $options->{Body}) or print "NOPE!! (got $v)\n" and return;
      print "ok\n";
    }
  }
  print "\n\n";
  return 1;
}
