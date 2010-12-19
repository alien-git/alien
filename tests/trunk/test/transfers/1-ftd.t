use strict;
use Test;


use AliEn::Service::FTD;
use AliEn::X509;
use AliEn::UI::Catalogue;

$ENV{SEALED_ENVELOPE_REMOTE_PUBLIC_KEY}="$ENV{ALIEN_HOME}/authen/rpub.pem";
$ENV{SEALED_ENVELOPE_REMOTE_PRIVATE_KEY}="$ENV{ALIEN_HOME}/authen/rpriv.pem";
$ENV{SEALED_ENVELOPE_LOCAL_PUBLIC_KEY}="$ENV{ALIEN_HOME}/authen/lpub.pem";
$ENV{SEALED_ENVELOPE_LOCAL_PRIVATE_KEY}="$ENV{ALIEN_HOME}/authen/lpriv.pem";
$ENV{ALIEN_DATABASE_ROLE}='admin';
$ENV{ALIEN_DATABASE_PASSWORD}='pass';


BEGIN { plan tests => 1 }
{
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("user_basic/021-se") or exit(-2);

  my $config=new AliEn::Config;
  $config or print "Error getting the configuration!!\n" and exit(-2);

  my $key="name=testFTD,ou=FTD,ou=Services,$config->{FULLLDAPDN}";

  print"ok\nGetting the subject of the certificate...";

  my $c=AliEn::X509->new();
  $c->load("$ENV{ALIEN_HOME}/globus/usercert.pem");
  my $subject=$c->getSubject();
  print "ok\n";
  addLdapEntry($key, ["objectClass",["AliEnFTD", "AliEnSOAPServer"],
		      "name", "testFTD",
		      "host", $config->{HOST},
		      "port", 7091,
		      "certsubject",$subject,
		      maxTransfers=>10,
		      protocol=>["cp",'rm']
		     ]) or exit(-2);
  $config=$config->Reload({force=>1});

  print "Copying the certificate to $ENV{ALIEN_HOME}/identities.ftd\n";
  system("mkdir","-p","$ENV{ALIEN_HOME}/identities.ftd") and exit(-2);
  
  my $cert="$ENV{ALIEN_HOME}/identities.ftd/cert.pem";
  $key="$ENV{ALIEN_HOME}/identities.ftd/key.pem";
  -e $cert or link ("$ENV{ALIEN_HOME}/globus/usercert.pem", $cert);
  -e $key or link ("$ENV{ALIEN_HOME}/globus/userkey.pem", $key);

  my  $done=startService("FTD", {nolisten=>1});
  print "Got ".($done || "undef") ."\n";

  $done or exit(-2);

  print "ok!!\n";

}
