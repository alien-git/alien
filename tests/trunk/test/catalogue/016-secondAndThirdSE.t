use strict;
use Test;


use AliEn::Service::SE;
use AliEn::X509;
use AliEn::UI::Catalogue;
BEGIN { plan tests => 1 }

{
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("user_basic/021-se") or exit(-2);
  

  my $config=new AliEn::Config;
  $config or print "Error getting the configuration!!\n" and exit(-2);

  my $host=$ENV{ALIEN_HOSTNAME};

  my $configKey="host=$host,ou=Config,ou=CERN,ou=Sites,$config->{LDAPDN}";

  print "BEFORE STARTING WE HAVE $config->{SE_NAME}\n";
  if ($config->{SE_NAME}!~ /testSE$/) {

    print "THIS IS NOT WHAT WE WANTED!! (let's try to fix it)\n";
    removeLdapEntry($configKey) or exit(-2);
    $config=$config->Reload({force=>1});
    if (!$config->{SE_NAME}=~ /testSE$/) {
      print "nope....\n";
      exit -2;
    }
  }
  my $key2="name=testSE2,ou=SE,ou=Services,$config->{FULLLDAPDN}";
  my $key3="name=testSE3,ou=SE,ou=Services,$config->{FULLLDAPDN}";


  print"ok\nGetting the subject of the certificate...";

  my $c=AliEn::X509->new();
  $c->load("$ENV{ALIEN_HOME}/globus/usercert.pem");
  my $subject=$c->getSubject();
  print "ok\n";
  addLdapEntry($key2, ["objectClass",["AliEnSE", "AliEnMSS", "AliEnSOAPServer"],
		      "name", "testSE2",
		      "host", $config->{HOST},
		      "mss", "File",
		      "savedir", "$config->{LOG_DIR}/SE2/DATA",
		      "port", 7093,
		      "certsubject",$subject,
                      "ioDaemons","file:host=$config->{HOST},port=7093",
		      'ftdprotocol','cp',
                      'QoS', 'disk',
		     ]) or exit(-2);

  addLdapEntry($key3, ["objectClass",["AliEnSE", "AliEnMSS", "AliEnSOAPServer"],
		      "name", "testSE3",
		      "host", $config->{HOST},
		      "mss", "File",
		      "savedir", "$config->{LOG_DIR}/SE3/DATA",
                      "ioDaemons","file:host=$config->{HOST},port=7094",
		      "port", 7094,
		      "certsubject",$subject,
		      'ftdprotocol','cp',
                      'QoS', 'tape',
		     ]) or exit(-2);


#  $config=$config->Reload({force=>1});
#  print "AFTER RELOADIN WE HAVE $config->{SE_NAME} (and $config->{HOST})\n";
#  $config->{SE_NAME}=~ /testSE2$/ or print "THIS IS NOT WHAT WE WANTED!!\n"
#    and exit -2;

  my $done=0;
  my $ui=AliEn::UI::Catalogue->new({role=>"admin"});
  if ($ui) {
    $ui->execute("resyncLDAP") and $done=1;
    $ui->execute("refreshSERankCache") and $done=1;
    $ui->close();
  }

  
  if (! $done) {
    print "Adding the SE didn't work!!!!\n";
#    removeLdapEntry($configKey);
    exit(-2);
  }

  print "ok\n";
  #$done=startService("SE");

  #Let's remove the hostconfig entry
  #removeLdapEntry($configKey) or exit(-2);


  print "Got ".($done || "undef") ."\n";

  $done or exit(-2);
  #$config=$config->Reload({force=>1});
  print "AT THE END WE HAVE $config->{SE_NAME} (and $config->{HOST})\n";
  $config->{SE_NAME}=~ /testSE$/ or print "THIS IS NOT WHAT WE WANTED!!\n"
    and exit -2;

  print "ok!!\n";

}




