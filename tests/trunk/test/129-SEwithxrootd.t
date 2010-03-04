use strict;
use Test;


use AliEn::Service::SE;
use AliEn::X509;
use AliEn::UI::Catalogue;
use AliEn::UI::Catalogue::LCM;
use Net::Domain qw(hostname hostfqdn hostdomain);

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

  print "Stopping previous SE ...";
  my $rez = stopService("SE");
  print $rez ? "ok\n" : "failed\n";
  
  my $key="name=testSExrootd,ou=SE,ou=Services,$config->{FULLLDAPDN}";

  print"ok\nGetting the subject of the certificate...";

  my $c=AliEn::X509->new();
  $c->load("$ENV{ALIEN_HOME}/globus/usercert.pem");
  my $subject=$c->getSubject();
  print "ok\n";
  addLdapEntry($key, ["objectClass", ["AliEnSE"],
		      "name", "testSExrootd",
		      "host", $config->{HOST},
		      "mss", "File",
		      "savedir", "$config->{LOG_DIR}/SExrootd/DATA",
		      "port", 7090,
		      "certsubject",$subject,
		      "iodaemons", "xrootd:port=5443",
		     ]) or exit(-2);

  addLdapEntry($configKey, ["objectClass", ["AliEnHostConfig"],
		      host=>$host,
		      se=>"testSExrootd",
		      logdir=>"$config->{LOG_DIR}/SExrootd/",
		     ]) or exit(-2);


  $config=$config->Reload({force=>1});
  print "AFTER RELOADIN WE HAVE $config->{SE_NAME} (and $config->{HOST})\n";
  $config->{SE_NAME}=~ /testSExrootd$/ or print "THIS IS NOT WHAT WE WANTED!!\n"
    and exit -2;

  my $done=0;
  my $ui=AliEn::UI::Catalogue->new({role=>"admin"});
  if ($ui) {
    $ui->execute("addSE", "-p", "cern", "testSExrootd") and $done=1;
    $ui->close();
  }
  if (! $done) {
    print "Adding the SE didn't work!!!!\n";
    removeLdapEntry($configKey);
    exit(-2);
  }

  print "ok\n";
  print "Starting SEwithXrootd...\n";
  $done=startService("SE");
  print "Got ".($done || "undef") ."\n";

  addFileToXrootd();

  print "Stopping SEwithXrootd...";
  $rez=stopService("SE");
  print $rez ? "ok\n" : "failed\n";

  #Lets remove the hostconfig entry
  removeLdapEntry($configKey) or exit(-2);

  print "Restarting previous SE...\n";
  $rez=startService("SE");
  print $rez ? "ok\n" : "failed\n";

  $done or exit(-2);
  $config=$config->Reload({force=>1});
  print "AT THE END WE HAVE $config->{SE_NAME} (and $config->{HOST})\n";
  $config->{SE_NAME}=~ /testSE$/ or print "THIS IS NOT WHAT WE WANTED!!\n"
    and exit -2;

  print "ok!!\n";

}

sub addFileToXrootd {
  my $host=Net::Domain::hostname();
  
  print "Creating a catalogue object....\n";
  my $cat=AliEn::UI::Catalogue::LCM->new({"role", "admin"});
  $cat or return 0;
  
  open(F, ">/tmp/xrootd.test") or print "Cannot create /tmp/xrootd.test\n" and return 0;
  print F "========\n";
  print F "Test file for xrootd.\n";
  print F "Current date:".`date`;
  print F "========\n";
  close(F);
  
  print "Getting current directory...\n";
  $cat->execute("pwd") or return 0;
  print "Creating directory to store the file...\n";
  $cat->execute("mkdir", "-p", "/$host/etc") or return 0;
  print "Trying to remove the file first...\n";
  $cat->execute("rm", "/$host/etc/xrootd.test");
  print "Adding file ...\n";
  $cat->execute("add", "/$host/etc/xrootd.test", "file://localhost/tmp/xrootd.test") or return 0;
  print "Finding file ...\n";
  $cat->execute("whereis", "/$host/etc/xrootd.test") or return 0;
  print "Content of the file was:\n";
  $cat->execute("cat", "/$host/etc/xrootd.test") or return 0;
  print "File operations were successfull!\n";
  unlink("/tmp/xrootd.test");
  return 1;
}

sub stopService {
  my $service = shift;

  my $cmd = "$ENV{ALIEN_ROOT}/bin/alien Stop$service";
  system($cmd);
  return 1;
}

