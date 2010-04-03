use strict;
use Test;


use AliEn::Service::SE;
use AliEn::X509;
use AliEn::UI::Catalogue;
BEGIN { plan tests => 1 }

{
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("14-se") or exit(-2);
  

  my $config=new AliEn::Config;
  $config or print "Error getting the configuration!!\n" and exit(-2);

  my $key="name=exclusiveSE,ou=SE,ou=Services,$config->{FULLLDAPDN}";

  print"ok\nGetting the subject of the certificate...";

  my $c=AliEn::X509->new();
  $c->load("$ENV{ALIEN_HOME}/globus/usercert.pem");
  my $subject=$c->getSubject();
  print "ok\n";
  addLdapEntry($key, ["objectClass",["AliEnSE", "AliEnMSS", "AliEnSOAPServer"],
		      "name", "exclusiveSE",
		      "host", $config->{HOST},
		      "mss", "File",
		      "savedir", "$config->{LOG_DIR}/exclSE/DATA",
		      "port", 7097,
		      "certsubject",$subject,
		      'ftdprotocol','cp',
                      'QoS', 'vip',
		     ]) or exit(-2);

#  $config=$config->Reload({force=>1});
#  print "AFTER RELOADIN WE HAVE $config->{SE_NAME} (and $config->{HOST})\n";
#  $config->{SE_NAME}=~ /testSE2$/ or print "THIS IS NOT WHAT WE WANTED!!\n"
#    and exit -2;

  my $done=0;
  my $ui=AliEn::UI::Catalogue->new({role=>"admin"});
  if ($ui) {
    $ui->execute("resyncLDAP") and $done=1;
    $ui->close();
  }
  if (! $done) {
    print "Adding the SE didn't work!!!!\n";
#    removeLdapEntry($configKey);
    exit(-2);
  }

  print "\nAll right, we added the SE, now lets add a file to it.\n\n";
  #$done=startService("SE");
  #$done=startService("SE");
  #$done=startService("SE");

  #Let's remove the hostconfig entry
  #removeLdapEntry($configKey) or exit(-2);


  print "Got ".($done || "undef") ."\n";

  $done or exit(-2);

  my $cat=AliEn::UI::Catalogue::LCM->new({"role", "admin"});
  $cat or exit (-1);

  my $file="exlusiveUserFlagTestOnSE";
  my $name="/tmp/testexclUser.$$";
  open (FILE, ">$name")
    or print "Error opening the file $name\n" and return;
  print FILE "Test file input for exclusive user test.";
  close FILE;

   my $vo=Net::Domain::hostname();
   chomp $vo;
   $done=$cat->execute("add", "$file", $name , "${vo}::cern::exclusiveSE");

  $cat->execute("whereis", "-i", "-silent", $file) 
         or print "ERROR: We could not add a file for the test \n" and exit(-2);

  my ($fileoutput)=$cat->execute("get", "-silent", "$file") or print "ERROR: We could not get the file.\n" and exit(-2);

  print "\nAll right, adding a test file worked, now let's make the SE exclusive.\n\n";

  addLdapEntry($key, ["objectClass",["AliEnSE", "AliEnMSS", "AliEnSOAPServer"],
		      "name", "exclusiveSE",
		      "host", $config->{HOST},
		      "mss", "File",
		      "savedir", "$config->{LOG_DIR}/exclSE/DATA",
		      "port", 7097,
		      "certsubject",$subject,
		      'ftdprotocol','cp',
                      'QoS', 'vip',
                      'exclusiveUsers', 'NOT_U'
		     ]) or exit(-2);

   $done=0;
  $ui=AliEn::UI::Catalogue->new({role=>"admin"});
  if ($ui) {
    $ui->execute("resyncLDAP") and $done=1;
    $ui->close();
  }
  if (! $done) {
    print "Adding the SE didn't work!!!!\n";
#    removeLdapEntry($configKey);
    exit(-2);
  }

  print "Got ".($done || "undef") ."\n";

  $done or exit(-2);

  print "\nAll right, SE is exclusive now and should be accessable only to 'NOT_U'.\n\n";


  ($fileoutput)=$cat->execute("get", "-silent", "$file") and print "ERROR: We were still able to get the file.\n" and exit(-2);
  
  $file .= "_copy";
  $cat->execute("add", "$file", $name , "${vo}::cern::exclusiveSE") and print "ERROR: We are still able to write on the SE.\n" and exit(-2);


  print "Perfect, we could neither read nor write on ${vo}::cern::exclusiveSE after making it exclusive.\n";



  print "ok!!\n";

}
