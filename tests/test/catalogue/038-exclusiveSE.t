use strict;
use Test;


use AliEn::Service::SE;
use AliEn::X509;
use AliEn::UI::Catalogue;
use AliEn::UI::Catalogue::LCM::Computer;
BEGIN { plan tests => 1 }

{
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;

  includeTest("user_basic/021-se") or exit(-2);


  my $file="exclusiveUserFlagTestOnSE";

  my $config=new AliEn::Config;
  $config or print "Error getting the configuration!!\n" and exit(-2);

#  my $key="name=exclusiveSE,ou=SE,ou=Services,$config->{FULLLDAPDN}";
   my $key="name=otherSE,name=testSE,ou=SE,ou=Services,$config->{FULLLDAPDN}";


  print"ok\nGetting the subject of the certificate...";

  my $c=AliEn::X509->new();
  $c->load("$ENV{ALIEN_HOME}/globus/usercert.pem");
  my $subject=$c->getSubject();
  print "ok\n";
#  addLdapEntry($key, ["objectClass",["AliEnSE", "AliEnMSS", "AliEnSOAPServer"],
#		      "name", "exclusiveSE",
#		      "host", "$config->{HOST}",
#		      "mss", "File",
#		      "savedir", "$config->{LOG_DIR}/exclSE/DATA",
#		      "port", "7097",
#		      "certsubject",$subject,
#                      "ioDaemons","file:host=$config->{HOST}:port=7097",
#		      'ftdprotocol','cp',
#                      'QoS', 'vip',
#		     ]) or exit(-2);

  addLdapEntry($key, ["objectClass", ["AliEnMSS"],
                      "name", "otherSE",
                      "mss", "File",
                      "Qos", "tape",
                      "ioDaemons","file:host=localhost:port=8062",
                      "savedir", "$config->{LOG_DIR}/OTHER_SE_DATA",
                     ]) or exit(-2);

  my $done=0;
  my $ui=AliEn::UI::Catalogue->new({role=>"admin"});
  if ($ui) {
    $ui->execute("resyncLDAP") and $done=1;
    $ui->execute("resyncLDAP") and $done=1;
    $ui->close();
  }
  if (! $done) {
    print "Adding the SE didn't work!!!!\n";
#    removeLdapEntry($configKey);
    exit(-2);
  }

  print "\nAll right, we added the SE, now lets add a file to it.\n\n";

  print "Got ".($done || "undef") ."\n";

  $done or exit(-2);


  my $filecat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser"});
  $filecat or exit (-1);

  $filecat->execute("rm", "$file");
  $filecat->execute("rm", $file."_copy");


  my $name="/tmp/testexclUser.$$";
  open (FILE, ">$name")
    or print "Error opening the file $name\n" and return;
  print FILE "Test file input for exclusive user test.";
  close FILE;

   my $vo=Net::Domain::hostname();
   chomp $vo;
#  $done=$filecat->execute("add", "$file", $name , "${vo}::cern::exclusiveSE");
   $done=$filecat->execute("add", "$file", $name , "${vo}::cern::otherSE");


  $filecat->execute("whereis", "-i", "-silent", $file) 
         or print "ERROR: We could not add a file for the test \n" and exit(-2);

  my ($fileoutput)=$filecat->execute("get", "-silent", "$file") or print "ERROR: We could not get the file.\n" and exit(-2);

  print "\nAll right, adding a test file worked, now let's make the SE exclusive.\n\n";

#  addLdapEntry($key, ["objectClass",["AliEnSE", "AliEnMSS", "AliEnSOAPServer"],
#		      "name", "exclusiveSE",
#		      "host", $config->{HOST},
#		      "mss", "File",
#		      "savedir", "$config->{LOG_DIR}/exclSE/DATA",
#		      "port", 7097,
#		      "certsubject",$subject,
#                      "ioDaemons","file:host=$config->{HOST}:port=7097",
#		      'ftdprotocol','cp',
#                      'QoS', 'vip',
#                      'seExclusiveWrite', 'NOT_U',
#                      'seExclusiveRead', 'NOT_U'
#		     ]) or exit(-2);

  addLdapEntry($key, ["objectClass", ["AliEnMSS"],
                      "name", "otherSE",
                      "mss", "File",
                      "Qos", "tape",
                      "ioDaemons","file:host=localhost:port=8062",
                      "savedir", "$config->{LOG_DIR}/OTHER_SE_DATA",
                      'seExclusiveWrite', 'NOT_U',
                      'seExclusiveRead', 'NOT_U'

                     ]) or exit(-2);



   $done=0;
  $ui=AliEn::UI::Catalogue->new({role=>"admin"});
  if ($ui) {
    $ui->execute("resyncLDAP") and $done=1;
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

  print "Trying to read the file...\n";

  ($fileoutput)=$filecat->execute("get", "-x", "$file") and print "ERROR: We were still able to get the file.\n" and exit(-2);
  
  print "Perfect it didn't work. No try to write a new file on the SE...\n";

  $file .= "_copy";
#  $filecat->execute("add", "$file", $name , "${vo}::cern::exclusiveSE") and print "ERROR: We are still able to write on the SE.\n" and exit(-2);
  $filecat->execute("add", "$file", $name , "${vo}::cern::otherSE") and print "ERROR: We are still able to write on the SE.\n" and exit(-2);


  $filecat->close();

#  print "Perfect, we could neither read nor write on ${vo}::cern::exclusiveSE after making it exclusive.\n";
  print "Perfect, we could neither read nor write on ${vo}::cern::otherSE after making it exclusive.\n";




  print "ok!!\n";

}
