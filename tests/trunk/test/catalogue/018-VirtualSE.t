#!/bin/env alien-perl

use strict;
use Test;

use AliEn::Service::SE;
BEGIN { plan tests => 1 }
use AliEn::UI::Catalogue::LCM;
use Net::Domain qw(hostname hostfqdn hostdomain);

{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR} = "/home/alienmaster/AliEn/t";
  push @INC, $ENV{ALIEN_TESTDIR};
  require functions;

  includeTest("user_basic/021-se") or exit(-2);

  my $host   = Net::Domain::hostname();
  my $config = new AliEn::Config;
  $config or print "Error getting the configuration!!\n" and exit(-2);

  my $key = "name=otherSE,name=testSE,ou=SE,ou=Services,$config->{FULLLDAPDN}";

  print "ok\n";
  addLdapEntry(
	$key,
	[ "objectClass", ["AliEnMSS"], "name", "otherSE", "mss", "File", "Qos", "tape", "ioDaemons",
	  "file:host=localhost:port=8062",
	  "savedir", "$config->{LOG_DIR}/OTHER_SE_DATA",
	]
  ) or exit(-2);

  $config = $config->Reload({force => 1});
  print "ok\nCreating the database...";
  my $error = 0;
  my $ui = AliEn::UI::Catalogue->new({role => "admin"}) or $error = 1;

  $error or $ui->execute("resyncLDAP")         or $error = 1;
  $error or $ui->execute("resyncLDAP")         or $error = 1;
  $error or $ui->execute("refreshSEDistance") or $error = 1;
  $error or $ui->close();

  if ($error) {
	removeLdapEntry($key);
	exit(-2);
  }

  #  startService("SE") or $error=1;
  #  if ($error) {
  #    removeLdapEntry($key);
  #    exit(-2);
  #  }
  print "The service started without problems!!!\n";

  print "Let's try asking for a new name to the new SE\n";

  #  getNewNames();

  print "\n\nOK, let's try adding a couple of files\n";

  my $localfile = "/tmp/alien_test.$<";
  open(my $FILE, ">", $localfile) or print "Error opening the file $localfile\n" and exit(-2);
  print $FILE "Hello world\n";
  close $FILE;
  $ui = AliEn::UI::Catalogue::LCM->new({user => "newuser"}) or $error = 1;
  $error or $ui->execute("rm", "-rf", "seVirtual1", "seVirtual2", "seVirtual3", "seVirtual4");

  $error or $ui->execute("add", "seVirtual1", $localfile) or $error = 1;
  $error or $ui->execute("add", "seVirtual2", $localfile, "${host}::CERN::otherSE") or $error = 1;

  my $seVirtual3 = "$config->{LOG_DIR}/OTHER_SE_DATA/seVirtual3";

  #  my $seVirtual4 = "$config->{LOG_DIR}/OTHER_SE_DATA/seVirtual4";
  system("cp $localfile $seVirtual3");

  #  system("cp $localfile $seVirtual4");

  $error or $ui->execute("add", "-r", "seVirtual3", "file://localhost//$seVirtual3", 1025, "abc") or $error = 1;

  unlink $localfile;
  ($error) and exit(-2);

  my @whereis1 = $ui->execute("whereis", "seVirtual1") or exit(-2);
  my @whereis2 = $ui->execute("whereis", "seVirtual2") or exit(-2);
  my @whereis3 = $ui->execute("whereis", "seVirtual3") or exit(-2);
  $ui->close();
  ($whereis1[1] and $whereis2[1]) or print "One of the lfn doesn't have a pfn!!\n" and exit(-2);
  print "Comparing @whereis1 and @whereis2\n";
  $whereis1[0] eq $whereis2[0] and print "They are in the same SE!!\n" and exit(-2);
  $whereis1[1] =~ s{/[0-9]{2}/[0-9]{5}/[^/]*}{};

  $whereis2[1] =~ s{/[0-9]{2}/[0-9]{5}/[^/]*}{};
  ($whereis1[1] eq $whereis2[1]) and print "They are in the same directory $whereis1[1]\n" and exit(-2);
  print "the files are in different directories: '$whereis1[1]' and '$whereis2[1]'\n";

  #  $whereis3[0] eq $whereis4[0] and print "The registered files are in the same SE!!\n" and exit(-2);

  print "YUHUUUU!!!\n";
  ok(1);

}

sub getNewNames {
  my $s = new AliEn::SOAP;

  my $name = $s->CallSOAP("SE", "getFileName", 44) or exit(-2);

  my @file1 = $s->GetOutput($name);

  $name = $s->CallSOAP("SE", "getFileName", "otherSE", 44) or exit(-2);
  my @file2 = $s->GetOutput($name);

  print "Comparing $file1[0] and  $file2[0]\n";
  $file1[0] =~ s{[^/]*$}{};
  $file2[0] =~ s{[^/]*$}{};
  $file1[0] eq $file2[0] and print "The paths are identical!!!\n" and exit(-2);
  print "Comparing also $file1[3] and $file2[3]\n";
  $file1[3] =~ s{[^:]*$}{};
  $file2[3] =~ s{[^:]*$}{};
  $file1[3] eq $file2[3]
	and print "The methods to retrieve are identical  $file1[3]  $file2[3] (@file1) and (@file2)!!!\n"
	and exit(-2);
}

