#!/bin/env alien-perl

use strict;
use Test;
use AliEn::UI::Catalogue::LCM::Computer;
use Net::Domain qw(hostname hostfqdn hostdomain);

BEGIN { plan tests => 1 }

{
  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser"});
  $cat or exit (-1);


  $cat->execute("cd") or exit (-2);

  doVersioningTest($cat,"VersioningTest","original","change1","change2","change3") or exit(-2);

  print "OK!!\n"

}




sub doVersioningTest {
  my $cat=shift;
  my $lfn=shift;
  my $v0=shift;
  my $v1=shift;
  my $v2=shift; 
  my $v3=shift; 


  $cat->execute("rm", "-silent", $lfn);
  $cat->execute("rmdir","-rf", ".$lfn");

  addFileForVersioning($cat, $lfn, $v0) or print "level 0.0\n" and exit(-2);
  addFileForVersioning($cat, $lfn,$v1) or print "level 0.2\n" and exit(-2);
  addFileForVersioning($cat, $lfn,$v2) or print "level 0.3\n" and exit(-2);
  addFileForVersioning($cat, $lfn,$v3) or print "level 0.4\n" and exit(-2);

  print "Added all files, now let's check them.\n";

  checkContentOfFile($cat, $lfn, $v3) or print "level 1.0\n" and exit(-2);
  checkContentOfFile($cat, ".$lfn/v1.0", $v0) or print "level 1.1\n" and exit(-2);
  checkContentOfFile($cat, ".$lfn/v1.1", $v1) or print "level 1.2\n" and exit(-2);
  checkContentOfFile($cat, ".$lfn/v1.2", $v2) or print "level 1.3\n" and exit(-2);

  return 1; 
}

sub checkContentOfFile{
  my $cat=shift;
  my $lfn=shift;
  my $content=shift;

  my ($c)=$cat->execute("get", $lfn) or print "error in get\n" and exit(-2);
  open (FILE, "<$c") or print "Error opening the file $c ($lfn)\n"
    and exit(-2);
  my @filec = <FILE>;
  close (FILE);

  my $rcont = join("",@filec);

  $rcont eq $content or print "error in content comparison\n" and exit(-2);

  print "File $lfn has the content '$content'\n";

  return 1;
}



sub addFileForVersioning {
  my $cat=shift;
  my $file=shift;
  my $content=shift;
  my $options=(shift or "");
  $options .= "-versioning";
  print "Registering the file $file...";

  my $name="/tmp/test16.$$";
  open (FILE, ">$name")
    or print "Error opening the file $name\n" and return;
  print FILE $content;
  close FILE;

  my $done=$cat->execute("add", "$file", $name, $options);
  system("rm", "-f", "$name");
  $done or print "not possible to add file" and return;
  print "ok adding $file\n";
  return 1;
}


