use strict;

sub setDirectDatabaseConnection {
  print "CONECTING TO THE DATABASE DIRECTLY ...\n";
$ENV{SEALED_ENVELOPE_REMOTE_PUBLIC_KEY}="$ENV{ALIEN_HOME}/authen/rpub.pem";
$ENV{SEALED_ENVELOPE_REMOTE_PRIVATE_KEY}="$ENV{ALIEN_HOME}/authen/rpriv.pem";
$ENV{SEALED_ENVELOPE_LOCAL_PUBLIC_KEY}="$ENV{ALIEN_HOME}/authen/lpub.pem";
$ENV{SEALED_ENVELOPE_LOCAL_PRIVATE_KEY}="$ENV{ALIEN_HOME}/authen/lpriv.pem";
$ENV{ALIEN_DATABASE_ROLE}='admin';
$ENV{ALIEN_DATABASE_PASSWORD}='pass';
}
sub unsetDirectDatabaseConnection{
  delete  $ENV{SEALED_ENVELOPE_REMOTE_PUBLIC_KEY};
  delete  $ENV{SEALED_ENVELOPE_REMOTE_PRIVATE_KEY};
  delete  $ENV{SEALED_ENVELOPE_LOCAL_PUBLIC_KEY};
  delete  $ENV{SEALED_ENVELOPE_LOCAL_PRIVATE_KEY};
  delete  $ENV{ALIEN_DATABASE_PASSWORD};
  
  
}
sub includeTest {
  my $test=shift;
  my $testdir=($ENV{ALIEN_TESTDIR} or "/home/alienmaster/AliEn/t");
  open (FILE, "<$testdir/$test.t") or 
    print "Error: IN $testdir/$test.t is not here!!\n" and return;
  my @output=<FILE>;
  close FILE;
  my $file=join("",@output);
  my $sub="";
  while ( $file=~ s/^(.*)(\ssub .*)$/$1/s){
    $sub.=$2;
  }
#  print "Let's require $sub\n";
  eval "$sub" and print "Error evaluating $sub ($@)\n" and return;
  if ($@) {
    print "Error requiring $@\n";
    return;
  }
 
  return 1;
}
return 1;
