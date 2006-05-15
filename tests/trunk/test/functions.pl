use strict;

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
