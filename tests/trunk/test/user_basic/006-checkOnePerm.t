use strict;

eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;

use AliEn::Catalogue::Basic;

use AliEn::Catalogue;
setDirectDatabaseConnection();
my $c=AliEn::Catalogue->new() or exit(-2);

my @list=(['r',0,0,0,0,1,1,1,1],['w',0,0,1,1,0,0,1,1],['x',0,1,0,1,0,1,0,1]);
foreach my $element (@list) {
  my ($operation, @values)=@$element;
  my $i=-1;
  print "Checking $operation privileges... ";
  while (@values){
    $i++;
    print "$i";
    my $expected=shift @values;
    my $real=$c->checkOnePerm($operation,$i,"");
    $real or $real=0;
    $real eq $expected and next;
    
    print "ERROR: CHECKIN THE PRIVILEGES $operation of $i, expected $expected and got $real\n";
    exit(-2);
  }
  print "ok\n";
}
