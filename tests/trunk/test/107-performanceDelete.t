use strict;

use AliEn::UI::Catalogue;

my $c=AliEn::UI::Catalogue->new({USER=>"admin", role=>"admin", 
				 USE_PROXY=>0, passwd=>"pass",
				}) or exit;

my $dir="/test/";

my @dirs=$c->execute("ls", $dir);
my $total=0;
my $before=time;
foreach my $entry (@dirs) {
  print "Deleting the entries in /test/$entry/\n";
  my $i=1;
  while (1) {
    $c->execute("rm", "$dir/$entry/file$i") or last;
    $i++;
    if ( $i%500 eq "0") {
      my $intermediate=time();
      my $insert=(1000.0*($intermediate-$before))/$i; 
      print "\tSo far, $i -> ". ($intermediate -$before) . " seconds ( $insert ms/delete)\n";
    }
  }
  $c->execute("rmdir", "-rf", "$dir/$entry/") or last;

  $total+=$i-1;
}

my $after=time();

my $insert=0;
$total and $insert=(1000.0*($after-$before))/$total; 
print "Deleting $total entries -> ". ($after -$before) . " seconds ( $insert ms/delete)\n";

$c->close();
