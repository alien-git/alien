use strict;

use AliEn::UI::Catalogue;

my $c=AliEn::UI::Catalogue->new({USER=>"admin", role=>"admin", 
#				 USE_PROXY=>0, passwd=>"pass",
				}) or exit;

my $dir="/test/";
$c->execute("silent") or exit(-2); 

my @dirs=$c->execute("ls", $dir);

my $before=time;
my $total=0;
foreach my $entry (@dirs) {
  print "Listing  files from /test/$entry/\n";
  my $i=1;
  while (1) {

    $c->execute("ls", "$dir/$entry/file$i") or last;
    $i++;
    if ( $i%500 eq "0") {
      my $intermediate=time();
      my $insert=(1000.0*($intermediate-$before))/$i; 
      print "\tSo far, $i -> ". ($intermediate -$before) . " seconds ( $insert ms/select)\n";
    }
    $i eq "2000" and print "QUITTING\n" and last
  }
  $total+=$i;
}

my $after=time();
my $insert=0;
$total and $insert=(1000.0*($after-$before))/$total; 

print "Listing $total entries -> ". ($after -$before) . " seconds ( $insert ms/select)\n";

$c->close();
