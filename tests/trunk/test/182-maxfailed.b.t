use strict;

use AliEn::UI::Catalogue::LCM::Computer;


my $id=shift;
$id or print "Error getting the id of the job\n" and exit(-2);

my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit (-1);

my ($status)=$cat->execute("top", "-id", $id);
print "The job is $status->{status}\n";

$status->{status}=~ /DONE/ or exit(-2);
my ($log)=$cat->execute("ps", "trace", $id);
my $found=0;
foreach my $entry (@$log){
  if ($entry->{trace}=~ /There were too many subjobs failing/){
    $found=1;
    last;
  }
};
$found or print "There is nothing in the log about being killed\n" and exit(-2);
print "ok!!\n";


