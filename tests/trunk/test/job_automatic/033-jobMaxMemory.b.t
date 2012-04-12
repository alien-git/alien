use strict;

use AliEn::UI::Catalogue::LCM::Computer;

my $jobid=shift;
$jobid or print "Error getting the id of the job\n" and exit(-2);
my $jobrun=shift;
$jobrun  or print "Error getting the id of the job that run\n" and exit(-2);
my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",})
  or exit (-1);

my ($status)=$cat->execute("top", "-id", $jobid);
print "The job is $status->{status}\n";
$status->{status}=~ /ERROR_E/ or print "The job $jobid didn't fail!! It used too much memory!!!\n" and exit(-2);

 ($status)=$cat->execute("top", "-id", $jobrun);
print "The job is $status->{status}\n";
$status->{status}=~ /DONE/ or print "The job $jobrun failed!! Why???\n" and exit(-2);

ok(1)
