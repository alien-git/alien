use strict;

use AliEn::UI::Catalogue::LCM::Computer;

my $id=shift;
my $secondId=shift;

$secondId or print "Error getting the job id\n" and exit(-2);

my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user"=>"newuser"}) or exit(-2);


print "The job $id should fail\n";
my ($status)=$cat->execute("top", "-id", $id);
$status->{statusId} eq "ERROR_E" or print "The job didn't fail!!\n" and exit(-2);

print "The job $secondId should work\n";
 ($status)=$cat->execute("top", "-id", $secondId);
$status->{statusId} eq "DONE" or print "The job didn't work!!!\n" and exit(-2);

print "OK!!\n";
