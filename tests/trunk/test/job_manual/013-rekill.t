use strict;


use AliEn::UI::Catalogue::LCM::Computer;

my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser"}) or exit(-2);

print "First, let's submit a job\n";
my ($id)=$cat->execute("submit", "jdl/date.jdl") or exit(-2);

print "Now, let's kill it\n";
$cat->execute("kill", $id) or exit(-2);
print "Let's get the status to see that is really killed\n";
my ($status)=$cat->execute("top", "-id",$id);
($status and $status->{statusId} != "KILLED" ) 
  and  print "The job is not dead $status->{statusId}!!\n" and exit(-2);
print "And now, let's kill it again (it should complain):\n";
$cat->execute("kill", $id) and exit(-2);

print "DONE!!\n";
