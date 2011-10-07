use strict;

use AliEn::UI::Catalogue::LCM::Computer;

push @INC, $ENV{ALIEN_TESTDIR};
require functions;

includeTest("job_manual/010-ProcessMonitorOutput") or exit(-2);

my $admin = AliEn::UI::Catalogue::LCM::Computer->new({role => "admin"}) or exit(-2);

my $host = $admin->{CONFIG}->{HOST};
my ($info) = $admin->execute("queueinfo") or exit(-2);

print "According to queueinfo, we have $info\nLet's check if top has the same values...";
my @status = @{AliEn::Util::JobStatus()};

my @sites = @{$info};
my $site  = shift @sites;
print "THE SITE IS $site->{site}\n";
foreach my $status (@status) {
  my $top = $admin->execute("top", "-site", $site->{site}, "-status $status", "-silent");
  print "$status jobs: according to top, $top. According to jobInfo $site->{$status}\n";
  $top eq $site->{$status} or print "The number is not the same!!\n" and exit(-2);
}

print "ok!\n";
