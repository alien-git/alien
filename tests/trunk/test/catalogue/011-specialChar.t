use strict;

use AliEn::UI::Catalogue::LCM;


my $cat=AliEn::UI::Catalogue::LCM->new({user=>"newuser"}) or exit(-2);

my $config=new AliEn::Config;





$cat->execute("rmdir", "-rf", "~/specialChar");
$cat->execute("mkdir", "-p", "~/specialChar") or exit(-2);

#$cat->{LOGGER}->debugOn("Catalogue");

print "Let's register several files\n";
my @files=("file", "f\\*le", "f\\?le");
foreach my $file (@files) {
  print "Trying $file\n";
#  my @f=$cat->{CATALOG}->ExpandWildcards("specialChar/$_");
#  print "AFTER EXPANDING @f\n";
  $cat->execute("add", "-r", "specialChar/$file", "file://$config->{HOST}:8092//d", 22,"aeedf") or exit(-2);

}
print "TENGO @files\n";
foreach (@files){
  print "Make sure that all of them exist $_\n";
  $cat->execute("whereis", "specialChar/$_") or 
    print "specialChar/$_ doesn't exist\n" and exit(-2);
}
print "let's delete f\\*le\n";
$cat->execute("rm", "specialChar/f\\*le") or exit(-2);
print "And let's see what we have in the directory...\n";
my (@now)=$cat->execute("ls", "specialChar/") or exit(-2);

print "NOW we have '@now'\n";

foreach my $name("file", "f\\?le"){
  my $ctr = 0;
  foreach my $file(@now) {
    ($file eq $name) and $ctr=1;
  }
#grep (/^$name$/, @now) 
  $ctr or print "Error: $name is not in the list (@now)\n" and exit(-2);
  @now = grep { $_ ne $name } @now
#@now=grep (! /^$name$/, @now);
}
if ($#now>-1) {
  print "Error there are still items in the list: '@now'\n" and exit(-2);
}
print "ok\n";

