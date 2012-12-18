use strict;

use AliEn::Logger;
my $l=new AliEn::Logger();
$l or exit(-2);

print "Got the logger";
$l->info("TEST", "Printing a test message") or exit(-2);


my $file="/tmp/alien_tests/current/109-extraFile.out";
unlink $file;
print "Redirecting the output to $file\n";
$l->redirect($file) or print "error redirecting\n" and exit(-2);

print "Printing something in the file\n";
$l->info("TEST", "Info message in the file");

$l->redirect()or print "Error going back to the normal file\n" and exit(-2);

print "We are supposed to be back\n";
$l->info("TEST", "Info back from the file");

open (my $FILE, "<", $file) or print "Error opening $file\n" and exit(-2);
my @file=<$FILE>;
close $FILE;
unlink $file;

print "GOT
' @file'\n";
foreach my $message ("Printing something in the file", 
		     "Info message in the file") {
  grep (/$message/, @file) 
    or print "The message is not in the file\n" and exit(-2);
}
foreach my $message ("We are supposed to be back", 
		     "Info back from the file") {
  grep (/$message/, @file) and
     print "The message '$message' is not supposed to be in the file\n" and exit(-2);
}


print "OK!!\n";


