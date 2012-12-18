use strict;


my $file="/tmp/unini.$$";
my $d;

print "Redirecting the output to $file\n";

open my $SAVEOUT,  ">&", STDOUT;
open my $SAVEERR, ">&", STDERR;

open (STDOUT, ">", $file) or print "Error opening $file\n" and exit(-2);
open STDERR, ">&", STDOUT;

print "Checking if there are messages\n";
print $d;
print "DONE\n";

open STDOUT, ">&", $SAVEOUT;
open STDERR, ">&", $SAVEERR;

print "done\n";

open (my $FILE, "<", $file) or print "Error reading the file $file\n" and exit(-2);


my $content=join ("", <$FILE>);
unlink $file;

$content =~ /Use of uninitialized/ or print "There is no error message!!(got only '$content')\n" and exit(-2);
print "ok!!\n";

