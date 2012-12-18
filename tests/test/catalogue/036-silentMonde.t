use strict;

use AliEn::UI::Catalogue::LCM;

my $file="/tmp/test_114.$<.$$";
my $ui=AliEn::UI::Catalogue::LCM->new ({user=>"newuser"}) or exit(-2);

$ui->{LOGGER}->redirect($file);
#open SAVEOUT,  ">&STDOUT";
#open (STDOUT, ">$file") or print "Error opening the file $file\n" and exit(-2);

my $done=$ui->execute("ls", "/bin");
$ui->execute("blabla");
my $done2=$ui->execute("cat", "/bin/date");
$ui->execute("blabla2");
my $done3=$ui->execute("ls", "/bin");
my $done4=$ui->execute("silent");
$ui->execute("blabla3");
my $done5=$ui->execute("ls", "/bin");
$ui->close();
#open (STDOUT,">&SAVEOUT");
$ui->{LOGGER}->redirect();

print "Let's read the output\n";

open (FILE, "<$file") or print "Error reading $file\n" and exit(-2);
my $c=join("", <FILE>);
close FILE;
unlink $file;
print "GOT $c\n";
($done and $done2 and $done3 ) or print "One of the commands failed!!!\n" and exit(-2);

$c=~ /^(.*)Unknown command: blabla\s/s or print "'blabla' not found\n" and exit(-2);
my $before=$1;

$c=~ /Unknown command: blabla2\s+(.*)$/s or print "'blabla' not found\n" and exit(-2);
my $after=$1;

$c=~ /blabla3/s and print "The silent didn't work!!\n" and exit(-2);

$before eq $after or print "Error: the print did not return the same (before we had '$before' and after '$after')\n" and exit(-2);

$c=~ /And the file is/s and print "The cat printed too much!!\n" and exit(-2);

print "OK!!\n"
;

