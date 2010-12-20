use strict;

use AliEn::UI::Catalogue;

my $c=AliEn::UI::Catalogue->new({role=>"admin"
                                }) or exit (-2);

my $lfn="~/pathToMove/";
print "Deleting a file that does not exist...\n";
$c->execute("rmdir","-silent", $lfn);

foreach my $op (
 ['mkdir', "-p","$lfn/remote"], 
  ["touch", "$lfn/file1"],
, ["touch", "$lfn/file2"], ["touch", "$lfn/remote/file3"],
  ['moveDirectory',"$lfn"],
  ["ls", "$lfn/file1"],["cd", "$lfn/remote"],
  ['moveDirectory',"-b $lfn"],
  ["ls", "$lfn/file1"],["cd", "$lfn/remote"],
  
) {
  print "\n\nLet's do now: '". join (" ", @$op) . "'...\n ";
  $c->execute(@$op) or print "Error doing @$op\n" and exit(-2);
}

print "ok\n"
