use strict;

use AliEn::UI::Catalogue;

my $cat=AliEn::UI::Catalogue->new({user=>"newuser"}) or exit(-2);


$cat->execute("rmdir", "-rf", "tabCompletion");
$cat->execute("mkdir", "-p", "tabCompletion") or exit(-2);

for my $file ("test1", "test2", "t.C") {
  $cat->execute("touch", "tabCompletion/$file") or exit(-2);
}

print "Let's start with the tabCompletion\n";

$cat->execute("cd", "tabCompletion");
check($cat, "test", ["test1", "test2"]) or exit(-2);

check($cat, "test", ["test1", "test2"]) or exit(-2);
check($cat, "t", ["test1", "test2", "t.C"]) or exit(-2);
check($cat, "t.", ["t.C"]) or exit(-2);



sub check {
  my $cat=shift;
  my $start=shift;
  my $result=shift;
  my @result=@$result;

  my @real=file_complete($cat->{CATALOG}, $start);
  print "Got @real\n Looking for @result\n";

  foreach my $file (@real){
    grep (/^$file$/, @result) or 
      print "The file $file wasn't expected!!\n" and return;
    @result=grep (! /^$file$/, @result);
  }
  if (@result){
    print "We were expecting also @result\n";
    return;
  }

  return 1;

}

# This function is copied-pasted from UI/Catalogue. We can't use the real 
# one because it gets the catalogue only when we do StartPrompt....
#

sub file_complete {
  my $catalog=shift;
  my $word=shift;
  my $path = $catalog->f_complete_path($word);
  $path or return;

  my ($dirname) = $catalog->f_dirname($path);

  $catalog->selectDatabase($dirname) or return;
  my @result=$catalog->{DATABASE}->tabCompletion ($dirname);
  $path =~ s{\.}{\\.}g;
  print "Looking for $path in $word\n";
  @result = grep (s/^$path/$word/, @result);


  ($#result)
    or ( $result[0] =~ /\/$/ )
      and return ( @result, $result[0] . "." );

  return @result;
  
}
