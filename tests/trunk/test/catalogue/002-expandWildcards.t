use AliEn::UI::Catalogue;
use strict;

my $c=AliEn::UI::Catalogue->new({user=>"newuser"}) or exit(-2);

$c->execute("rmdir", "-rf", "-silent", "wildcards");
$c->execute("mkdir", "-p", "wildcards") or exit(-2);

for (my $i=0;$i<10;$i++){
  $c->execute("mkdir", "-silent","-p", "wildcards/d$i") or exit(-2);
  for  (my $j=0; $j<10; $j++){
    $c->execute("touch", "-silent","wildcards/d$i/f$i$j") or exit(-2);
  }
}

my ($home)=$c->execute("pwd");
print "Ok, let's start with the wildcards\n";

my $allRef=[
	    {value=>"wildcards/%/%0", result=>["wildcards/d0/f00","wildcards/d1/f10","wildcards/d2/f20","wildcards/d3/f30","wildcards/d4/f40","wildcards/d5/f50","wildcards/d6/f60","wildcards/d7/f70","wildcards/d8/f80","wildcards/d9/f90",]},
	    {value=>"wildcards/", result=>["wildcards/"]},
	    {value=>"wildcards/%", result=>["wildcards/d0","wildcards/d1","wildcards/d2","wildcards/d3","wildcards/d4","wildcards/d5","wildcards/d6","wildcards/d7","wildcards/d8","wildcards/d9"]},
	    {value=>"wildcards/%/", result=>["wildcards/d0/","wildcards/d1/","wildcards/d2/","wildcards/d3/","wildcards/d4/","wildcards/d5/","wildcards/d6/","wildcards/d7/","wildcards/d8/","wildcards/d9/"]},
	    {value=>"w%/d2/%", result=>["wildcards/d2/f20","wildcards/d2/f21","wildcards/d2/f22","wildcards/d2/f23","wildcards/d2/f24","wildcards/d2/f25","wildcards/d2/f26","wildcards/d2/f27","wildcards/d2/f28","wildcards/d2/f29"]},	
	    {value=>"../../*/*/wildcards", result=>["wildcards"]},
	   ];


foreach my $ref (@$allRef){
  print "Searching for $ref->{value}\n";
  my @list=$c->{CATALOG}->ExpandWildcards($ref->{value});
#  print "Got @list\n";
  my @result=@{$ref->{result}};
  foreach my $item (@result){
    grep (/^$home$item$/,@list) 
      or print "Error $home$item is not in the result (@list)\n" and exit(-2);
    @list=grep (! /^$home$item$/,@list);
  }
  if (@list) {
    print "Error: we found too many solutions: @list\n";
    exit(-2);
  }
}
