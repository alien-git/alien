use strict;

use AliEn::UI::Catalogue;
my $config=AliEn::Config->new() or exit(-2); 

my $vo=$config->{ORG_NAME};
my $host=$config->{HOST};


open(FILE, ">$ENV{ALIEN_HOME}/$vo.conf") or print "Error opening the file\n" and exit(-2);
print FILE "<PACKMAN test>
INSTALLDIR /tmp/my_packman/packages
</PACKMAN>
OTHER_VARIABLE sipe
";
close FILE or exit(-2);
eval {
  defineLocal("overwrite");
  $config=$config->Reload({force=>1}) or die("no catalogue");
  my $cat=AliEn::UI::Catalogue->new({user=>"newuser"}) or die("\n");
  
  my ($d)=$cat->execute("echo", "PACKMAN_INSTALLDIR") or die("\n");
  my ($d2)=$cat->execute("echo", "OTHER_VARIABLE") or die("\n");
  $cat->close();
  print "The directory is $d (and $d2)\n";
  
  $d=~ m{^/tmp/my_packman/packages$} or print "This is not the directory that is supposed to be!!\n" and die("\n");
  $d2 or print "We can't define new variables\n" and die("\n");
  
  print "Let's try adding\n";
  defineLocal("add");
  $config=$config->Reload({force=>1}) or die("\n");
  
  $d=$config->{PACKMAN_INSTALLDIR};
  $d2=$config->{OTHER_VARIABLE};
  
  $d and $d=~ m{^/tmp/my_packman/packages$} 
    and print "This is not the directory that is supposed to be!!\n" and die("\n");
  
  $d2 or print "We can't define new variables\n" and die("\n");
  
  print "Finally, with any other value\n";
  defineLocal("other");
  $config=$config->Reload({force=>1}) or die("\n");
  
  $d=$config->{PACKMAN_INSTALLDIR};
  $d2=$config->{OTHER_VARIABLE};
  
  $d and $d=~ m{^/tmp/my_packman/packages$} and print "This is not the directory that is supposed to be!!\n" and die("\n");
  $d2 and print "We can define new variables\n" and die("\n");
};
my $error=$@;
unlink "$ENV{ALIEN_HOME}/$vo.conf";

($error) and exit(-2);

print "ok\n";


sub defineLocal{
  my $status=shift;

  my $ldap= Net::LDAP->new("$host:8389", "onerror" => "warn") 
    or print "failed\nError conecting to the ldap server\n $? and $! and  $@\n" 
      and exit (-3);
my $suffix=Net::Domain::hostdomain();
$suffix=~ s/\./,dc=/g;
$suffix="dc=$suffix";
  my $result=$ldap->bind("cn=manager,$suffix", "password" => "ldap-pass");
  $result->code && print "failed\nCould not bind to LDAP-Server: ",$result->error 
    and exit (-4);

  my $mesg=$ldap->modify( $config->{FULLLDAPDN}, replace=>{"localconfig", $status});
  $mesg->code && print "failed\nCould not modify: ",$result->error and exit (-5);
  $ldap->unbind();
  return 1;
}
