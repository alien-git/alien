
use Net::LDAP;
use strict;
use Net::Domain qw(hostname hostfqdn hostdomain);

my $h=Net::Domain::hostname();
my $host="$h:8389";
my $LDAPpassword="ldap-pass";
my $debug="";

my $LDAP;

Connect() or exit(-1);

if (! Search()) {
  print "Errotr!!\n";
  exit;
} 

fork();
Connect() or exit(-1);
if (! Search()) {
  print "Errotr!!\n";
  exit;
} 
if (! Search()) {
  print "Errotr!!\n";
  exit;
}

sub Connect(){
my $suffix=Net::Domain::hostdomain();print "\nthis is the suffix $suffix";

$suffix=~ s/\./,dc=/g;
$suffix="dc=$suffix";
  print "Connectingto LDAP server .........";
  $LDAP=Net::LDAP->new( $host, "onerror" => "warn",
			   "debug", $debug) or print "$@" and return;
  my $result=  $LDAP->bind( "cn=Manager,$suffix", 
			    password => $LDAPpassword );
  $result->code && print "failed\nCould not bind to LDAP-Server: ",$result->error and return;
  print "OK\n";


}
sub Search{

my $suffix=Net::Domain::hostdomain();
$suffix=~ s/\./,dc=/g;
$suffix="dc=$suffix";
  my $user="newuser";
  my $mesg = $LDAP->search(base   => "ou=People,o=$h,$suffix",
			   filter => "(uid=$user)");


  my $total = $mesg->count;
  print "GOT $total\n";

  return $mesg->count;
}
