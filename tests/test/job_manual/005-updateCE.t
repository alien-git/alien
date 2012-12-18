use strict;
use AliEn::Database::TaskQueue;
use Net::Domain qw(hostname hostfqdn hostdomain);

$ENV{SEALED_ENVELOPE_REMOTE_PUBLIC_KEY}="$ENV{ALIEN_HOME}/authen/rpub.pem";
$ENV{SEALED_ENVELOPE_REMOTE_PRIVATE_KEY}="$ENV{ALIEN_HOME}/authen/rpriv.pem";
$ENV{SEALED_ENVELOPE_LOCAL_PUBLIC_KEY}="$ENV{ALIEN_HOME}/authen/lpub.pem";
$ENV{SEALED_ENVELOPE_LOCAL_PRIVATE_KEY}="$ENV{ALIEN_HOME}/authen/lpriv.pem";
$ENV{ALIEN_DATABASE_ROLE}='admin';
$ENV{ALIEN_DATABASE_PASSWORD}='pass';


print "Connecting to ldap...";
my $host=Net::Domain::hostfqdn();
my $port=$ENV{ALIEN_MYSQL_PORT} ||3307;
my $d=AliEn::Database::TaskQueue->new({DRIVER=>"mysql", HOST=>"$host:$port", DB=>"processes", "ROLE", "admin", 				      }) 
  or print "Error connecting to the database\n" and exit(-2);

my $number=$d->queryValue("SELECT maxjobs FROM HOSTS where hostName='$d->{CONFIG}->{HOST}'");
if (!$number) {
  print "Error getting the maxjobs\n Let's sleep for a while and try again\n";
  sleep (30);
  $d->{LOGGER}->debugOn();
  $number=$d->queryValue("SELECT maxjobs FROM HOSTS where hostName='$d->{CONFIG}->{HOST}'");
  $d->{LOGGER}->debugOff();
  if (!$number) {
    print "Let's see what we have in the HOSTS table\n";
    my $data=$d->query("SELECT * FROM HOSTS");
    use Data::Dumper;
    print Dumper($data);
    exit(-2);
  }
  print "And now we have $number\n";

}

changeNumberJobs(++$number);

print "Let's sleep for a while and check if the number is updated...";
sleep(90);
my $newNumber=$d->queryValue("SELECT maxjobs FROM HOSTS where hostName='$d->{CONFIG}->{HOST}'") or exit(-2);
print "\nWE SHOULD HAVE $number (now $newNumber)\n";
($number eq $newNumber) or exit(-2);
$d->close();
print "DONE\n";

delete  $ENV{SEALED_ENVELOPE_REMOTE_PUBLIC_KEY};
delete  $ENV{SEALED_ENVELOPE_REMOTE_PRIVATE_KEY};
delete  $ENV{SEALED_ENVELOPE_LOCAL_PUBLIC_KEY};
delete  $ENV{SEALED_ENVELOPE_LOCAL_PRIVATE_KEY};
delete  $ENV{ALIEN_DATABASE_PASSWORD};

sub changeNumberJobs{
  my $number=shift;
  my $ldap = Net::LDAP->new("$host:8389", "onerror" => "warn") 
    or print "failed\nError conecting to the ldap server\n $? and $! and  $@\n" 
      and exit (-3);
my $suffix=Net::Domain::hostdomain();

$suffix=~ s/\./,dc=/g;
$suffix="dc=$suffix";
  my $result=$ldap->bind("cn=manager,$suffix", "password" => "ldap-pass");
  $result->code && print "failed\nCould not bind to LDAP-Server: ",$result->error 
    and exit (-4);
  
  my $config=new AliEn::Config;
  $config or print "Error getting the configuration!!\n" and exit(-2);
  my $ldapDN="ou=CE,ou=Services,$config->{FULLLDAPDN}";
  my $key="name=testCE,$ldapDN";

  my $mesg=$ldap->modify( $key, replace=>{"maxjobs", $number});
  $mesg->code && print "failed\nCould not modify  $key: ",$result->error and exit (-5);
  $ldap->unbind;
  return 1;

}

