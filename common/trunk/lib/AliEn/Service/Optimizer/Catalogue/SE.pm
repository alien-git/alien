package AliEn::Service::Optimizer::Catalogue::SE;
 
use strict;

use AliEn::Service::Optimizer::Catalogue;
use AliEn::Database::IS;


use vars qw(@ISA);
push (@ISA, "AliEn::Service::Optimizer::Catalogue");

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;
  my @info;

  my $method="info";
  $silent and $method="debug" and  @info=1;

  $self->$method(@info, "The SE optimizer starts");

  my $ldap;
  eval {
    $ldap=Net::LDAP->new($self->{CONFIG}->{LDAPHOST}) or die("Error contacting $self->{CONFIG}->{LDAPHOST}");
    $ldap->bind();
  };
  if ($@){
    $self->info("Error connecting to ldap!: $@");
    return;
  }
  my $mesg=$ldap->search(base=>$self->{CONFIG}->{LDAPDN},
			 filter=>"(objectClass=AliEnMSS)");
  my $total=$mesg->count;
  $self->info("There are $total entries under AliEnMSS");

  
  foreach my $entry ($mesg->entries){
    my $name=uc($entry->get_value("name"));
    my $dn=$entry->dn();
    $dn=~ /ou=Services,ou=([^,]*),ou=Sites,o=([^,]*),/i or $self->info("Error getting the site name of '$dn'") and next;
    my ($site, $vo)=($1,$2);
    my $sename="${vo}::${site}::$name";
    $self->debug(1, "And the name is $sename ($dn)");
    my (@io)=$entry->get_value('iodaemons');
    my $found= join("", grep (/xrootd/, @io)) or next;
    $self->debug(1, "This se has $found");

    $found =~ /port=(\d+)/i or $self->info("Error getting the port from '$found' for $sename") and next;
    my $port=$1;
    $found =~ /host=([^:]+)(:.*)?$/i or $self->info("Error getting the host from '$found' for $sename") and next;
    my $host=$1;
    my $path=$entry->get_value('savedir') or next;
    $path=~ s/,.*$//;
    my $seioDaemons="root://$host:$port";
    $self->info("***And the update should $sename be: $seioDaemons, $path"); 
    
    print "$self->{CATALOGUE} and $self->{CATALOGUE}->{CATALOG} and $self->{CATALOGUE}->{CATALOG}->{DATABASE}\n";
    my $e=$self->{CATALOGUE}->{CATALOG}->{DATABASE}->{LFN_DB}->query("SELECT sename,seioDaemons,sestoragepath from SE where seName='$sename'");
    use Data::Dumper;
    my $path2=$path;
    $path2 =~ s/\/$// or $path2.="/";
    my $total=$self->{CATALOGUE}->{CATALOG}->{DATABASE}->{LFN_DB}->queryValue("SELECT count(*) from SE where seName='$sename' and seioDaemons='$seioDaemons' and ( seStoragePath='$path' or sestoragepath='$path2')");
    print Dumper($total);
    if ($total<1){
      print "NOPE SELECT count(*) from SE where seName='$sename' and seioDaemons='$seioDaemons' and seStoragePath='$path'\n";
      print Dumper($e);
    }
  }

  

  return;
}

1;
