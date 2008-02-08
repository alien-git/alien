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
    $dn=~ /disabled/ and 
      $self->debug(1, "Skipping '$dn' (it is disabled)") and next;
    my ($site, $vo)=($1,$2);
    my $sename="${vo}::${site}::$name";
    $self->info("Doing the SE $sename");
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
    $self->debug(1, "And the update should $sename be: $seioDaemons, $path");

    my $e=$self->{CATALOGUE}->{CATALOG}->{DATABASE}->{LFN_DB}->query("SELECT sename,seioDaemons,sestoragepath from SE where seName='$sename'");
    my $path2=$path;
    $path2 =~ s/\/$// or $path2.="/";
    my $total=$self->{CATALOGUE}->{CATALOG}->{DATABASE}->{LFN_DB}->queryValue("SELECT count(*) from SE where seName='$sename' and seioDaemons='$seioDaemons' and ( seStoragePath='$path' or sestoragepath='$path2')");

    if ($total<1){
      $self->info("***Updating the information of $site, $name ($seioDaemons and $path");
      $self->{CATALOGUE}->execute("setSEio", $site, $name, $seioDaemons, $path);
    }
    my @paths=$entry->get_value('savedir');
    my $db_name=lc("se_$self->{CONFIG}->{ORG_NAME}_${site}_${name}");
    my $db=$self->{CATALOGUE}->{CATALOG}->{DATABASE}->{LFN_DB};
    my $info=  $db->query("select * from $db_name.VOLUMES");
    $info or $self->info("Error getting the list of volumes for this SE") and next;
#    $db->do("update VOLUMES set 
    use Data::Dumper;
    my @existingPath=@$info;
    my $t=$entry->get_value("mss");
    my $method=lc("$t://$host");
    foreach my $path (@paths){
      my $found=0;
      my $size=-1;
      $path =~ s/,(\d+)$// and $size=$1;
      $self->info("  Checking the path of $path");
      for my $e (@existingPath){
        $e->{mountpoint} eq $path or next;
        $self->debug(1, "The path already existed");
        $e->{FOUND}=1;
        $found=1;
        ( $size eq $e->{size}) and next;
        $self->info("**THE SIZE IS DIFFERENT ($size and $e->{size})");
        $db->do("update $db_name.VOLUMES set size=? where mountpoint=?", {bind_values=>[$size, $e->{mountpoint}]});
      }
      $found and next;
      $self->info("**WE HAVE TO ADD THE SE '$path'");
      $db->do("insert into $db_name.VOLUMES(volume,method, mountpoint,size) values (?,?,?,?)", {bind_values=>[$path, "$host", $path, $size]});
    }
    foreach my $oldEntry (@existingPath){
      $oldEntry->{FOUND} and next;
      $self->info("**The path $oldEntry->{mountpoint} is not used anymore");
      $db->do("update $db_name.VOLUMES set size=usedspace where mountpoint=?", {bind_values=>[$oldEntry->{mountpoint}]});
    }

    $db->do("update $db_name.VOLUMES set freespace=size-usedspace where size!= -1");
    $db->do("update $db_name.VOLUMES set freespace=2000000000 where size=-1");
  }

  

  return;
}

1;
