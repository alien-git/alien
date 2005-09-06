package AliEn::Config::LCG;

use strict;

use vars qw (@ISA);

use AliEn::Config;

push @ISA, 'AliEn::Config';

sub ConfigureVirtualSite {
   my $self=shift;
   $self->info("Configuring an LCG site");
   my $ref=$self->getSEList() or return;
   $self->info("We found several se:");
   foreach my $se (@$ref) {
     $self->info($se);
   }
   if ($ENV{uc("VO_$self->{ORG_NAME}_DEFAULT_SE")}){
     $self->GetEndPoint($ENV{uc("VO_$self->{ORG_NAME}_DEFAULT_SE")});
   }else {
     $self->info("WARNING!!! The variable ".uc("VO_$self->{ORG_NAME}_DEFAULT_SE")." is not defined!");
   }
   return 1;
}

sub getSEList {
  my $self=shift;

  $self->info("Looking in the ldap server for all the SE defined in LCG");
  my @list;
  $ENV{LCG_GFAL_INFOSYS} or $self->info("Error: the environment variable LCG_GFAL_INFOSYS is not defined") and return;
  my $ldap=Net::LDAP->new($ENV{LCG_GFAL_INFOSYS}) or print "Error connecting \n" and return;

  $ldap->bind or print  "Error binding \n" and return;
  my $mesg=$ldap->search(base=>"mds-vo-name=local,o=grid",
                         filter=>"(GlueCESEBindSEUniqueID=*)"
                        );
  my %seen;
  foreach my $entry ($mesg->entries) {
    my $value=$entry->get_value("GlueCESEBindSEUniqueID");
    push @list, $value unless $seen{$value}++;
  }

  $ldap->unbind;
  return \@list;
}


sub GetEndPoint {
  my $self=shift;
  my $SE=shift;

  $self->debug(1,"Looking for $SE");

  my $BDII = $self->{CONFIG}->{LCG_GFAL_INFOSYS};#"ldap://lcg-bdii.cern.ch:2170"
  $BDII = "ldap://$ENV{LCG_GFAL_INFOSYS}" if defined $ENV{LCG_GFAL_INFOSYS};
  $self->debug(1,  "Querying $BDII...");

  my $URI = '';
  
  my $ldap =  Net::LDAP->new($BDII) or $self->info("Error conecting to ldap $@") and return;
  $ldap->bind() or $self->info("Error binding to ldap: $@") and return;
  my $result = $ldap->search( base   => "mds-vo-name=local,o=grid",
			      filter => "GlueServiceURI=*$SE*");
  $result->code && $self->info("Error querying the ldap" . $result->error) and return;
  $self->debug(1, "Got ".$result->count()." entries.");

  foreach my $entry ($result->all_entries) {
    my $types = $entry->get_value("GlueServiceType");
    if ($types =~ m/srm/ ) {
      $URI = $entry->get_value("GlueServiceURI");
      $self->info("Using the se in $URI (type $types)");
      last;
    }
    $self->debug(2,"$values is not an SRM service ($types)");
  }
  $URI or $self->info("Error getting the URI!") and return;

  $result = $ldap->search( base   => "mds-vo-name=local,o=grid",
			   filter => "(&(GlueSARoot=$self->{CONFIG}->{ORG_NAME}*)(gluechunkkey=*$SE*))");
  $result->code && die $result->error;

  $self->debug(1, "Getting the mountpoint");
  foreach my $entry ($result->all_entries) {
    my @values = $entry->get_value("GlueSARoot");
    print "FOUND something @values\n";
    

  }

  $ldap->unbind();
  return $URI;
}


return 1;
