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
   return 1;
}

sub hello{
  my $self=shift;
  print "I'm in LCG!!! :D\n";
  $self->SUPER::hello();
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

  print "Looking for $SE\n";

  my $BDII = $self->{CONFIG}->{LCG_GFAL_INFOSYS};#"ldap://lcg-bdii.cern.ch:2170"
  $BDII = "ldap://$ENV{LCG_GFAL_INFOSYS}" if defined $ENV{LCG_GFAL_INFOSYS};
  print "Querying $BDII...\n";
  
  my $URI = '';
  
  my $ldap =  Net::LDAP->new($BDII) or die $@;
  $ldap->bind() or die $@;
  my $result = $ldap->search( base   => "mds-vo-name=local,o=grid",
			      filter => "GlueServiceURI=*$SE*");
  $result->code && die $result->error;
  print "Got ",$result->count()," entries.\n";
  foreach my $entry ($result->all_entries) {
    my $values = $entry->get_value("GlueServiceURI");
    my $types = $entry->get_value("GlueServiceType");
    if ($types =~ m/srm/ ) {
      print "Got it, it\'s $types\n";
      $URI = $values;
      last;
    }
    print "$values is not an SRM service ($types)\n";
  }
  print "URI is $URI\n" if $URI;

  my $result = $ldap->search( base   => "mds-vo-name=local,o=grid",
			      filter => "(&(GlueSARoot=$self->{CONFIG}->{ORG_NAME}*)(gluechunkkey=*$SE*))");
  $result->code && die $result->error;
  print "HELLO\n";
  foreach my $entry ($result->all_entries) {
    my @values = $entry->get_value("GlueSARoot");
    print "FOUND something @values\n";
    

  }

  $ldap->unbind();
  return $URI;
}


return 1;
