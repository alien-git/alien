package AliEn::MSS::srm;
use strict;

# Author: (c) 2004, CERN, Akos.Frohner <Akos.Frohner@cern.ch>
# On behalf of the EU EGEE project.

use AliEn::MSS;

#use SRM;
use AliEn::X509;
use vars qw(@ISA  $DEBUG);
@ISA = ( "AliEn::MSS" );

# copied from MSS::LCG::new
sub new {
  my $self = shift;
  my $options2 = shift;
  my $options = (shift or {});

  use Data::Dumper;
  print "Creating the new MSS interface with\n";
  print Dumper($options);
 
  $self = $self->SUPER::new($options2, $options);
  $self->{X509}=AliEn::X509->new() or return;


  foreach my $command ({command=>"srmcp"},{name=>"srmstat", command=>"srm-get-metadata"}) {
    my $path = `which $command->{command} 2> /dev/null`;
    ($path && !$?) or printf STDERR "Error: No  $command->{command} command found in your path.\n" and exit 5;
    chomp $path;
    $path or $self->info("Error: could not find $command->{command}") and return;
    my $name=uc($command->{name} || $command->{command});
    $self->{$name}=$path;
  }
  my @list=();
  push @list,"-debug=true" if $DEBUG;
  push @list,"-retry_num=0";
  $self->{SRMCP_OPTIONS}=\@list;

  if ( $self->{NO_CREATE_DIR}) {
    $self->debug(1, "Skipping connecting to the BDII");
    $options->{HOST} and $self->{URI}="httpg://$options->{HOST}:$options->{PORT}/srm/managerv1";
    $self->{MOUNTPOINT}="";
    return $self;
  }

  

  my $VO =$self->{CONFIG}->{ORG_NAME};
  my $BDII = $ENV{LCG_GFAL_INFOSYS};
  $self->{CONFIG}->{CE_LCG_GFAL_INFOSYS} and $BDII=$self->{CONFIG}->{CE_LCG_GFAL_INFOSYS};

  $BDII=~ s{ldap://}{};

  my @se_options;

  my $item=$self->{CONFIG}->{SE_OPTIONS_LIST};
  $options2->{VIRTUAL} and $item=$self->{CONFIG}->{"SE_$options2->{VIRTUAL}"}->{OPTIONS_LIST};
  if ($item) {
    foreach (@$item) {
      push @se_options, split ('=', $_);
    }
  }
  my %se_hash=@se_options;
  if (! $se_hash{host} ) {
    $self->{LOGGER}->error("SRM", "Error: the host is not specified. Please, put in the ldap configuration of $self->{CONFIG}->{SE_FULLNAME}, in the options something like: host=<hostname>");
    return;
  }
  my $SE=$se_hash{host};
  
  if ($se_hash{uri} and $se_hash{mountpoint}){
    $self->info("The uri and mountpoint are defined in the ldap");
    $self->{URI}=$se_hash{uri};
    $self->{MOUNTPOINT}=$se_hash{mountpoint};
    return $self;
  }

  $BDII or $self->info("Can't find the address of the BDII") and return;
  $self->info("Contacting the BDII at $BDII");
  
  my $ldap =  Net::LDAP->new($BDII) or die $@;
  $ldap->bind() or die $@;
  print "Looking for $SE\n" if $DEBUG;
  my $result = $ldap->search( base   => "mds-vo-name=local,o=grid",
			      filter => "(&(GlueServiceURI=*$SE*)(GlueServiceType=srm*))");
  $result->code && die $result->error;
  print "Got ",$result->count()," entries.\n" if $DEBUG;
  $result->count() or printf STDERR "Error: SE $SE not found in the BDII.\n" and exit 10;
  my $URI;
  foreach my $entry ($result->all_entries) {
    my @values = $entry->get_value("GlueServiceURI");
    print "Warning: more than one entry!\n" if ($#values>0  && $DEBUG );
    $URI = $values[0];
    $URI or printf STDERR "Error: Error getting URI for $SE.\n" and exit 11;
    print "SRM endpoint is $URI\n" if $DEBUG;
    last;
  }
  my $mountpoint = '';
  $result = $ldap->search( base   => "mds-vo-name=local,o=grid",
			   filter => "(&(GlueSARoot=$VO*)(gluechunkkey=*$SE*))");
  $result->code && die $result->error;
  print "Got ",$result->count()," entries.\n" if $DEBUG;
  $result->count() or print "Mountpoint for $SE not found.\n" and exit 20;
  foreach my $entry ($result->all_entries) {
    my @values = $entry->get_value("GlueSARoot");
    print "Warning: more than one entry!\n" if ($#values>0 && $DEBUG);
    (undef, $mountpoint) = split (/:/,$values[0],2);
    last;
  }
  $mountpoint or printf STDERR "Error: Error getting mountpoint for $VO.\n" and exit 21;
  print "SRM mountpoint path is $mountpoint\n" if $DEBUG;
  $ldap->unbind();
  $self->{URI}=$URI;
  $self->{MOUNTPOINT}=$mountpoint;

  return $self;
}

sub mkdir {
    # SRM creates the directories in 'put' -> skip this
    return 0;
}

sub get {
  my $self = shift;
  my ($from, $to) = @_;
  
  $self->debug(1, "Doing get $to : $from");
#  if (!($ENV{X509_USER_CERT} and  ( -f $ENV{X509_USER_CERT}))){
#    $self->{LOGGER}->info("MSS::SRM", "We are not authenticated to get the file :(\n");
#    return;
#  }
  
  $self->{X509}->checkProxy();
  
  
  
  $from = $self->url($from);
  $to = 'file:///'.$to;
   my @command = ($self->{SRMCP},@{$self->{SRMCP_OPTIONS}},$from,$to);
  print "Doing ",join (" ",@command),"\n" if $DEBUG;
  my $error = system(@command);
  print "$error\n";
  return $error;
}


sub put {
  my $self = shift;
  my ( $from, $to ) = @_;

  $self->debug(1, "Doing put $from : $to");

  $self->{X509}->checkProxy();
  $from = 'file:///'.$from;
  $to = $self->url($to);

  my @command = ($self->{SRMCP},@{$self->{SRMCP_OPTIONS}},$from,$to);
  print "Doing ",join (" ",@command),"\n" if $DEBUG;
  my $error = system(@command);
  print "$error\n";
  return $error;
}

sub sizeof {
  my $self = shift;
  my $file = shift;
  my $fullPath = shift;

  $fullPath or $fullPath=$self->url($file);
  $self->info("Getting the size of $fullPath");
  my $command="$self->{SRMSTAT} ".join(" ", @{$self->{SRMCP_OPTIONS}})." $fullPath";

  open (FILE, "$command |") or 
    $self->info("Error doing srmstat: $!") and return;
  my @data=<FILE>;
  close FILE or $self->info("Error getting the size $!") and return;
  $self->debug(1, "Got the size: @data");
  my $size=join("", grep (s{^\s*size\s*:}{}i, @data));
  chomp $size;
  $self->info("The size of $fullPath is $size");
  # returning the file size
  return $size ;
}

sub url {
  my $self = shift;
  my $file = shift;

  if ($file !~ m/^srm:\/\/.*/) {
    (undef,undef,my $host,undef) = split /\//, $self->{URI};
    $file =~ m{^/} or $file.="$self->{MOUNTPOINT}$file";
    $file = "srm://$host$file";
  }
  print "The full url is $file\n";
  return $file;
}



return 1;
