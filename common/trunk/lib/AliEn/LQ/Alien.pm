package AliEn::LQ::Alien;

@ISA = qw( AliEn::LQ );

use AliEn::LQ;

use AliEn::CE;
use strict;

#sub initialize {
#  my $self=shift;
#  $self->{UI}=
#  return 1;
#}
sub submit {
  my $self = shift;
  my $classad=shift;
  my ( $command, @args ) = @_;
  $self->{LOGGER}->info("LQ/Alien", "Submitting the job to another V.O ($self->{ORG})");



  my $oldOrg=$self->{CONFIG}->{ORG_NAME};
  $self->debug(1, "Organisation -> $self->{ORG}");


  my $oldCM=$ENV{ALIEN_CM_AS_LDAP_PROXY};
  my $oldid=$ENV{ALIEN_PROC_ID};


  delete $ENV{ALIEN_CM_AS_LDAP_PROXY};

  my $tmp=$self->{CONFIG}->Reload({"organisation", $self->{ORG}, "force", 1, 
				  "debug", 0});
  $ENV{ALIEN_CM_AS_LDAP_PROXY}=$oldCM;

  if (!$tmp) {
    $self->{LOGGER}->info("LQ/Alien", "Error getting the config of $self->{ORG}");
    return -1;
  }
  $self->{CONFIG}=$tmp;
  $self->debug(1, "Creating the new CE");
  delete $ENV{ALIEN_PROC_ID};

  my $ce=AliEn::CE->new();
  if (!$ce){
    $self->{CONFIG}=$self->{CONFIG}->Reload({"organisation", $oldOrg});
    $self->{LOGGER}->info("LQ/Alien", "Error getting the authentication for $self->{ORG}");
    return -1;
  }

  my $jdlFile=$self->CreateJDL($oldOrg, $oldCM, $oldid,$ENV{ALIEN_JOB_TOKEN});
  $jdlFile or return -1;

  $self->debug(1, "In submit, sending $jdlFile ");
  my @done=$ce->submitCommand("<$jdlFile");

  $self->debug(1, "In submit got @done");
  $ENV{ALIEN_PROC_ID}=$oldid;

  $self->{CONFIG}=$self->{CONFIG}->Reload({"organisation", $oldOrg});

  if (! @done) {
    $self->{LOGGER}->info("LQ/Alien", "Error getting the file");
    return -1;
  }
  $self->debug(1, "Everything worked!!");
  return 0 ;
}
sub CreateJDL {
  my $self =shift;
  my $oldOrg=shift;
  my $oldCM=shift;
  my $oldid=shift;
  my $oldToken=shift;

  my $file="$self->{CONFIG}->{TMP_DIR}/alien.submit.$$";

  $self->debug(1,"In submit creating the JDL");
  AliEn::MSS::file::mkdir("", $self->{CONFIG}->{TMP_DIR} ) ;
  my $jdl=$self->{JDL};
  $jdl =~ s/^\s*\[//s;
  $jdl =~ s/^\s*requirements[^;]*;//mi;
  $jdl =~ s/\]\s*$//s;

  $jdl =~ s/(AliEn_Master_VO=\")(.*\")/$1$oldOrg $2/ or 
    $jdl.=";AliEn_Master_VO=\"$oldOrg#$oldCM#$oldid#$oldToken\"";
  $jdl =~ s/;\s*origrequirements[^;]*;/;/mi;

  if (!open (FILE, ">$file")){
    $self->{LOGGER}->error("LQ/Alien", "Error opening the file $file");
    $self->{CONFIG}=$self->{CONFIG}->Reload({"organisation", $oldOrg});
    
    return -1;
  }
  print FILE $jdl;
  close FILE;
  $self->debug(1,"JDL file written with $jdl");
  return $file;
}


sub initialize() {
  my $self = shift;
  
  my $org="";
  
  
  if ( $self->{CONFIG}->{CE_SUBMITARG} ) {
    $self->debug(1, "Arguments @{$self->{CONFIG}->{CE_SUBMITARG_LIST}}");
    my @list = grep (s/^org(anisation)?=//i, 
		     @{ $self->{CONFIG}->{CE_SUBMITARG_LIST} });
    
    @list and $org=$list[0];
    $self->debug(1, "Name of the organisation-> $org");
    
  }
  if (!$org) {
    $self->{LOGGER}->error("LQ/Alien", "Error: your ldap configuration says that this is an AliEn queue, but there is no organisation in SUBMITARG");
    return;
  }
  $self->{ORG}=$org;
  return 1;
}
return 1;
