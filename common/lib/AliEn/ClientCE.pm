package AliEn::ClientCE;

use AliEn::CE;

use vars qw(@ISA);

@ISA = ("AliEn::CE",@ISA);

sub new {
  my $proto = shift;
  my $class = ref($proto) || $proto;
  my $self  = {};
  
  my $options = shift;
  
  $self->{SOAP}=new AliEn::SOAP;
  
  bless( $self, $class );
  
  $self->{LOGGER} = new AliEn::Logger;

  $self->{DEBUG} = ( $options->{debug} or 0 );
  ( $self->{DEBUG} ) and $self->{LOGGER}->debugOn($self->{DEBUG});
  $self->{SILENT} = ( $options->{silent} or 0 );
  $DEBUG and $self->debug(1, "Creating a new RemoteQueue" );
  $self->{CONFIG} = new AliEn::Config() or return;

  my @possible = ();
  $self->{CONFIG}->{CEs} and @possible = @{ $self->{CONFIG}->{CEs} };
  $DEBUG and $self->debug(1,
			  "Config $self->{CONFIG}->{SITE} (Possible queues @possible)" );

  ( $self->{CONFIG} )
    or $self->{LOGGER}->warning( "ClientCE", "Error: Initial configuration not found!!" )
	and return;

  $self->{HOST} = $self->{CONFIG}->{HOST};

  $self->{QUEUEID} = "";
  $self->{COMMAND} = "";

  $self->{WORKINGPGROUP} = 0;

  $self->{CATALOG} = ( $options->{CATALOG} or AliEn::UI::Catalogue::LCM->new($options) );
  $self->{CATALOG} or return;
  my $queuename = "AliEn::LQ";
  ( $self->{CONFIG}->{CE} ) 
    and $queuename .= "::$self->{CONFIG}->{CE_TYPE}";

  $DEBUG and $self->debug(1, "Batch sytem: $queuename" );

  eval "require $queuename"
    or print STDERR "Error requiring '$queuename': $@\n"
      and return;
  $options->{DEBUG} = $self->{DEBUG};
  $self->{BATCH}    = $queuename->new($options);

  $self->{BATCH} or $self->info( "Error getting an instance of $queuename") and return;

  $self->{LOGGER}->notice( "ClientCE", "Starting remotequeue..." );

  my $pOptions={};

  $options->{PACKMAN} and $self->{PACKMAN}=$pOptions->{PACKMAN}=$options->{PACKMAN};
  my $ca=AliEn::Classad::Host->new($pOptions) or return;

  AliEn::Util::setCacheValue($self, "classad", $ca->asJDL);
  $self->info($ca->asJDL);
  $self->{X509}=new AliEn::X509 or return;
  $self->{DB}=new AliEn::Database::CE or return;

  my $role = $self->{CATALOG}->{CATALOG}->{ROLE} || "";
  
  if ($options->{MONITOR}) {
    AliEn::Util::setupApMon($self);
    AliEn::Util::setupApMonService($self, "CE_$self->{CONFIG}->{CE_FULLNAME}");
  }

  return $self;
}

sub f_queue {
  my $self = shift;
  return $self->{CATALOG}->{CATALOG}->callAuthen("queue",@_);
}

sub f_jquota {
  my $self = shift;
  return $self->{CATALOG}->{CATALOG}->callAuthen("jquota",@_);
}

sub calculateJobQuota {
  my $self = shift;
  return $self->{CATALOG}->{CATALOG}->callAuthen("calculateJobQuota",@_);
}

sub checkJobAgents {
  my $self = shift;
  return $self->{CATALOG}->{CATALOG}->callAuthen("checkAgents",@_);
}

sub resyncJobAgent{
  my $self = shift;
  return $self->{CATALOG}->{CATALOG}->callAuthen("resyncJobAgent",@_);
}

sub resubmitCommand {
  my $self = shift;
  return $self->{CATALOG}->{CATALOG}->callAuthen("resubmit",@_);
}

sub masterJob {
	my $self = shift;
	return $self->{CATALOG}->{CATALOG}->callAuthen("masterJob", @_);	
}

sub f_top{
	my $self=shift;
	return $self->{CATALOG}->{CATALOG}->callAuthen("top", @_);
}

sub f_kill{
	my $self=shift;
	return $self->{CATALOG}->{CATALOG}->callAuthen("kill", @_);
}

sub f_jobListMatch{
	my $self=shift;
	my $options=shift;
	return $self->{CATALOG}->{CATALOG}->callAuthen("jobListMatch", "-$options", @_);
}

sub f_queueinfo {
  my $self = shift;
  return $self->{CATALOG}->{CATALOG}->callAuthen("queueinfo", @_);
}

sub f_ps{
	my $self=shift;
	$_[0] =~ /trace$/ and shift and return $self->f_ps_trace(@_);
	return $self->{CATALOG}->{CATALOG}->callAuthen("ps", @_);
}


__END__
