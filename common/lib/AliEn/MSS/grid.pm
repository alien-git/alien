package AliEn::MSS::grid;

@ISA = qw (AliEn::MSS);

use AliEn::MSS;

use strict;

sub initialize {
  my $self=shift;
  $self->{FTP_LOCALCOPIES}=1;
  return 1;
}


sub  GetEDGSE {
  my $self= shift;
  my $options=shift;


  open( OUT, "edg-brokerinfo getCloseSEs |" );
  my $edgse = <OUT>;
  chomp($edgse);
  $options->{EDGSE} = $edgse;
  my $done=close(OUT);
  my $config =AliEn::Config->new();
  if ((!$done) or (! $options->{EDGSE})) {
    #we should ask LDAP
    
    $options->{EDGSE}=$config->{SE_SAVEDIR};
    $options->{EDGSE}=~ s/\/(.*)$// and $config->{SE_SAVEDIR}=$1; 
    
    print "Got from LDAP: SE -> $options->{EDGSE}; SE_SAVEDIR -> $config->{SE_SAVEDIR}\n";		     
    return 1;
  }

  my $edgsemount=`edg-brokerinfo getSEMountPoint $options->{EDGSE}`;
  chomp($edgsemount);
  $options->{EDGSEMOUNT}=$edgsemount;
  $config->{SE_SAVEDIR}="$edgsemount/alice";
  print "Got from BrokerInfo: SE -> $options->{EDGSE} SE_SAVEDIR -> $config->{SE_SAVEDIR}\n";
  return 1;
}



sub mkdir {
    my $self = shift;

    my $dir=shift;
    my @command=("globus-job-run",$self->{EDGSE},"/bin/mkdir",
		 "-p", "$self->{EDGSEMOUNT}/$dir");
    my $error=system(@command);
    return $error;
}


sub cp {
    my $self = shift;
    my ( $from, $to ) = @_;

    if ( -f $from ) {
      return $self->put($from, $to);

    }
    return $self->get($from, $to);
}

sub mv {
    my $self = shift;
    my ( $from, $to ) = @_;
    if ( $self->cp( $from, $to ) ) {
        $self->rm($from);
    }
}


sub sizeof {

    my $self = shift;
    my $file = shift;

    my $edgPFN=$self->GetPhysicalFileName($file);

    $edgPFN or return;
    my $se = $edgPFN;
    my $mp = $edgPFN;
    my $filebase = $edgPFN;
    $se =~ s/\/.*$//;
    $filebase =~ s/^.*\///;
    $mp =~ s/$se//;
    $mp =~ s/$file//;
#    my @command=("globus-job-run",$self->{EDGSE},"/bin/ls", "-la",
#		 "$self->{EDGSEMOUNT}/$file");
    my @command=("globus-job-run",$se,"/bin/ls", "-la",
		 "$mp/$filebase");
    $self->{LOGGER}->info("GridMSS","doing @command");
    my $line= join (" ", @command);

    my $error=`$line`;

    $line or return;
    $self->{LOGGER}->info("GridMSS","RESULT $error");

    $error =~ s/^(\S+\s+){4}(\S+).*$/$2/;
    return $error;
}

sub url {
    return 0;
#  edg://<REPLICA CATALOG><EDG lfn>
}

sub GetPhysicalFileName {
  return 0;
}

return 1;
