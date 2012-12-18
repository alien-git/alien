package AliEn::MSS::edg;

@ISA = qw (AliEn::MSS);

use AliEn::MSS;

use strict;
#
#  edg://<REPLICA CATALOG><EDG lfn>
#
# 
sub initialize {
  my $self=shift;
  $self->{FTP_LOCALCOPIES}=1;
  return 1;
}


sub new {
    my $self = shift;#
    my $options2= shift;
    my $options =(shift or {});

    $self->GetEDGSE($options);

    $ENV{GLOBUS_LOCATION}="/opt/globus";
    delete $ENV{X509_CERT_DIR};
  
    $self = $self->SUPER::new($options2, $options);
    $self or return;

    $ENV{GDMP_CONFIG_FILE}="/opt/edg/etc/\L$self->{CONFIG}->{ORG_NAME}\E/gdmp.conf";
    $ENV{RC_CONFIG_FILE}="/opt/edg/etc/\L$self->{CONFIG}->{ORG_NAME}\E/rc.conf";
#    $ENV{RC_CONFIG_FILE}="/home/bagnasco/AliEn/rcINFN.conf";
#    $ENV{RC_CONFIG_FILE}="/home/bagnasco/test/rc.conf";


    return $self;
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

#    return (0);
    my $dir=shift;
#    my (@args) = @_;
    my @command=("globus-job-run",$self->{EDGSE},"/bin/mkdir",
		 "-p", "$self->{EDGSEMOUNT}/$dir");
    print "Skipping @command";
    #    my $error=system(@command);
    my $error=0;

    return $error;
}

sub put {
  my $self=shift;
  my $from=shift;
  my $to= shift;

  $self->{LOGGER}->info("EDG", "Putting a file into EDG (from $from)");
  $self->{LOGGER}->warning("EDG", "put will overwrite!");


  $to =~ s/^$self->{SAVEDIR}\///;
  my $prefix = $to;
  my $postfix = $to;
  if ($prefix !~ s/\/.*$//) {$prefix=""} else {$prefix .= ".";};
  if ($postfix !~ s/^.*\.//) {$postfix=""} else {$postfix=".$postfix"};
  my $basename = $from;
  $basename=~ s/[^\/].*\///;
  $basename=~ s/\///;

  $to = "$prefix$basename$postfix";
  my $newfile=$from;
  $newfile=~  s/\/([^\/]*)$/\/$to/;

  #
  # Do some tricks
  #
  $self->debug(1, "Making a link from $newfile to $from");
  `ln -sf $from  $newfile`;
  my $edgse= $self->{EDGSE};
  chomp ($edgse);
  #
  # Check whether the file already exists
  #
  my @command = ("edg_rc_getPhysicalFileNames",
		 "-C","-c","$ENV{RC_CONFIG_FILE}",
		 "-d","-l","$to");
  $self->debug(1, "Doing @command");
  print "\n";
  my $error=system(@command);
  $self->debug(1,"RESULT $error $! and $? and $@");
  if ($! == "") {
    @command = ("edg-replica-manager-deleteFile",
		"-s","$edgse/$self->{SAVEDIR}/$to",
	        "-c","$ENV{RC_CONFIG_FILE}",
		"-l","$to","-a");
    
    $self->debug(1, "Doing @command");
    $error=system(@command);
    $self->debug(1,"RESULT $error $! and $? and $@");
  }
  @command =("edg-replica-manager-copyAndRegisterFile", 
	     "-d", "$edgse/$self->{SAVEDIR}/$to",
	     "-s","$self->{HOST}$newfile",
	     "-c","$ENV{RC_CONFIG_FILE}",
	     "-l", "$to");
  
  $self->debug(1, "Doing @command\n");

  $error=system(@command);
  $self->debug(1,"RESULT $error $! and $? and $@");

#  if ($error) {
#      $self->debug(1, "Trying with CopyFile");
#      $command[0]="edg-replica-manager-copyFile";
#      $command[5]="";
#      $command[6]="";
#      $error=system(@command);
#  }

  $self->{FILE_LFN}=$to;

  return $error;

}

sub GetPhysicalFileName {
    my $self=shift;
    my $file=shift;

    #
    # Get the EDG PFN from the Replica Catalogue
    #
    my $basename;
    $self->{LOGGER}->info("EDG", "Getting the physical file name of $file");
    $file=~ /\/([^\/]*)$/ and $basename=$1;
    $basename or $basename=$file;
    $self->{LOGGER}->info("EDG","Command is edg_rc_getPhysicalFileNames -C -c $ENV{RC_CONFIG_FILE} -l $basename -d" );
    print "\n";
    open (BATCH, "edg_rc_getPhysicalFileNames -C -c $ENV{RC_CONFIG_FILE} -l $basename -d |");
    my $edgPFN=<BATCH>;    
    my $done=close BATCH;
    
    chomp $edgPFN;
    $self->{LOGGER}->info("EDG", "Got $done and $edgPFN");
    $done or return;
    
    return $edgPFN;
}

sub get {
  my $self=shift;
  my ($from, $to)=@_;

  $self->{LOGGER}->info("EDG", "Getting a file from EDG");
  $self->debug(1,"from $from to $to");
  my $edgPFN=$self->GetPhysicalFileName($from);
  
  $edgPFN or return ;
  $self->debug(1, "Making a copy of $edgPFN");
  my @command =("globus-url-copy", 
		"gsiftp://$edgPFN", "file://$to");
  $self->debug(1,"DOING @command");
  my $error=system(@command);
  $self->debug(1, "RESULT $error");
  
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
    $self->{LOGGER}->info("EDG","doing @command");
    my $line= join (" ", @command);

    my $error=`$line`;

    $line or return;
    $self->{LOGGER}->info("EDG","RESULT $error");

    $error =~ s/^(\S+\s+){4}(\S+).*$/$2/;
    return $error;
}

sub url {
    my $self = shift;
    my $file = shift;
    $file = "\L$file\E";
    return "edg://\L$self->{CONFIG}->{ORG_NAME}\E/$self->{FILE_LFN}";
}
return 1;
