package AliEn::MSS::grid::edg;

@ISA = qw (AliEn::MSS::grid);

use AliEn::MSS::grid;

use strict;
#
#  edg://<REPLICA CATALOG><EDG lfn>
#
# 
sub initialize{
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

    return $self;
}

sub mkdir {
    print "Skipping mkdir, not implemented for EDG\n";
    my $error=0;

    return 0;
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

sub url {
    my $self = shift;
    my $file = shift;
    $file = "\L$file\E";
    return "edg://\L$self->{CONFIG}->{ORG_NAME}\E/$self->{FILE_LFN}";
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

return 1;
