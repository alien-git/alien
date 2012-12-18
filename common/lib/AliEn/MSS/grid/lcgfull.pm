package AliEn::MSS::grid::lcgfull;

@ISA = qw (AliEn::MSS::grid);

use AliEn::MSS::grid;

use strict;
#
#  edg://<REPLICA CATALOG><EDG lfn>
#
# 

sub new {
    my $self = shift;#
    my $options2= shift;
    my $options =(shift or {});

    $self->GetEDGSE($options);
    $ENV{GLOBUS_LOCATION}="/opt/globus";
    delete $ENV{X509_CERT_DIR};
#    $options->{NO_CREATE_DIR} = 1;
    $self = $self->SUPER::new($options2, $options);
    $self or return;
    # Check whether LCG is working...
    $self->{LOGGER}->info("LCG","Command is edg-rm --vo $self->{CONFIG}->{ORG_NAME} printInfo" );
    my $result = system("edg-rm --vo $self->{CONFIG}->{ORG_NAME} printInfo");
    if ($result) {
      $self->{LOGGER}->error("LCG","Could not contact LCG services ($result), aborting the SE.");
      return;
    }
    return $self;
}


sub mkdir {
    print "Skipping mkdir, not implemented for LCG\n";
    return 0;
}

sub GetPhysicalFileName {
    my $self=shift;
    my $file=shift;

    #
    # Get the EDG PFN from the Replica Catalogue
    # For LCG,PFN is actually a GUID (???)
    #
    my $basename;
    $self->{LOGGER}->info("LCG", "Getting the GUID of $file");
    $file=~ /\/([^\/]*)$/ and $basename=$1;
    $basename or $basename=$file;
    $self->{LOGGER}->info("LCG","Command is edg-rm --vo $self->{CONFIG}->{ORG_NAME} listGUID lfn:$basename" );
    print "\n";
    open (BATCH, "edg-rm --vo $self->{CONFIG}->{ORG_NAME} listGUID lfn:$basename |");
    my $edgPFN=<BATCH>;    
    my $done=close BATCH;
    
    chomp $edgPFN;
    $self->{LOGGER}->info("LCG", "Got $done and $edgPFN");
    $done or return;
    
    return $edgPFN;
}

sub url {
    my $self = shift;
    my $file = shift;
    $file = "\L$file\E";
    my $url = "lcg://\L$self->{CONFIG}->{ORG_NAME}\E/$self->{FILE_LFN}";
    $self->{LOGGER}->info("LCG","AliEn URL is $url\n");
    return $url;
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

sub put {
  my $self=shift;
  my $from=shift;
  my $to= shift;
  $self->{LOGGER}->info("LCG", "Putting a file into LCG (from $from)");
#  $self->{LOGGER}->warning("LCG", "put will overwrite!");


  $to =~ s/^$self->{SAVEDIR}\///;
  my $prefix = $to;
  my $postfix = $to;
  if ($prefix !~ s/\/.*$//) {$prefix=""} else {$prefix .= ".";};
  if ($postfix !~ s/^.*\.//) {$postfix=""} else {$postfix=".$postfix"};
  my $basename = $from;
  $basename=~ s/[^\/].*\///;
  $basename=~ s/\///;
  $to = "$basename$postfix";
 my $newfile=$from;
#  $newfile=~  s/\/([^\/]*)$/\/$to/;

  #
  # Do some tricks
  #
#  $self->debug(1, "Making a link from $newfile to $from");
#  `ln -sf $from  $newfile`;
  my $edgse= $self->{EDGSE};
  chomp ($edgse);
  #
  # Check whether the file already exists
  #
  #my @command = ("edg_rc_getPhysicalFileNames",
#		 "-C","-c","$ENV{RC_CONFIG_FILE}",
#		 "-d","-l","$to");
#  $self->debug(1, "Doing @command");
#  print "\n";
#  my $error=system(@command);
#  $self->debug(1,"RESULT $error $! and $? and $@");
#  if ($! == "") {
#    @command = ("edg-replica-manager-deleteFile",
#		"-s","$edgse/$self->{SAVEDIR}/$to",
#	        "-c","$ENV{RC_CONFIG_FILE}",
#		"-l","$to","-a");
#    
#    $self->debug(1, "Doing @command");
#    $error=system(@command);
#    $self->debug(1,"RESULT $error $! and $? and $@");
#  }
  my @command =("edg-rm", 
	     "--vo","\L$self->{CONFIG}->{ORG_NAME}\E",
	     "copyAndRegisterFile", "file:$newfile",
	     "-d", "srm://$edgse/$self->{SAVEDIR}/$to",
  	     "-l", "lfn:$to");

  $self->debug(1, "Doing @command\n");

  my $error=system(@command);
  $self->debug(1,"RESULT $error $! and $? and $@");

  $self->{FILE_LFN}=$to;

  return $error;

}

sub get {
  my $self = shift;
  my ($from, $to) = @_;
  chomp $from;
  chomp $to;
  $self->{LOGGER}->info("LCG", "Getting a file from LCG storage");
  $self->{LOGGER}->info("LCG","From $from to $to");
  my $edgPFN = $self->GetPhysicalFileName($from);
  my $edgse= $self->{EDGSE};
  $self->{LOGGER}->info("LCG","The LCG SE is $edgse");
  chomp ($edgse);
  $self->{LOGGER}->info("LCG","The LCG SE is $edgse");
  $edgPFN or return ;
  $self->{LOGGER}->info("LCG", "Making a copy of $edgPFN");
  my @command =("edg-rm", 
	        "--vo","\L$self->{CONFIG}->{ORG_NAME}\E",
	        "copyFile", "--force",
		"$edgPFN", "file:$to");
  $self->{LOGGER}->info("LCG","Get: doing @command");
  my $error=system(@command);

  return $error;

}

return 1;
