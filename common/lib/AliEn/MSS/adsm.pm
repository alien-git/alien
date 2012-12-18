package AliEn::MSS::adsm;

@ISA = qw (AliEn::MSS);

use AliEn::MSS;

use strict;

sub new {
    my $self = shift;

    $self = $self->SUPER::new(@_);
    $self or return;

    #the archive and archivepath will be set in the config 
    #but for the moment, let's just specify them here
    $self->{SAVEDRIR} or $self->{SAVEDIR}=$self->{CONFIG}->{SE_SAVEDIR};
    $self->{SAVEDIR} =~ /^\/([^\/]*)(\/.*$)/;    #)
    $self->{ARCHIVE}     = ( $1 or "" );
    $self->{ARCHIVEPATH} = ( $2 or "" );
    $self->debug(1,"Using '$self->{ARCHIVE}' and '$self->{ARCHIVEPATH}'\n" );
    return $self;
}

sub mkdir {
    my $self = shift;
    my (@args) = @_;
    return (0);
}

sub get {
    my $self = shift;
    my $file =shift;
    my $localfile=shift;

     $self->debug(1, "Getting file from $self->{ARCHIVE} ( $self->{ARCHIVEPATH} and  $self->{PATH})");

    my @cmd = (
        "tsmcli", "retrieve", "$localfile", $self->{ARCHIVE},
        $self->{ARCHIVEPATH}, "stage=no"
    );

    my $error = system(@cmd);

    if ($error) {
        my $mess = ( $_ or "" );
        print STDERR
"ERROR getting the file from the robot!\nTrying to do @cmd and got $mess\n";
        return 1;
    }
    return 0;
}

sub put {
  my $self=shift;
  my ( $from, $to ) = @_;
  
  if ( $from =~ /[A-Z]/ ) {
    
    #If there are any capital letters in the name, make them lowercase;
    my $old = $from;
    $from = "\L$from\E";
    
    $self->{LOGGER}->info( "MSS:adsm", "Doing a link from $old to $from\n" );
    symlink( $old, $from ) or 
      $self->{LOGGER}->error( "MSS:adsm", "Error doing a link from $old to $from\n@_\n" ) 
	and return 1;
  }


  #If we want to copy into adsm, we hae to do this
  $to = "\L$to\E";
  $to =~ s/\/$self->{ARCHIVE}$self->{ARCHIVEPATH}//;
  ($to !~ /\/\d+$/ )and ( $to =~ s/\/[^\/]*$// );
  my @cmd = (
	  "tsmcli", "archive", $from, $self->{ARCHIVE},
	  "$self->{ARCHIVEPATH}$to", "0"
	 );
  $self->{LOGGER}->info( "MSS:adsm", "In MSS:adsm, doing @cmd\n" );
  
  return ( system(@cmd) );

}
#sub cp {
#    my $self = shift;
#    my ( $from, $to ) = @_;#
#
#    #Here, we should decide if this goes into adsm, or comes from adsm
#    # copy FROM GSI to another site
#    my @cmd = (
#        "adsmcli", "retrieve", "$to", $self->{ARCHIVE}, $self->{ARCHIVEPATH},
#        "stage=no"
#    );#
#
#    if ( -f $from ) {
#        if ( $from =~ /[A-Z]/ ) {#
#
#            #If there are any capital letters in the name, make them lowercase;
#            my $old = $from;
#            $from = "\L$from\E";##
#
#            $self->{LOGGER}->info( "MSS:adsm", "Doing a link from $old to $from\n" );
#            symlink( $old, $from ) or 
#	      $self->{LOGGER}->error( "MSS:adsm", "Error doing a link from $old to $from\n@_\n" ) 
#		and return 1;##
#
#        }
#
#        #If we want to copy into adsm, we hae to do this
#        $to = "\L$to\E";
#        $to =~ s/\/$self->{ARCHIVE}$self->{ARCHIVEPATH}//;
#        ($to !~ /\/\d+$/ )and ( $to =~ s/\/[^\/]*$// );
#        @cmd = (
#            "adsmcli", "archive", $from, $self->{ARCHIVE},
#            "$self->{ARCHIVEPATH}$to", "0"
#        );
#    }
#
    # actually "adsmcli retrieve" needs as argument not $from, $to
    # but rather $file, $archive, $archivepath = 
    # "localpath/file ARCHIVE archivepath"
    #       
    # we need also a copy FROM another site TO GSI
    # which should look somehow like
    #   my  @cmd = ("adsmcli archive", "$from", "$to");
    # or rather
    #   my  @cmd = ("adsmcli archive", $file, $archive, $archivepath); ???    

#    $self->{LOGGER}->info( "MSS:adsm", "In MSS:adsm, doing @cmd\n" );#
#
#    return ( system(@cmd) );
#}

sub mv {
    my $self = shift;
    my ( $from, $to ) = @_;
    if ( $self->cp( $from, $to ) ) {
        $self->rm($from);
    }
}

sub rm {
    my $self = shift;
    my (@args) = @_;
    my @cmd =
      ( "tsmcli", "delete", $self->{ARCHIVE}, $self->{ARCHIVEPATH}, @args );

    # @args should contain filename, ARCHIVE and path 
    # see above   
    return ( system(@cmd) );
}
sub getSize {
  my $self=shift;
  return $self->sizeof(@_);
}

sub lslist {
  my $self=shift;
  my @fileInSE;
  return \@fileInSE;
}

sub sizeof {

    # sizeof can be acchieved by using "adsmcli query"
    # for example
    my $self = shift;
    my $file = shift;

    # my ($file, $archive, $archivepath) = @_;
    my @cmd = (
        "/u/aliprod/bin/tsmsizeof.bash",
        $file, $self->{ARCHIVE}, $self->{ARCHIVEPATH}
    );
    return ( system(@cmd) );
}

sub url {
    my $self = shift;
    my $file = shift;
    $file = "\L$file\E";
    return "adsm://$self->{HOST}$file";
}

return 1;

# remark: the directory tree structure for the adsmcli command at GSI
#         is as follows:
#         example:
#       > adsmcli retrieve "/d/alice03/aliprod/test" ALIPROD /prod/2001-01
#         here /d/alice03/aliprod is the local SAVE directory 
#              test               is the filename
#              /prod/2001-01      is the MSS-representation of the directory
#                                                                  tree

