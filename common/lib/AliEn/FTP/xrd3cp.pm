package AliEn::FTP::xrd3cp;



use strict;
use vars qw( @ISA $DEBUG);


use AliEn::FTP;
use AliEn::Util;
use AliEn::UI::Catalogue::LCM;

@ISA = ( "AliEn::FTP" );

$DEBUG=0;

sub initialize {
  my $self=shift;
  $self->info("HEllo, creating a new xrd3cp package");

  (open(FILE, "which xrd3cp 2>&1 |")) or $self->info("Error: xrd3cp is not in the path") and return;
  my @info=<FILE>;
  (close FILE) or $self->info("Error: xrd3cp is not in the path (closing) @info and $?") and return;

  return $self;
}

sub copy {
  my $self=shift;
  my $sEnvelope = shift;
  my $tEnvelope = shift;

  my $sourceEnvelope = $sEnvelope->{signedEnvelope};
  my $targetEnvelope = $tEnvelope->{signedEnvelope};

  my $sxurl = (AliEn::Util::getValFromEnvelope($sourceEnvelope,'xurl') || AliEn::Util::getValFromEnvelope($sourceEnvelope,'turl'));
  my $txurl = (AliEn::Util::getValFromEnvelope($targetEnvelope,'xurl') || AliEn::Util::getValFromEnvelope($targetEnvelope,'turl'));

  # if we have the old styled envelopes
  (defined($sEnvelope->{oldEnvelope})) and $sourceEnvelope = $sEnvelope->{oldEnvelope};
  (defined($tEnvelope->{oldEnvelope})) and $targetEnvelope = $tEnvelope->{oldEnvelope};

  $self->info("Ready to copy $sEnvelope->{turl} into $tEnvelope->{turl}");
  
  my $args="-m -S $sxurl $txurl  authz=\"\\\"$sourceEnvelope\\\"\" authz=\"\\\"$targetEnvelope\\\"\" ";
  
  $DEBUG and $args = " -d ".$args;
  my $output = `xrd3cp  $args  2>&1 ; echo "ALIEN_XRD_SUBCALL_RETURN_VALUE=\$?"` or $self->info("Error: Error doing the xrd3cp $args",1) and return;
  $output =~ s/\s+$//;
  $output =~ /ALIEN_XRD_SUBCALL_RETURN_VALUE\=([-]*\d+)$/;
  my $com_exit_value = $1;


  $self->debug(2, "Exit code: $com_exit_value, Returned output: $output");

  if($com_exit_value ne 0) {

#### Disabled per Andreas' request since retrieving the logs crashes the source
#     # &xferuuid=2d20aade-5859-11df-9e4b-001e0bd3f44c&xfercmd=preparetoput located at 128.142.216.111:1094
#
#     $self->info("Error doing the xrd3cp $args. Trying to retrieve additional log info.",1);
     $self->info("Exit code not equal to zero. Something went wrong with xrdcp!! Exit code: $com_exit_value, Returned output: $output",1);
#     $output=~ /xferuuid=([0-9a-fA-F\-]+)/;
#     my $xferuuid = $1;
#     (AliEn::Util::isValidGUID($xferuuid))
#       or $self->info("Error doing the xrd3cp $args. Not possible to retrieve additional log info due to invalid xferuuid",1) and return;
#
#     my @logargs= ("-l $xferuuid $sEnvelope->{turl} $tEnvelope->{turl}  \"authz=$sourceEnvelope\" \"authz=$targetEnvelope\" ");  
#     my $logoutput = `xrd3cp $args  2>&1 ` or $self->info("Error: Error doing the xrd3cp $args",1) and return;
#     #my $com_exit_value=$? >> 8;
#     $logoutput =~ s/\s+$//;
#     $self->info("Additional log info from xrd3cp for xferuuid ($xferuuid): $logoutput",1);
#     if ($output =~ /file is not online/) { 
#          $self->info("Error doing the xrd3cp $args. File is not online",1);
#          return 3;
#     }
#

     return;
  }

  $self->info("The transfer worked!!");
  return 1;
}

1;
