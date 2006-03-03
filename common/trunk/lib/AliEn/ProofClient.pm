#####################################################
#  Proof interactive Analysis Client Module         #
#  (C) Andreas-J. Peters @ CERN                     #
#  mailto: Andreas.Peters@cern.ch                   #
#####################################################

package AliEn::ProofClient;
use strict;

use AliEn::Logger;
use AliEn::SOAP;

sub new {
  my $proto = shift;
  my $class = ref($proto) || $proto;
  my $self  = {};
  bless( $self, $class );

  $self->{SOAP}=new AliEn::SOAP;
  $self->{SOAP} or return;
  $self->{USER} = "";

#  @{$self->{SITEPROCHASHARRAY}}={};
  
  return $self;
}

sub init {
  my $self = shift;
  $self->{USER} = shift;
  $self->{REQUESTEDTIME} = (shift or time);
}

sub addSite {
  my $self = shift;
  my $site = shift;
  my $ntimes = (shift or '1');
  
  my $siteprochash;
  $siteprochash->{'SITE'}   = "$site";
  $siteprochash->{'NPROOF'} = $ntimes;
  push @{$self->{SITEPROCHASHARRAY}}, 
    {SITE   => $siteprochash->{'SITE'},
     NPROOF => $siteprochash->{'NPROOF'}};
}


sub addFile {
  my $self = shift;
}

sub addQuery {
  my $self = shift;
}

#################################################################
# this are internal module routines to prepare and contact      #
# the central Proof Service                                     #
#################################################################

# SOAP does not know arrays of hashes, we have to serialize them
sub Serializer {
  my $self = shift;
  my $siteprochash;
  my $serializer="";
  foreach $siteprochash (@{$self->{SITEPROCHASHARRAY}}) {
    $serializer .= "$siteprochash->{'SITE'}###$siteprochash->{'NPROOF'};";
  }
  return $serializer;
}

# contact the Proof Service with our request
  
sub CallService {
  my $self = shift;

  # serialize for SOAP
  my $serializer = $self->Serializer();

  # call the Proof Service with our request 
  my $response = $self->{SOAP}->CallSOAP("Proof", "SetUpMasterProxy",$self->{USER},$serializer);
  $self->{SOAP}->checkSOAPreturn($response, "Proof") or  $self->{LOGGER}->error_msg() and return;
  $response=$response->result;

  $self->{SESSIONID} = $response->{SESSIONID};
  $self->{NMUX}      = $response->{NMUX};
  $self->{MUXHOST}   = $response->{MUXHOST};
  $self->{MUXPORT}   = $response->{MUXPORT};
  $self->{LOGINUSER} = $response->{LOGINUSER};
  $self->{LOGINPWD}  = $response->{LOGINPWD};
  $self->{CONFIGFILE}= $response->{CONFIGFILE};
  $self->{MASTERURL} = $response->{MASTERURL};
  $self->{SITELIST}  = $response->{SITELIST};
  return 1;
}

# contact the Proof Service to ask for a status

sub QueryStatus {
  my $self = shift;
  my $sessionId = shift or return;

  # call the Proof Service 
  my $response = $self->{SOAP}->CallSOAP("Proof", "QueryStatus",$sessionId);
  $self->{SOAP}->checkSOAPreturn($response, "Proof") or  $self->{LOGGER}->error_msg() and return;
  $response=$response->result;

  $self->{SESSIONID} = $response->{SESSIONID};
  chomp $self->{SESSIONID};
  $self->{NMUX}      = $response->{NMUX};
  chomp $self->{NMUX};
  $self->{NREQUESTED}= $response->{NREQUESTED};
  chomp $self->{NREQUESTED};
  $self->{NASSIGNED} = $response->{NASSIGNED};
  chomp $self->{NASSASSIGNED};
  $self->{MUXHOST}   = $response->{MUXHOST};
  chomp $self->{MUXHOST};
  $self->{MUXPORT}   = $response->{MUXPORT};
  chomp $self->{MUXPORT};
  $self->{STATUS}    = $response->{STATUS};
  chomp $self->{STATUS};
  $self->{SCHEDULEDTIME} = $response->{SCHEDULEDTIME};
  chomp $self->{SCHEDULEDTIME};
  $self->{VALIDITYTIME}  = $response->{VALIDITYTIME};
  chomp $self->{VALIDITYTIME};
  $self->{SESSIONUSER}      = $response->{USER};
  chomp $self->{SESSIONUSER};
  return 1;
}  

sub CancelSession{
  my $self = shift;
  my $sessionId = shift or return;
  $self->{SESSIONID} = 0;
  # call the Proof Service
  my $response = $self->{SOAP}->CallSOAP("Proof", "CancelSession",$self->{USER}, $sessionId);
  $self->{SOAP}->checkSOAPreturn($response, "Proof") or  $self->{LOGGER}->error_msg() and return;
  $response=$response->result;
  $self->{SESSIONID} = $response->{SESSIONID};
  return 1;
}

sub ListSessions{
  my $self = shift;
  $self->{SESSIONLIST} = "";
  print "List Session in ProofClient\n";
  my $response =  $self->{SOAP}->CallSOAP("Proof", "ListSessions",$self->{USER});
  $self->{SOAP}->checkSOAPreturn($response, "Proof") or  $self->{LOGGER}->error_msg() and return;
  $response=$response->result;
  $self->{SESSIONLIST} = $response;
 # my (@allsessions) = split "#LINEBREAK#",$response;
 # foreach (@allsessions) {
 #   print $_,"\n";
 # }
  return 1;
}

sub ListDaemons{
  my $self = shift;
  $self->{DAEMONLIST} = "";
  my $response =  $self->{SOAP}->CallSOAP("Proof", "ListDaemons",$self->{USER});
  $self->{SOAP}->checkSOAPreturn($response, "Proof") or  $self->{LOGGER}->error_msg() and return;
  $response=$response->result;
  $self->{DAEMONLIST} = $response;
 # my (@alldaemonss) = split "#LINEBREAK#",$response;
 # foreach (@alldaemonss) {
 #   print $_,"\n";
 # }
  return 1; 
}

sub Dump             {
  my $self = shift;
  if ($self->{SESSIONID}) {
    print "----------------------------------------\n";
    print "Proof Session Id:     $self->{SESSIONID}\n";
    print "#    of MUXs    :     $self->{NMUX}     \n";
    print "Host of MUX     :     $self->{MUXHOST}  \n";
    print "Port of MUX     :     $self->{MUXPORT}  \n";
    print "----------------------------------------\n";
  } else {
    print "----------------------------------------\n";
    print "Proof Session not established!          \n";
    print "----------------------------------------\n";
  }

  print "****************************************\n";
  print "Session sites   :                       \n";
  print "****************************************\n";
  foreach (@{$self->{SITEPROCHASHARRAY}}) {
    printf "%32s %4d\n",$_->{'SITE'}, $_->{'NPROOF'};
  }
  print "****************************************\n";
}

return 1;
