#/**************************************************************************
# * Copyright(c) 2001-2002, ALICE Experiment at CERN, All rights reserved. *
# *                                                                        *
# * Author: The ALICE Off-line Project / AliEn Team                        *
# * Contributors are mentioned in the code where appropriate.              *
# *                                                                        *
# * Permission to use, copy, modify and distribute this software and its   *
# * documentation strictly for non-commercial purposes is hereby granted   *
# * without fee, provided that the above copyright notice appears in all   *
# * copies and that both the copyright notice and this permission notice   *
# * appear in the supporting documentation. The authors make no claims     *
# * about the suitability of this software for any purpose. It is          *
# * provided "as is" without express or implied warranty.                  *
# **************************************************************************/
package AliEn::Logger::SoapLog;

no warnings 'deprecated';
use strict;
#use Log::Dispatch::Output;
#use base qw( Log::Dispatch::Output );

use AliEn::Config;
use AliEn::SOAP;

my $host;
my $port;
my $proxy;
my $localhost;
my $soap;

sub new {
    my $proto = shift;
    my $class = ref $proto || $proto;

    my %params = @_;
      my $self = {};
    {
        no strict 'refs';
        $self = bless {}, $class;
    }
    $localhost = $ENV{ALIEN_HOSTNAME};

    $self->_basic_init(%params);

    return $self;
}

sub log_message {
    my AliEn::Logger::SoapLog $self = shift;
    if (!$host) {
      my $ini    = new AliEn::Config();
      ($ini) or return;
      
      $host = $ini->{LOG_HOST};
      $port = $ini->{LOG_PORT};
      $proxy=$ini->{SOAPPROXY_ADDRESS};
      my $org=$ini->{ORG_NAME};
      $soap=new AliEn::SOAP;
    }
    my %params = @_;

    my @message = split "::::", $params{message};

#    my $done=$self->{SOAP}->
#	CallSOAP("Logger", "log",  $localhost, $params{level}, 
#		 $message[1], $message[0] );
#    $proxy or 
    eval {
      my $done= #0;
	SOAP::Lite->uri('AliEn/Service/Logger')
	    ->proxy("http://$host:$port", timeout=>2)
	      #	   ->on_error( {print "Socorro"})
	      ->log( $localhost, $params{level}, $message[1], $message[0] );#
      #
    };
#    $proxy and $done= SOAP::Lite->uri('AliEn/Service/SOAPProxy')
#	  ->proxy("http://$proxy")
#      ->Call("$org", "Logger", "log", $localhost, 
#	     $params{level}, $message[1], $message[0] );

#    ($done) and ( $done = $done->result );    # or do something
#    ($done)
#      or print STDERR
#      "Error contacting Logger as AliEn/Service/Logger in $host:$port\n";
    return;

    # Do something with message in $params{message}
}
1;

