#!/usr/bin/perl -w

BEGIN{ $Devel::Trace::TRACE = 0 }

use strict;

use AliEn::Service;
use Getopt::Long;

$Devel::Trace::TRACE = 0;

my $options = {
	       'debug'    => 0,
	       'user'     => $ENV{ALIEN_USER},
	       'queueId'  =>"",
	       'disablePack' =>0,
	       'port'     =>"",
	       'fullname' =>0,
	       'callback' =>"",
	       'logfile' =>"",
	      };


my $service=shift; 


Getopt::Long::Configure("pass_through");
#First, let's see if we have to redirect the output
Getopt::Long::GetOptions($options,"logfile=s", "debug=s"  )  or exit;
my $Logger=AliEn::Logger->new({logfile=>$options->{logfile}, debug=>$options->{debug}}) or exit;
Getopt::Long::Configure("default");

( Getopt::Long::GetOptions( $options, "help", "user=s", "debug=s", 
			    "queueId=n", "disablePack", "port=n", "fullname", "callback=s","logfile=s") )
  or exit;

#This script starts an AliEn Service. It receives the service to start as the first argument;

$service or print "Error : no service to start!!\n You have to specify a service (i.e. SE, FTD, CE, Monitor)\n" and exit;


my $name="AliEn::Service::$service";

$options->{fullname} and $name=$service;

eval "require $name"
  or print STDERR "Error requiring the service $service\n$@\nDoes the service $service exist?\n"
      and exit;

my $serv= $name->AliEn::Service::new($options);

$serv or exit;
$serv->startListening();

print STDERR "Error!! Service died!!\n";

