#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;
use AliEn::SOAP;
use Data::Dumper;

my $ALIEN_ROOT=$ENV{ALIEN_ROOT};

#
# checks bank user command

BEGIN { plan tests => 2}
{
  my $soap= AliEn::SOAP->new();

	my $done = $soap->CallSOAP("LBSG", "bank", ("glsuser" ));
 	   if (!$done)
	   {
		print " Try to start apache\n";
		system("env LD_LIBRARY_PATH=$ALIEN_ROOT/httpd/lib:$ENV{LD_LIBRARY_PATH} $ALIEN_ROOT/httpd/bin/httpd -f $ALIEN_ROOT/httpd/conf/httpd.conf -k restart"); 
	        system ("ps -ef|grep httpd");
		$done = $soap->CallSOAP("LBSG", "bank", ("glsuser" ));

	   }
	   
	   		
	   $done or print "Error: Can not do 'glsuser', SOAP call to LBSG service failed" and exit(-1);

	   my $result = $done->result;
        
 
	     if ($result =~ /Super User/)
	     {
		ok(1);
	     }
	     else 
	     {
		ok(0);
	        print "Result does not contain 'Super User': \nResult is:\n$result";
	     }

	     if ($result =~ /Gold Admin/) 
	     {
		ok(1);
	     }
	     else
	     {		
	     	ok(0);
	        print "Result does not contain 'Gold Admin': \nResult is:\n$result";
	     }

}
