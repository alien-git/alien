#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;
use AliEn::SOAP;

#
# checks bank user command

BEGIN { plan tests => 1}
{
  my $soap= AliEn::SOAP->new();

	my $done = $soap->CallSOAP("LBSG", "bank", ("gmkuser" , "SomeRandomUserName"));
	   $done or print "Error: Can not do 'gmkuser SomeRandomuserName', SOAP call to LBSG service failed" and exit(-1);

	   my $result = $done->result;
       
             
        
        
	 if ($result =~ /Successfully created 1 User/) 
         { 
            ok(1);
         }
         elsif ($result =~ /User already exist/)
         {
            ok(1);
         }
         else 
         {
            ok(0);
		print "Can not create user (gmkuser SomeRandomUserName): \nResult is:\n$result";

         }


        
}
