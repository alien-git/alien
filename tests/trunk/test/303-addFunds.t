#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;
use AliEn::SOAP;

#
# - creates account with 0 AliDrams
# - adds 500 AliDrams to first account 
# - checks the remainding funds on the account
#

BEGIN { plan tests => 4}
{
  my $soap= AliEn::SOAP->new();

  #
  # create account
  #
  	my $pid =$$;
	my $done = $soap->CallSOAP("LBSG", "createBankAccount", "account_hamar_mek_$pid");
	   $done or print "Error: Can not createBank account, SOAP call to LBSG
                          service failed" and exit(-1);

	   my $result = $done->result;
     
	     ($result eq 1) or ok(0);
	     ($result eq 1) and ok(1);

  #
  # transact funds 
  # 

	 $done = $soap->CallSOAP("LBSG", "addFunds", "account_hamar_mek_$pid", 500);      
         $done or print "Error: Can not transact funds, SOAP call to LBSG
                          service failed" and exit(-1);

	     $result = $done->result;
     
	     ($result eq 1) or ok(0);
	     ($result eq 1) and ok(1);

  #
  # check funds addition
  # 
   	$done = $soap->CallSOAP("LBSG", "getBalance", "account_hamar_mek_$pid");                          
        $done or print "Error: Can not transact funds, SOAP call to LBSG
                          service failed" and exit(-1);

             $result = $done->result;
     
	     ($result eq 500) or ok(0);
	     ($result eq 500) and ok(1);

               

  #
  # delete account 
  #   

   	$done = $soap->CallSOAP("LBSG", "deleteBankAccount", "account_hamar_mek_$pid");
        $done or print "Error: Can not delete account, SOAP call to LBSG
                          service failed" and exit(-1);

	     $result = $done->result;

	     ($result eq 1) or ok(0);
	     ($result eq 1) and ok(1);

}
