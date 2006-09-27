#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;
use AliEn::SOAP;

#
# - creates 2 accounts with initial 1000 AliDrams
# - transacts 500 AliDrams from first to second 
# - checks the remainding funds on both accounts
# - deletes both accounts 
#

BEGIN { plan tests => 7}
{
  my $soap= AliEn::SOAP->new();

  #
  # create first account
  #
  	my $pid =$$;
	my $done = $soap->CallSOAP("LBSG", "createBankAccount", "account_hamar_mek_$pid","1000");
	   $done or print "Error: Can not createBank account, SOAP call to LBSG
                          service failed" and exit(-1);

	   my $result = $done->result;
     
	     ($result eq 1) or ok(0);
	     ($result eq 1) and ok(1);

  #
  # create second  account
  #

	  $done = $soap->CallSOAP("LBSG", "createBankAccount", "account_hamar_erku_$pid", "1000");                    
	  $done or print "Error: Can not delete account, SOAP call to LBSG
                          service failed" and exit(-1);

	     $result = $done->result;
     
	     ($result eq 1) or ok(0);
	     ($result eq 1) and ok(1);

  #
  # transact funds 
  # 

	 $done = $soap->CallSOAP("LBSG", "transactFunds", "account_hamar_mek_$pid", "account_hamar_erku_$pid", 500);      
         $done or print "Error: Can not transact funds, SOAP call to LBSG
                          service failed" and exit(-1);

	     $result = $done->result;
     
	     ($result eq 1) or ok(0);
	     ($result eq 1) and ok(1);

  #
  # check  funds transaction
  # 
   	$done = $soap->CallSOAP("LBSG", "getBalance", "account_hamar_mek_$pid");                          
        $done or print "Error: Can not transact funds, SOAP call to LBSG
                          service failed" and exit(-1);

             $result = $done->result;
     
	     ($result eq 1500) or ok(0);
	     ($result eq 1500) and ok(1);

        $done = $soap->CallSOAP("LBSG", "getBalance", "account_hamar_erku_$pid");                           
        $done or print "Error: Can not transact funds, SOAP call to LBSG
                          service failed" and exit(-1);

     	     $result = $done->result;
     
	     ($result eq 500) or ok(0);
	     ($result eq 500) and ok(1);

  #
  # delete accounts 
  #   

   	$done = $soap->CallSOAP("LBSG", "deleteBankAccount", "account_hamar_mek_$pid");
        $done or print "Error: Can not delete account, SOAP call to LBSG
                          service failed" and exit(-1);

	     $result = $done->result;

	     ($result eq 1) or ok(0);
	     ($result eq 1) and ok(1);

   	$done = $soap->CallSOAP("LBSG", "deleteBankAccount", "account_hamar_erku_$pid");
        $done or print "Error: Can not delete account, SOAP call to LBSG
                          service failed" and exit(-1);

	     $result = $done->result;

	     ($result eq 1) or ok(0);
	     ($result eq 1) and ok(1);

}
