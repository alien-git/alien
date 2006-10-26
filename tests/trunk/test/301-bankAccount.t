#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;
use AliEn::SOAP;

BEGIN { plan tests => 2}
{
  my $ALIEN_ROOT=$ENV{ALIEN_ROOT};
  my $soap= AliEn::SOAP->new();

  # create first account
  my $pid =$$;
  my $done = $soap->CallSOAP("LBSG", "createBankAccount", "account_hamar_mek_$pid","1000");
     $done or system ("$ALIEN_ROOT/httpd/bin/httpd");
     $done or sleep(5);

  $done or $done = $soap->CallSOAP("LBSG", "createBankAccount", "account_hamar_mek_$pid","1000");
  $done or print "Error: Can not createBank account, SOAP call to LBSG
                          service failed" and exit(-1);

     my $result = $done->result;
     
     ($result eq 1) or ok(0);
     ($result eq 1) and ok(1);

  # delete account
  
   $done = $soap->CallSOAP("LBSG", "deleteBankAccount", "account_hamar_mek_$pid");
                              
   $done or print "Error: Can not delete account, SOAP call to LBSG
                          service failed" and exit(-1);

     $result = $done->result;
     
     ($result eq 1) or ok(0);
     ($result eq 1) and ok(1);
    

}