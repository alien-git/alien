# This optimizer will calculate the final price of the DONE jobs 
# and charge for them
package AliEn::Service::Optimizer::Job::Charge;


use strict;

use AliEn::Service::Optimizer::Job;
use vars qw(@ISA );
use Data::Dumper;


push (@ISA, "AliEn::Service::Optimizer::Job");

use AliEn::Util;

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;
  $self->{SLEEP_PERIOD} =  60*10; #once in ten minutes 
  my $method="info";
  $silent and $method = "debug";
    
  $self->{LOGGER}->$method("Charge",  "Charge optimizer starts");
    
 my $queueTable = "QUEUE";
 
  $self->{CONFIG} = new AliEn::Config;

  # get nominal price from LDAP 
  my $nominalPrice = $self->{CONFIG}->{SI2K_NOMINAL_PRICE};

  my $taxAccount;   
  my $taxRate;
  
  $taxAccount = $self->{CONFIG}->{TAX_ACCOUNT} || "" ;
  $taxAccount and ($taxRate  = $self->{CONFIG}->{TAX_RATE} || 0);

  if ($taxRate > 1)
  {
    $taxRate = 1;
    $self->info("Warning: Tax rate can not be more than '1'. Seting tax rate to 1");

  }

  # if nominal price is not defined in LDAP set it to '1'
  if (!$nominalPrice)
  { 
    $nominalPrice=1; 
    $self->info("Warning: Nominal price for si2k not set in LDAP. Seting nominal price to 1");
  } 

  # set charge status variables
  $self->{CHARGING_DONE} = "CHARGE_DONE";
  $self->{CHARGING_NOW} = "CHARGE_IN_PROGRESS";
	$self->{CHARGING_FAILED} = "CHARGE_FAILED";


	# we will 'mark' job entries for current session of Charge optimizer by
	# seting their charge status to $self->{CHARGING_NOW}
 		      
  # 'mark' job entries for current session and   
	# update finalPrice in $queueTable
  my $update = " UPDATE $queueTable q, QUEUEPROC p SET finalPrice = round(p.si2k * $nominalPrice * price),chargeStatus=\'$self->{CHARGING_NOW}\'";
	my $where  = " WHERE (status='DONE' AND p.si2k>0 AND chargeStatus!=\'$self->{CHARGING_DONE}\' AND chargeStatus!=\'$self->{CHARGING_FAILED}\') and p.queueid=q.queueid";
  
  my $updateStmt = $update.$where;	
  
  $self->{DB}->do($updateStmt);

  # select job entries for current session 
  my $stmt = "SELECT queueId,submitHost,site,finalPrice FROM $queueTable WHERE  (chargeStatus=\'$self->{CHARGING_NOW}\') AND (finalPrice>0)";
  my $table = $self->{DB}->query("$stmt"); 

  @$table or ($self->info("No jobs to charge.Going to sleep...") and return);

  my $jobsListToCharge="";
  
 
  #make connection to LDAP (will be passed to internal functions)
  $self->{LDAP_CONNECTION} = Net::LDAP->new($self->{CONFIG}->{LDAPHOST}) or return "Error: Can't connect to LDAP server $self->{CONFIG}->{LDAPHOST}";
  $self->{LDAP_CONNECTION}->bind;

  my $userAccountCache = {};   
  my $siteAccountCache = {};   
  my @chargeList;    
  # prepare list of transactions 
 
   my ( $jobId, $user, $site, $userAccount, $siteAccount , $amount);

  for (my $i = 0; $i < @$table; $i++)
  {
     #get the job ID
     $jobId = $table->[$i]->{'queueId'};	
    
     # get the user name from submitHost
     ($user) = split ("@", $table->[$i]->{'submitHost'});

     # get the bank account name of the $user (we will charge from this account)
     # first check the hash, if it's not there get from LDAP
     $userAccountCache->{$user} or ($userAccountCache->{$user} =  $self->getUserBankAccount($user));
     $userAccount = $userAccountCache->{$user};

     if ( !$userAccount)
     {
         $self->setFailedToCharge($jobId, "Error: Can not charge for job no. $jobId . No bank account defined for user $user");
         next;
     }
   
     # get the name of the site from 'site' entry in QUEUE   
       (undef, $site, undef) = split ("::", $table->[$i]->{'site'});
       
     # get bank account name of $_site (we will put money to this account
     # first check the hash, if it's not there get from LDAP
     $siteAccountCache->{$site} or ($siteAccountCache->{$site} = $self->getSiteBankAccount($site));
     $siteAccount = $siteAccountCache->{$site};

     if ( !$siteAccount )
     {
         $self->setFailedToCharge($jobId, "Error: Can not charge for job no. $jobId . No bank  account defined for site $site");
         next;
     }
     
     # get amount 
     $amount = $table->[$i]->{'finalPrice'};
     if (!$amount)
     {
        $self->setFailedToCharge($jobId, "Error: Can not charge for job. no $jobId . No  finalPrice defnied"); 
        next;
     }


     my $jobChargeData = { 
                           'id'          => $jobId,
                           'user'        => $user,
                           'userAccount' => $userAccount,
                           'site'        => $site,
                           'siteAccount' => $siteAccount,
                           'price'       => $amount,
                           'payToSie'    => $amount,
                           'payTax'      => 0,
                         }; 
     
     if ($taxRate)
     {
        $jobChargeData->{'payToSite'} = int ($amount * (1 - $taxRate) + 0.5);
        $jobChargeData->{'payTax'}    = int ($amount * ($taxRate)     + 0.5);
     }

     push (@chargeList, $jobChargeData);
     $self->info("Prepared charge list for job $jobId");


  }
  
 
  my $done;
  my $depositList = {};
  my $finalTax = 0;
  my $command;
  my $res;

  # charge for jobs
  foreach my $job (@chargeList)
  {
        # call SOAP to check user and corresponding account 
        $done = $self->{SOAP}->CallSOAP("LBSG", "checkUserAccount", ($job->{'user'}, $job->{'userAccount'}));
        $done or ($self->setFailedToCharge($job->{'id'}, "SOAP Call to LBSG failed for 'checkUserAccount' (Job id: $job->{'id'})") and next);
        my $userAccountId = $done->result();
        chomp ($userAccountId);
        
        ($userAccountId =~ /^\d+\s*$/) or ($self->setFailedToCharge($job->{'id'}, "Failed to do 'checkUserAccount' for job $job->{'id'}: $userAccountId") and next);
                                                                                                                                      
        
        # call SOAP to check machine and corresponding account (get machine account) 
        $done = $self->{SOAP}->CallSOAP("LBSG", "checkMachineAccount", ($job->{'site'}, $job->{'siteAccount'}));
        $done or ($self->setFailedToCharge($job->{'id'}, "SOAP Call to LBSG failed for 'checkMachineAccount' (Job id: $job->{'id'})") and next);
        my $siteAccountId = $done->result();
        chomp ($siteAccountId);
        ($siteAccountId =~ /^\d+\s*$/) or ($self->setFailedToCharge($job->{'id'}, "Failed to do 'checkMachineAccount' for job $job->{'id'}: $siteAccountId") and next);
        

        # charge for the job
        $command = 'gcharge';
        my @ARGV = split (" ", "-J $job->{'id'} -p ALICE -u $job->{'user'} -m $job->{site} -P 1 -N 1 -t $job->{price} -d");
        push (@ARGV, "Charging $job->{'price'} credits for the job $job->{'id'} executed on $job->{'site'}");
        
        $done= $self->{SOAP}->CallSOAP ("LBSG", "bank", ($command, @ARGV));
        $done or ($self->setFailedToCharge($job->{'id'}, "SOAP Call to LBSG failed (for '$command @ARGV')") and next);
        $res = $done->result();
        ($res =~ /Successfully charged job/ ) or ($self->setFailedToCharge($job->{'id'}, $res) and next);
        

        # deposit money to site account 
        # $command = 'gdeposit';
        # @ARGV = split (" ", "-a $siteAccountId -z $job->{price} -d");
        # push (@ARGV, "Paying $job->{'price'} to site for the execution of $job->{'id'}");

        # $done= $self->{SOAP}->CallSOAP ("LBSG", "bank", ($command, @ARGV));
        # $done or ($self->setFailedToCharge($job->{'id'}, "SOAP Call to LBSG failed (for '$command @ARGV')") and next);
        # $res = $done->result();
        # ($res =~ /Successfully deposited/ ) or ($self->setFailedToCharge($job->{'id'}, $res) and next);
       
        # prepare data for paying sites for jobs 
        unless (defined $depositList->{$job->{'site'}})
        {
          $depositList->{$job->{'site'}}->{'amount'} = 0;
          $depositList->{$job->{'site'}}->{'jobs'} = "";
          $depositList->{$job->{'site'}}->{'accountId'} = $siteAccountId;
          $depositList->{$job->{'site'}}->{'accountName'} = $job->{'siteAccount'};

        }

        $depositList->{$job->{'site'}}->{'amount'} += $job->{'payToSite'};
        $depositList->{$job->{'site'}}->{'jobs'}   .= $job->{'id'}.", ";

        # calculate tax 
        $finalTax += $job->{'payTax'} if $taxRate;


        # change charged status in DB
      	$self->{DB}->do("UPDATE $queueTable SET chargeStatus=\'$self->{CHARGING_DONE}\' WHERE queueId=\'$job->{id}\' ");
        $self->info ("Successfully charged $job->{'user'} for the job $job->{'id'} ");    
  }
  
  # deposit money to site accounts
  foreach my $site (keys (%$depositList))
  {
    
    my $accountId = $depositList->{$site}->{'accountId'};
    my $jobs      = $depositList->{$site}->{'jobs'};
    my $amount    = $depositList->{$site}->{'amount'};
  
    next if ($amount < 1);
  
    # deposit money to site account 
    $command = 'gdeposit';
    @ARGV = split (" ", "-a $accountId -z $amount -d");
    push (@ARGV, "Paying $amount to $site for the execution of jobs: $jobs");

    $done= $self->{SOAP}->CallSOAP ("LBSG", "bank", ($command, @ARGV));
    $done or ($self->info("SOAP Call to LBSG failed (for '$command @ARGV')") and next);
    $res = $done->result();
    ($res =~ /Successfully deposited/ ) or ($self->info ("Failed to pay $site (account id: $accountId) for jobs: $jobs") and next);
    $self->info ("Successfully paid to $site for jobs: $jobs");
  }

  # deposit money to tax account 
  if ($taxRate)
  {
    # call SOAP to check tax account 
    $done = $self->{SOAP}->CallSOAP("LBSG", "checkAccount", ($taxAccount));
    if ($done)
    {
      my $taxAccountId = $done->result();
      chomp ($taxAccountId);

      if ( $taxAccountId =~ /^\d+\s*$/ )
      {
        # call SOAP to deposit money to tax account
        $command = 'gdeposit';
        @ARGV = split (" ", "-a $taxAccountId -z $finalTax -d");
        push (@ARGV, "Paying tax ($finalTax credits)");

        $done= $self->{SOAP}->CallSOAP ("LBSG", "bank", ($command, @ARGV));
        if ($done)
        {
          $res = $done->result();
          if ($res =~ /Successfully deposited/ )
          {
            $self->info ("Successfully deposited $finalTax credits to tax account \'$taxAccount\'");
          }
          else
          {
            $self->info ("Failed to deposit money on tax account: $res ");
          }
        }
        else 
        {
          $self->info("SOAP Call to LBSG failed (for '$command @ARGV')");
        }

      }
      else 
      {
        $self->info("Failed to do 'checkTaxAccount for $taxAccount (account Id is: $taxAccountId");
      }
    }
    else 
    {
      $self->info("SOAP Call to LBSG failed for 'checkTaxAccount' ($taxAccount)");
    }
    
  }

  #
  # Distribute money from accounts
  my $toAccountId = {};
 
  foreach my $site (keys (%$depositList)) # Loop through the sites to which funds were deposited 
  {
    my $accountName   = $depositList->{$site}->{'accountName'};
    my $fromAccountId = $depositList->{$site}->{'accountId'};

    my @distributionList = $self->getSiteDistributionList($site);

    @distributionList or next;

    #get money available on the site  account 
    my $availableCredits;
    $command = 'glsaccount';
    @ARGV = split (' ', "-n $accountName --show Amount --quiet");

    $done= $self->{SOAP}->CallSOAP ("LBSG", "bank", ($command, @ARGV));
    $done or ($self->info("SOAP Call to LBSG failed (for '$command @ARGV')") and next);
    $availableCredits = $done->result();
    chomp $availableCredits;
    $availableCredits =~ /^\s*(-?\d+)\s*$/;
    $availableCredits = $1;
    $availableCredits or ($self->info ("Failed to get available credits on the account of site \'$site\'") and next);
 
    unless ($availableCredits > 0)
    {
      $self->info ("Credits available on account of site \'$site\' are $availableCredits. Not doing distribution");
      next;
    }

    my $siteFractionSum = 0;
    my @transferList;

    #prepare trasfers list
    foreach my $distEntry (@distributionList)
    {
      my ($toAccount, $fraction) = split (':', $distEntry);
      $fraction or ($fraction = 1);
      $siteFractionSum += $fraction;

      unless (defined ($toAccountId->{$toAccount}))
      {
        $done = $self->{SOAP}->CallSOAP ("LBSG", "checkAccount", ($toAccount)); 
        $done or ($self->info("SOAP Call to LBSG failed (for 'checkAccount $toAccount')") and next);
        $toAccountId->{$toAccount} = $done->result; 
        chomp $toAccountId->{$toAccount}; 
        $toAccountId->{$toAccount} =~ /^(\d+)\s*$/;
        $toAccountId->{$toAccount} = $1;
        $toAccountId->{$toAccount} or ($self-info("Warning: Id of '\$toAccount'\ from distributeCreditsTo of site \'$site\' is not numeric.") and next);
        #$toAccountId->{$toAccount} or ($self-info("Warning: Id of '\$toAccount'\ from distributeCreditsTo of site \'$site\' is not numeric.") and next)

      }
      
      my $transferAmount = int ($availableCredits * $fraction + 0.5);
      
      push (@transferList, "--fromAccount $fromAccountId --toAccount  $toAccountId->{$toAccount} -z  $transferAmount" );
    }

    if ($siteFractionSum > 1)
    {
      $self->info ("Warning: Sum of fractions for distributions is invalid for site \'$site\'. Please correct 'distributeCreditsTo' in LDAP for \'$site\'.");
      next;
    }
    
    $self->info ("Starting to make transfers to distribute credits from site accounts");

    # make transfers 
    foreach my $transfer (@transferList)
    {
      $command = 'gtransfer';
      my @ARGV = split (" ", $transfer );
#      push (@ARGV, "-d \'Distributing credits from the account of $site\' ");
        
      $done= $self->{SOAP}->CallSOAP ("LBSG", "bank", ($command, @ARGV));
      $done or ($self->info( "SOAP Call to LBSG failed (for '$command @ARGV')") and next);
      $res = $done->result();
      ($res =~ /Successfully transferred/ ) or ($self->info ("Error: Command returned $res") and next);
      $self->info ("Successfully transferred credits from site account of $site.");
    }   
  }

  $self->{LDAP_CONNECTION}->unbind;
  return;
}

#
# Internal functions 
#

#
# retrieves the account for a given user 

sub getUserBankAccount {

  my $self=shift;
  my $username = shift;
	
  my $base = $self->{CONFIG}->{LDAPDN};
  my $entry;

  # seacrh users 
  my $mesg = $self->{LDAP_CONNECTION}->search(
  		    	                                   base   => "ou=People,$base",
		    	                                     filter => "(&(objectclass=pkiUser)(uid=$username))"
                        			               );
   
   
  if ( $mesg->count)
  {
       $entry = $mesg->entry(0);
       return $entry->get_value('accountName');
  }

  # search roles
  $mesg = $self->{LDAP_CONNECTION}->search(
                                           base   => "ou=Roles,$base",
                                           filter => "(&(objectClass=AliEnRole)(uid=$username))"
                                          );

  $mesg->count or return;
  $entry = $mesg->entry(0);
  return $entry->get_value('accountName');
	
}

sub getSiteBankAccount {

  my $self=shift;
  my $site = shift;
	
  my $base = $self->{CONFIG}->{LDAPDN};
  
  my $mesg = $self->{LDAP_CONNECTION}->search(
                            		              base   => "ou=Sites,$base",
		                                  	      filter => "(&(objectclass=AliEnSite)(ou=$site))"
  		    	                                 );

  $mesg->count or return;

   my $entry = $mesg->entry(0);
   return $entry->get_value('accountName');
}

sub getSiteDistributionList
{
  my $self = shift;
  my $site = shift;

  my $base = $self->{CONFIG}->{LDAPDN};

  my $mesg = $self->{LDAP_CONNECTION}->search(
                            		              base   => "ou=Sites,$base",
		                                  	      filter => "(&(objectclass=AliEnSite)(ou=$site))"
  		    	                                 );



  $mesg->count or return;

  my $entry = $mesg->entry(0);
  my @distributionList = $entry->get_value ('distributeCreditsTo');

  return @distributionList;
}

sub setFailedToCharge
{
    my $self = shift;
    my $jobId = shift;
    my $msg = shift;

    $self->info($msg);
    $self->{DB}->do("UPDATE QUEUE SET chargeStatus=\'$self->{CHARGING_FAILED}\' WHERE queueId=$jobId ");

      
return 1;
}

1;
