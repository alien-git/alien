# This optimizer will calculate the final price of the DONE jobs 
# and charge for them
package AliEn::Service::Optimizer::Job::Charge;


use strict;

use AliEn::Service::Optimizer::Job;
use vars qw(@ISA );
use Data::Dumper;


push (@ISA, "AliEn::Service::Optimizer::Job");

use AliEn::Util;

sub checkWakesUp 
{
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
    #my $update = " UPDATE $queueTable q, QUEUEPROC p SET finalPrice = round(p.si2k * $nominalPrice * price),chargeStatus=\'$self->{CHARGING_NOW}\'";
	#my $where  = " WHERE (status='DONE' AND p.si2k>0 AND chargeStatus!=\'$self->{CHARGING_DONE}\' AND chargeStatus!=\'$self->{CHARGING_FAILED}\') and p.queueid=q.queueid";
  
    my $updateStmt = $self->{DB}->getJobOptimizerCharge($queueTable,$nominalPrice,$self->{CHARGING_NOW},$self->{CHARGING_DONE},$self->{CHARGING_FAILED});
  
    $self->{DB}->do($updateStmt);

    # select job entries for current session 
    my $stmt = "SELECT queueId,submitHost,siteid,finalPrice FROM $queueTable WHERE 
     (chargeStatus=\'$self->{CHARGING_NOW}\') AND (finalPrice>0)";
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
        # # first check the hash, if it's not there get from LDAP
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
                              'payToSite'    => $amount,
                              'payTax'      => 0,
                          }; 
                           
        if ($taxRate)
        {
            $jobChargeData->{'payToSite'} = int ($amount * (1 - $taxRate));
            $jobChargeData->{'payTax'}    = $amount - $jobChargeData->{'payToSite'};
        }

        push (@chargeList, $jobChargeData);
        $self->info("Prepared charge list for job $jobId");
    }
  
 
    my $done;
    my $depositList = {};
    my $finalTax = 0;
    my $command;
    my $res;

    my $queueIdList = '';
    
    my $usersToCheck = { };
    my $sitesToCheck = { };
    my $chargeCommandsList = '';
    my $depositCommandsList = '';

    my $tmpAccount;

    # charge for jobs
    foreach my $job (@chargeList)
    {
        # Add user to the list for checking        
        $tmpAccount = $job->{'user'}.'#'.$job->{'userAccount'};
        $usersToCheck->{$tmpAccount} = 1;
        
        # Add site to the list for checking         
        my $siteAccountId = $job->{'siteAccount'}."_ID";
        $tmpAccount = $job->{'site'}.'#'.$job->{'siteAccount'}; 
        $sitesToCheck->{$tmpAccount} = 1; 

        # charge for the job (add command to the charge list)
        $chargeCommandsList and ($chargeCommandsList .= '###');
        $chargeCommandsList .= "gcharge -J $job->{'id'} -p ALICE -u $job->{'user'} -m $job->{site} -P 1 -N 1 -t $job->{price} -d ". 
                               " Charging $job->{'price'} credits for the job $job->{'id'} executed on $job->{'site'}";

        # deposit money to site account (add site to the list of sites to pay) 
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


        # prepare Job IDs for changing charge status in DB
        $queueIdList and ($queueIdList .= ' or ');
        $queueIdList .= " queueId='$job->{'id'}' ";        
        $self->info ("Successfully prepared charge information for the job $job->{'id'} ");    
    }
  
   
    # deposit money to site accounts
    foreach my $site (keys (%$depositList))
    {
        my $accountId = $depositList->{$site}->{'accountId'};
        my $jobs      = $depositList->{$site}->{'jobs'};
        my $amount    = $depositList->{$site}->{'amount'};

        next if ($amount < 1);
  
        # deposit money to site account (add comand to the deposit list)
        $depositCommandsList and ($depositCommandsList .= '###');
        $depositCommandsList .= "gdeposit -a $accountId -z $amount -d Paying $amount to $site for the execution of jobs: $jobs";
    }

    $self->{LDAP_CONNECTION}->unbind;
   
    # deposit money to tax account (add command to the deposit list) 
    if ($taxRate and $depositCommandsList)
    {   
        $tmpAccount = $taxAccount.'#'.$taxAccount;  
        $sitesToCheck->{$tmpAccount} = 1;
        my  $taxAccountId = $taxAccount."_ID";  
        
        $depositCommandsList .= '###';
        $depositCommandsList .= "gdeposit -a $taxAccountId -z $finalTax -d Paying tax $finalTax credits"; 
    }

    my $stringToSend = $chargeCommandsList."#####".$depositCommandsList."#####".join ('###', keys %$sitesToCheck)."#####".join('###', keys %$usersToCheck);
    $done = $self->{SOAP}->CallSOAP ('LBSG', "chargeOptimizer", $stringToSend); 
    
    my $changeChargeStatus;
    if ($done)
    {
        my $ok = $done->result;
        # change charged status in DB
        if ($queueIdList)
        { 
            $changeChargeStatus =  "UPDATE $queueTable SET chargeStatus=\'$self->{CHARGING_DONE}\' WHERE $queueIdList";
            $self->info ("Changing jobs charge status in DB. Doing $changeChargeStatus"); 
            $self->{DB}->do ($changeChargeStatus);
        }
        $self->info ("Sent charge request to LBSG. Got $ok");
        return;
    }
    else
    {
        $self->info("SOAP Call to LBSG failed (for '$stringToSend ')");
        # change charged status in DB
        if ($queueIdList)
        { 
            $changeChargeStatus =  "UPDATE $queueTable SET chargeStatus=\'$self->{CHARGING_FAILED}\' WHERE $queueIdList";
            $self->info ("Changing jobs charge status in DB. Doing $changeChargeStatus"); 
            $self->{DB}->do ($changeChargeStatus);
        }
        
        return;
    }
  
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
