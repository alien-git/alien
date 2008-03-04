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
  $self->{SLEEP_PERIOD} = 60 * 10; #once in ten minutes 
  my $method="info";
  $silent and $method = "debug";
    
  $self->{LOGGER}->$method("Charge",  "Charge optimizer starts");
    
 my $queueTable = "QUEUE";
 
  #  my $config = new AliEn::Config;
  # get nominal price from LDAP 
  my $nominalPrice = $self->{CONFIG}->{SI2K_NOMINAL_PRICE};
  # if nominal price is not defined in LDAP set it to '1'
     if (!$nominalPrice){ 
     $nominalPrice=1; 
     $self->info("Warning: Nominal price for si2k not set in LDAP. Seting Seting nominal price to 1");
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
  my $stmt = "SELECT queueId,submitHost,site,finalPrice FROM $queueTable WHERE  chargeStatus=\'$self->{CHARGING_NOW}\'";
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
                           'id' => $jobId,
                           'user'  => $user,
                           'userAccount' => $userAccount,
                           'site'  => $site,
                           'siteAccount' => $siteAccount,
                           'price' => $amount
                         }; 
  
     push (@chargeList, $jobChargeData);
     $self->info("Prepared charge list for job $jobId");


  }
  
  $self->{LDAP_CONNECTION}->unbind;
 
  my $done;

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
        my $command = 'gcharge';
        my @ARGV = split (" ", "-J $job->{'id'} -p ALICE -u $job->{'user'} -m $job->{site} -P 1 -N 1 -t $job->{price} -d");
        push (@ARGV, "Charging $job->{'price'} credits for the job $job->{'id'} executed on $job->{'site'}");
        
        $done= $self->{SOAP}->CallSOAP ("LBSG", "bank", ($command, @ARGV));
        $done or ($self->setFailedToCharge($job->{'id'}, "SOAP Call to LBSG failed (for '$command @ARGV')") and next);
        my $res = $done->result();
        ($res =~ /Successfully charged job/ ) or ($self->setFailedToCharge($job->{'id'}, $res) and next);
        

        # deposit money to site account 
        $command = 'gdeposit';
        @ARGV = split (" ", "-a $siteAccountId -z $job->{price} -d");
        push (@ARGV, "Paying site $job->{'price'} for the execution of $job->{'id'}");

        $done= $self->{SOAP}->CallSOAP ("LBSG", "bank", ($command, @ARGV));
        $done or ($self->setFailedToCharge($job->{'id'}, "SOAP Call to LBSG failed (for '$command @ARGV')") and next);
        $res = $done->result();
        ($res =~ /Successfully deposited/ ) or ($self->setFailedToCharge($job->{'id'}, $res) and next);
       
         # change charged status in DB
      	 $self->{DB}->do("UPDATE $queueTable SET chargeStatus=\'$self->{CHARGING_DONE}\' WHERE queueId=\'$job->{id}\' ");
         $self->info ("Successfully charged $job->{'user'} for the $job->{'id'} ");    
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
