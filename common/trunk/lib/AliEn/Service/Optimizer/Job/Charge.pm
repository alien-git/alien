# This optimizer will calculate the final price of the DONE jobs 
# and charge for them
package AliEn::Service::Optimizer::Job::Charge;


use strict;

use AliEn::Service::Optimizer::Job;
use vars qw(@ISA );


push (@ISA, "AliEn::Service::Optimizer::Job");

use AliEn::Util;

sub checkWakesUp {
  my $self=shift;
  my $silent=shift;
  $self->{SLEEP_PERIOD}=10 * 60 ; #once in ten minutes 
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
  	# we will use effectivePriority field to indicate the status of 'charging'
	# of the given entry:
	# -271828 - successfully charged 
	# -314159 - charging in process
	# -626606 - charging failed
		       
	# we will 'mark' job entries for current session of Charge optimizer by
	# seting their effective priority to '-314159'
 		      
  	  # 'mark' job entries for current session and   
	  # update finalPrice in $queueTable
  	my $update = " UPDATE $queueTable SET finalPrice = si2k * $nominalPrice * price,effectivePriority='-314159' ";
	my $where  = " WHERE (status='DONE' AND si2k>0 AND effectivePriority!='-271828' AND effectivePriority!='-626606') ";
  
  my $updateStmt = $update.$where;	
  
  $self->{DB}->do($updateStmt);

  # select job entries for current session 
  my $stmt = "SELECT queueId,submitHost,site,finalPrice FROM $queueTable WHERE  effectivePriority='-314159'";
  my $table = $self->{DB}->query("$stmt"); 
     @$table or ($self->info("No jobs to charge.Going to sleep...") and return);
     
  my $jobsListToCharge="";
  
  my ($queueId, $fromAccount, $toAccount, $amount);

  #make connection to LDAP (will be passed to internal functions)
  $self->{LDAP_CON} = Net::LDAP->new($self->{CONFIG}->{LDAPHOST}) or return 
 		"Error: Can't connect to LDAP server $self->{CONFIG}->{LDAPHOST}";
  $self->{LDAP_CON}->bind;

		  # declare a HASH where keys are usernames and values are corresponding bank
		  # accounts 
		  my %userBankAccount;   
		  # declare a HASH where keys are names of the sites and values are corresponding bank
		  # accounts 
		  my %siteBankAccount;   
  
		  my $_user;
		  my $_site;
 
  # prepare list of transactions 
  
  for (my $i = 0; $i < @$table; $i++)
  {
     #get the job ID
     $queueId = $table->[$i]->{'queueId'};	
     #######################
     # $queueID ready !    #
     #######################
 
     # get the user name from submitHost
       ($_user) = split ("@", $table->[$i]->{'submitHost'});

     # get the bank account name of $_user (we will charge from this account)
     # first check the hash, if it's not there get from LDAP
     $userBankAccount{$_user} or $userBankAccount{$_user} =
				     $self->getUserBankAccount($_user);

     $fromAccount = $userBankAccount{$_user};
     $fromAccount or (
   $self->info("Error: Can not charge for job no. $queueId . No bank account defined for user $_user") and next);
     #######################
     # $fromAccount ready! #
     #######################
   
     # get the name of the site from 'site' entry in QUEUE   
       (undef, $_site, undef) = split ("::", $table->[$i]->{'site'});
       
     # get bank account name of $_site (we will put money to this account
     # first check the hash, if it's not there get from LDAP
     $siteBankAccount{$_site} or $siteBankAccount{$_site} = 
     				    $self->getSiteBankAccount($_site);

     $toAccount = $siteBankAccount{$_site};
     $toAccount or (
   $self->info("Error: Can not charge for job no. $queueId . No bank  account defined for site $_site") and next);
     #######################
     # $toAccount ready!   #
     #######################
     
     
     $amount = $table->[$i]->{'finalPrice'};
     $amount or  ($self->info("Error: Can not charge for job. no $queueId . No  finalPrice defnied") and next);
     ######################
     # $amount ready !    #
     ######################

     
     $jobsListToCharge .= $queueId.":".$fromAccount.":".$toAccount.":".$amount."\n";  	
     
  }
  
  $self->{LDAP_CON}->unbind;
  # charge for them 


  
  my $done = $self->{SOAP}->CallSOAP("LBSG", "transactFundsList", $jobsListToCharge); 
  
  if (! $done ){
	$self->info("Error: Can not charge for jobs. SOAP call to LBSG failed");
	$self->info("Setting jobs to '-626606' (charging failed) ");

        $update = " UPDATE $queueTable SET effectivePriority='-626606' WHERE effectivePriority='-314159' ";
  
	$self->{DB}->do($update);
	return;
  }
 
  # change the status (value of effectivePriority) accordingly 

  my $result = $done->result;

  # everything went well
  if ($result eq 1) 
  {
     $update = " UPDATE $queueTable SET effectivePriority='-271828' WHERE effectivePriority='-314159' ";	  
     $self->{DB}->do($update);
     $self->info("Charger optimizer worked. All jobs were charged");
     return;
  }
  
  # some transactions failed 
  my @failed = split ("\n", $result);
  my ($failedId, $reason);
  
  foreach (@failed){
	($failedId, $reason) =  split (":",$_);
        $self->info("Error: Can not charge for job no $failedId: $reason");
	$self->info("Setting job no $failedId to '-626606' (charging failed) ");

        $update = " UPDATE $queueTable SET effectivePriority='-626606' WHERE queueId='$failedId' ";
  	$self->{DB}->do($update);
  }

  # Failed transactions are now set,  which means that transactions which are
  # still in progress (-314159) went without errors
  
      $update = " UPDATE $queueTable SET effectivePriority='-271828' WHERE effectivePriority='-314159' ";
      $self->{DB}->do($update);

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
  my $mesg = $self->{LDAP_CON}->search(
  		    	base   => "ou=People,$base",
		    	filter => "(&(objectclass=pkiUser)(uid=$username))"
  			);
   if ( !$mesg->count ) {
	 # perform search through all roles' entries
                    $mesg = $self->{LDAP_CON}->search(
                         base   => "ou=Roles,$base",
                         filter => "(&(objectClass=AliEnRole)(uid=$username))"
                                     );

                 if (!$mesg->count){
                        # User not found in LDAP !!!
                        return;
                                   }

                # found in roles
                  $entry = $mesg->entry(0);
                  return $entry->get_value('accountName');



	# User not found in LDAP !!!
	   return;
   }

   $entry = $mesg->entry(0);
   return $entry->get_value('accountName');
 
 	
}

sub getSiteBankAccount {

  my $self=shift;
  my $site = shift;
	
  my $base = $self->{CONFIG}->{LDAPDN};
  
  my $mesg = $self->{LDAP_CON}->search(
  		    	base   => "ou=Sites,$base",
		    	filter => "(&(objectclass=AliEnSite)(ou=$site))"
  			);
   if ( !$mesg->count ) {
	#  Site not found in LDAP !!!
	   return;
   }

   my $entry = $mesg->entry(0);
   return $entry->get_value('accountName');
 	
}



1;
