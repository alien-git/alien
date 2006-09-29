package AliEn::Service::LBSG;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;
 
use strict;

use Net::LDAP;

use AliEn::Database::TaskQueue;
use AliEn::Service::Manager;
use AliEn::JOBLOG;
use AliEn::Util;
use Classad;
use AliEn::Database::Admin;
#use AliEn::Config;

use vars qw (@ISA $DEBUG);
@ISA=("AliEn::Service");

$DEBUG=0;

my $self = {};

############################################################################
############################################################################
##                                                                        ##
##    			 Private (internal) functions                     ## 
##                                                                        ##
############################################################################
############################################################################

#
# Does the user authentication 
 my $_authenticate  = sub {

     my $config = AliEn::Config->new();
     
     my $ldap = Net::LDAP->new( $config->{LDAPHOST}) or return
	"Error: Can't connect to ldap server $config->{LDAPHOST}";
	$ldap->bind();

	
	my $base = $config->{LDAPDN};

	my $mesg;
	my $entry;
	my $subject;
	my $bankAdmin;

	my @adminList;

	$config->{BANK_ADMIN_LIST} and 
			@adminList = @{$config->{BANK_ADMIN_LIST}};
 	
	if ( @adminList )
	{
		foreach $bankAdmin (@adminList) {
 			#get the cert subject for $bankAdmin user
	
			$mesg = $ldap->search (
				base   => "ou=People, $base",
				filter => "(&(objectclass=pkiUser)(uid=$bankAdmin))" );

			$mesg->count or next;
			$entry = $mesg->entry(0);
			$subject = $entry->get_value('subject');

			# see if it is equal to one which came with SSL

			($subject eq $ENV{SSL_CLIENT_I_DN}) or next;
			$ldap->unbind;
			return 1;
			
		}
	    $ldap->unbind;      
	    return "Error: User is not a bank admin";
	}
	
	$ldap->unbind;
	return "No bank admins defined in LDAP";
};

#
# Internal function for funds transaction
my $_transactFundsEx = sub {

	my $fromAccount   = shift;
	my $toAccount = shift;
	my $amount      = shift;
	
	$self->info("Transfering $amount from $fromAccount to $toAccount ");

	# Try to do fund transaction
	# 'prepare' statements
         my $stmtFrom = "UPDATE $self->{BALANCE} SET balance = balance - $amount WHERE groupName='$fromAccount'";
	 my $stmtTo   = "UPDATE $self->{BALANCE} SET balance = balance + $amount WHERE groupName='$toAccount'";
	
	 my $details = { toGroup   => $toAccount, 
		         fromGroup => $fromAccount, 
			 amount    => $amount,
			 initiator => $ENV{SSL_CLIENT_I_DN},
		       };

	# Add debug output here $self->debug($stmt);
        # 
	  $self->debug(1, "In LBSG: trying to execute $stmtFrom");
          $self->debug(1, "In LBSG: trying to execute $stmtTo");

	#
	# We need to update BALANCE table, and record fund TRANSACTION simultaneously ! 
	# We will turn off auto commit, try to execute two statements, and commit them together 
        # Since AlienProxy.pm does not support TRANSACTIONS we turn them off
	#$self->{DB}->{DBH}->{'AutoCommit'} = 0;
	#$self->{DB}->{DBH}->{'RaiseError'} = 1;

	my $res1 = "";
	my $res2 = "";	
	my $res3 = "";
	eval {
 	
	 $res1 = $self->{DB}->do($stmtFrom);
         
 	 ($res1 != 0) and ($res2 = $self->{DB}->do($stmtTo));
	 ($res2 != 0) and ($res3 = $self->{DB}->insert("$self->{TRANSACTION}", $details));
	 #       $self->{DB}->{DBH}->commit();  
        };
	if ($@) {
           eval { $self->{DB}->{DBH}->rollback(); };
	   $self->info("Error: In transactFunds SQL queries failed: $@");	
  	}

	#  Turn autocommit back on 
	#  $self->{DB}->{DBH}->{'AutoCommit'} = 1;

	($res1 != 0) or return "Error: $stmtFrom failed"; 
	($res2 != 0) or return "Error: $stmtTo failed"; 
	($res3 != 0) or return "Error: INSERT into $self->{TRANSACTION} table failed. Transaction not registered !!!";

	return 1;

};

#
# Internal function for bank account creation 

my $_createBankAccountEx = sub {

	my $account = shift;
	my $amount  = shift;

	$amount or $amount = 0;

    my $details = { balance => $amount, groupName => $account};
   
    my $res = $self->{DB}->insert("$self->{BALANCE}", $details);
    
    #
    # Check the result after {DB}->insert() call 
    # If everything was OK, then $res should be '1'
    ($res != 0) and return 1;
    return "Error: Failed to create bank account for $account";

};

#
# Internal function which retrieves users' and sites' bank account names from LDAP

my $_getBankAccounts = sub {

     my $config = AliEn::Config->new();
     
     my $ldap = Net::LDAP->new( $config->{LDAPHOST}) or return
	"Error: Can't connect to ldap server $config->{LDAPHOST}";
	$ldap->bind();

     my $base = $config->{LDAPDN};

     my $entry;
     my $_account;
     my $i;

     my %accounts;


     # perform search through all users' entries 
     my $mesg = $ldap->search(
     		 base   => "ou=People,$base",
		 filter => "(objectClass=AliEnUser)"   	
                             );

     # this will loop over all users' entries from LDAP and fill in the  hash 
     # where keys will be the names of bank accounts 
     
	     for ($i = 0; $i < $mesg->count; $i++)
	     {
		     $entry = $mesg->entry($i);
	     
		     $_account = $entry->get_value('accountName');
		     $accounts{$_account} = 1;
	     }

     # perform search through all roles' entries 
      $mesg = $ldap->search(
     		 base   => "ou=Roles,$base",
		 filter => "(objectClass=AliEnRole)"   	
                             );

     # this will loop over all users' entries from LDAP and fill in the  hash 
     # where keys will be the names of bank accounts 
     
	     for ($i = 0; $i < $mesg->count; $i++)
	     {
		     $entry = $mesg->entry($i);
	     
		     $_account = $entry->get_value('accountName');
		     $accounts{$_account} = 1;
	     }

     # perform search through site entries 
       $mesg = $ldap->search(
     		 base   => "ou=Sites,$base",
		 filter => "(objectClass=AliEnSite)"   	
                             );

	     # loop over all sites' entries from LDAP and fill in the hash 
	     for ($i = 0; $i < $mesg->count; $i++)
	     {
		     $entry = $mesg->entry($i);
		     
		     $_account = $entry->get_value('accountName');
		     $accounts{$_account} = 1;
	     }
  
    
     
     # prepare the return value of the function which is the list of all accounts
     # delimited by ':'
     
     my $ret="";
     foreach (keys(%accounts)){
     	$ret .= $_;
	$ret .= ":"
     }

     return $ret;
};

#
# Internal function which checks the existence of the given accounts, if they don't exists creates
# them 

my $_checkAccounts = sub {
	
	my $_accounts  = shift;
	$_accounts or return "Error: No account names given.";

	my $amount = shift;
           $amount or $amount = 0;	
	my @accounts = split (":", $_accounts);
        my $account; 
	my $res;
	
	foreach $account (@accounts){
	$res = $self->{DB}->queryValue("SELECT groupName FROM $self->{BALANCE} WHERE groupName='$account'");
	  $res and next;
	  $account or next;
            $self->info("In checkAccounts, creating bank account for user  $account");
	  $res = $_createBankAccountEx->($account, $amount);	  
	  ($res eq 1) or return $res; 	
	  $self->info("In checkAccounts, account for user $account created ");
	}
	
	return 1;
};


############################################################################
############################################################################
##                                                                        ##
##                       Service Initialization function                  ##
##                                                                        ##
############################################################################
############################################################################

sub initialize {
  $self     = shift;
  my $options =(shift or {});
  
  $DEBUG and $self->debug(1, "In initialize initializing service LBSG" );
  $self->{SERVICE}="LBSG";
  $self->{SERVICENAME}="LBSG";
  
  $self->{DB_MODULE}="AliEn::Database::TaskQueue";
  
  ## Taken from Manager/Job.pm, needed, since we are connecting to the same DB
  ## as the Job Manager

  my $name = "LBSG";
  my ($host, $driver, $db) =
            split ("/", $self->{CONFIG}->{"${name}_DATABASE"});

  ($self->{HOST}, $self->{PORT})=
            split (":", $self->{CONFIG}->{"${name}_ADDRESS"});


  $self->{LISTEN}=1;
  $self->{PREFORK}=5;
 
  $options->{role}="admin";
         $ENV{ALIEN_DATABASE_SSL} and delete $options->{role};

#   $self->{CATALOGUE} = AliEn::UI::Catalogue->new($options)
#          or $self->{LOGGER}->error( "LBSG", "In initialize error creating AliEn::UI::Catalogue instance" )
#	                 and return;
 

  $self->{DB_MODULE}="AliEn::Database::TaskQueue";     
  my $role="admin";

  $ENV{ALIEN_DATABASE_SSL} and $role.="ssl";

  if ( (defined $ENV{ALIEN_NO_PROXY}) && ($ENV{ALIEN_NO_PROXY} eq "1") && (defined $ENV{ALIEN_DB_PASSWD}) ) {
     $self->{DB} = "$self->{DB_MODULE}"->new({DB=>$db,HOST=> $host,DRIVER =>$driver,ROLE=>$role,"USE_PROXY" => 0,
		   PASSWD=>"$ENV{ALIEN_DB_PASSWD}"});
  } else {
     $self->{DB} = "$self->{DB_MODULE}"->new({DB=>$db,HOST=> $host,DRIVER =>$driver,ROLE=>$role});	  
  }


  $self->{DB} 
  	or $self->{LOGGER}->error( "LBSG", "In initialize creating $self->{DB_MODULE} instance failed" )
      	   and return;
     
  if ($ENV{'ALIEN_N_MANAGER_JOB_SERVER'}  ) {
      $self->{PREFORK} = $ENV{'ALIEN_N_MANAGER_JOB_SERVER'};
  }

  $self->{TRANSACTION} = "TRANSACTION";
  $self->{BALANCE}     = "BALANCE";

  my $accountsList = $_getBankAccounts->();
     $accountsList or $self->{LOGGER}->error(
	  "LBSG",  "In initalize failed to get get the bank accounts' names from LDAP") 
           and return;
  
  my $res = $_checkAccounts->($accountsList); 
  ($res eq 1) or $self->{LOGGER}->error(
	   "LBSG", "In initialize failed to checkAccounts. Error is $res")
            and return;
  
  return $self;
}

############################################################################
############################################################################
##                                                                        ##
##                             Public functions                           ##
##                                                                        ##
############################################################################
############################################################################


#
# getTransactions - return the transfers of a given user. If username if not
# given returns all the transfers 
# 

sub getTransactions {
     shift;
       
	my $account = shift || "";

	my $res1 = "";
	my $res2 = "";

	if ($account){
		$self->info("Geting transactions for $account");

		my $stmt1 = "SELECT * FROM $self->{TRANSACTION} WHERE toGroup='$account'";
		my $stmt2 = "SELECT * FROM $self->{TRANSACTION} WHERE fromGroup='$account'";

		# Add debug output here $self->debug($stmt);
	         $self->debug(1, "In LBSG: trying to execute $stmt1 ");
 	         $self->debug(1, "In LBSG: trying to execute $stmt2 ");

		eval {
		$res1 = $self->{DB}->query($stmt1);			
		$res2 = $self->{DB}->query($stmt2);
		};
		if ($@){
		$self->info("Error: In getTransactions SQL query failed: $@");
		return $@;
		}
	}
	else ## gotta fetch'em all
	{
		$self->info("Getting all transactions");
	
		my $stmt = "SELECT * FROM $self->{TRANSACTION}";
	         $self->debug(1, "In LBSG: trying to execute $stmt ");
	

		# TODO
	        # Add debug output here $self->debug($stmt);
	        #
	        # $self->info($stmt);


		eval{
		$res1 = $self->{DB}->query($stmt);
		};
		if ($@){
		$self->info("Error: In getTransactions SQL query failed: $@");
		return $@;
		}
	}


	my @transactions;

	# Prepare the return data (inspired from alien 'ps' and 'top')
	# 
	if ($res1){
         for (@$res1){
	   push @transactions, join ("###", $_->{toGroup}, $_->{fromGroup},
		                            $_->{amount},  $_->{moment}, 
					    $_->{initiator});
	   }	
	}

	if ($res2){
         for (@$res2){
	    push @transactions, join ("###", $_->{toGroup}, $_->{fromGroup},
		                             $_->{amount}, $_->{moment}, 
					     $_->{initiator});
	    }
        }
	
	(@transactions) or (push @transactions, "\n");

	return join ("\n", @transactions);
}

#
# getBalance - return the amount of funds on a given account 
# 
sub getBalance {
     shift;
           
	my $account = shift;  
           $account or return "No account name given ! Exiting.";
	
	$self->info("Geting fund remainder on $account ");
        my $res;
 
	# workaround for remainder=0 case
        my $stmt = "SELECT groupName from $self->{BALANCE} where groupName='$account'";
	        eval {
	        $res = $self->{DB}->queryValue($stmt);
	        };
	        if ($@){
	        $self->info("Error: In getBalance SQL query failed: $@");
	        return $@;
	        }
        $res or return "Account: $account does not exist";


	$stmt = "SELECT balance from $self->{BALANCE} where groupName='$account'";

	
	# debug output here $self->debug($stmt);
          $self->debug(1, "In LBSG: trying to execute $stmt");
 
	eval {
	$res = $self->{DB}->queryValue($stmt);			
	};
	if ($@){
	$self->info("Error: In getBalance SQL query failed: $@");
	return $@;
	}
        
        $res or $res="O";
 	return $res;
}



#
# transactFunds - transacts funds between given 2 accounts  
# 

sub transactFunds {
     shift;

     	my $fromAccount = shift;
	my $toAccount   = shift;
	my $amount  = shift;

	# A regexp to validate that $amount is a number
	 $amount =~ m/^\s*\d+[\.\d+]*\s*$/ or 
	     ($self->info("Warinng: Invalid input from user. Amount is not numeric, exiting.") and return
		     "Error: amount must be numeric !!!");
 
      $fromAccount or  
      ($self->info("Warinng: Invalid input from user. Account name is not specified exiting.") 
		     and return "Error: acount name is not specified");

      $toAccount or  
      ($self->info("Warinng: Invalid input from user. Account name is not specified exiting.") 
		     and return "Error: acount name is not specified");

      #  AUTHENTICATE !!!!!!!!!!!
      #
       my $authErr = $_authenticate->(); 
         ($authErr eq 1) or return $authErr; 

	 return $_transactFundsEx->($toAccount, $fromAccount, $amount);
}

#
# transactFundsList - for making multiple transactions  
# 

sub transactFundsList {
     shift;


      #  AUTHENTICATE !!!!!!!!!!!
      #
      my $authErr = $_authenticate->(); 
        ($authErr eq 1) or return $authErr; 

      my $_list = shift;
	 $_list or return "Error no list given";
      
      my @list = split ("\n", $_list);
      my @res;
      my $_res;


      # @elem will contain $transactionID, $fromAccount, $toAccount and $amount
      my @elem;
      my $err;

	foreach (@list)
	{
	  @elem = split (":", $_);
	  $_res = $_transactFundsEx->($elem[1], $elem[2], $elem[3]);
	 ($_res eq '1') and next;
	  
	 $err = "$elem[0]:$_res";
	  push @res, $err;
	   
	}
	
	# if everything went well ;-) return 1
	@res or return 1; 

	#otherwise return list of failures
	return join ("\n", @res);
	
}


#
# addFunds - adds given amount of funds (maybe be negative) to the given account 
# 
sub addFunds {
     shift;
         
	my $account = shift;
	my $amount  = shift;

 	# A regexp to validate that $amount is a number
	 $amount =~ m/^\s*-?\d+[\.\d+]*\s*$/ or 
	     ($self->info("Warinng: Invalid input from user. Amount is not numeric, exiting.") and return
		     "Error: amount must be numeric !!!");
	
     $account or  ($self->info("Warinng: Invalid input from user. Account name is not specified exiting.") 
		     and return "Error: acount name is not specified");

      #  Authenticate 
      #
       my $authErr = $_authenticate->(); 
         ($authErr eq 1) or return $authErr; 

	$self->info("Adding $amount to $account\'s account ");

	# 'prepare' statement
         my $stmt = "UPDATE $self->{BALANCE} SET balance = balance + $amount WHERE groupName='$account'";
	
	 my $details = { toGroup   => $account, 
		         fromGroup => "DADDY", 
			 amount    => $amount,
			 initiator => $ENV{SSL_CLIENT_I_DN},
		       };

	# TODO 
	# Add debug output here $self->debug($stmt);
        # 
	 $self->debug(1, "In LBSG: Trying to execute $stmt" );
	
	#
	# We need to update BALANCE table, and record fund TRANSACTION simultaneously ! 
	# We will turn off auto commit, try to execute two statements, and commit them together 
        # Since AlienProxy.pm does not support TRANSACTIONS we turn them off
	#$self->{DB}->{DBH}->{'AutoCommit'} = 0;
	#$self->{DB}->{DBH}->{'RaiseError'} = 1;
	 
 	my ($res1, $res2);	
	eval {
 	 $res1 = $self->{DB}->do($stmt);
  	 ($res1 != 0) and ($res2 = $self->{DB}->insert("$self->{TRANSACTION}", $details));
	 #    $self->{DB}->{DBH}->commit();  
        };
	if ($@) {
           eval { $self->{DB}->{DBH}->rollback(); };
	   $self->info("Error: In addFunds SQL queries failed: $@");	
  	}

	# Turn autocommit back on 
	# $self->{DB}->{DBH}->{'AutoCommit'} = 1;


	($res1 != 0) or return "Error: $stmt failed";
	($res2 != 0) or return "Error: INSERT into $self->{TRANSACTION} table failed. Transaction not registered";
	return 1;
}

sub createBankAccount {
    shift;
   
    my $account = shift;
    my $amount  = shift;

    $amount or $amount = 0;
    
    	# A regexp to validate that $amount is a number
	 $amount =~ m/^\s*\d+[\.\d+]*\s*$/ or 
	     ($self->info("Warinng: Invalid input from user. Amount is not numeric, exiting.") 
			     and return "Error: amount must be numeric !!!");
 
     $account or  ($self->info("Warinng: Invalid input from user. Account name is not specified exiting.") 
		     and return "Error: acount name is not specified");

      #  Authenticate 
      #
       my $authErr = $_authenticate->(); 
         ($authErr eq 1) or return $authErr; 

	 return $_createBankAccountEx->($account, $amount);
    
}

sub deleteBankAccount  {
  shift;
      my $authErr = $_authenticate->(); 
         ($authErr eq 1) or return $authErr; 

	my $account = shift;
	$account or return "Error: No account name specified";

   
    my $res = $self->{DB}->do("DELETE from $self->{BALANCE} WHERE groupName='$account'");
    $self->info("Deleting account $account");
    
    #
    # Check the result after {DB}->do() call 
    # If everything was OK, then $res should be '1'
    ($res != 0) and return 1;
    return "Error: Failed to delete account $account";

}

1;


