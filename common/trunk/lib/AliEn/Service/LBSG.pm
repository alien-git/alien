package AliEn::Service::LBSG;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;
 
use strict;
use warnings;

use Net::LDAP;

use AliEn::Service::Manager;
use AliEn::Util;
use Classad;

use AliEn::Config;

use vars qw (@ISA $DEBUG);
@ISA=("AliEn::Service");

$DEBUG=0;

my $self = {};

############################################################################
##                       Internal functions                               ##
############################################################################
my $_initCommands = sub 
{
        $self->{COMMANDS} = {};
        # init anonymous commands 
        $self->{COMMANDS}->{"gbalance"}   = { "privilege" => "anon", "code" => ""};
        $self->{COMMANDS}->{"glsmachine"} = { "privilege" => "anon", "code" => ""};
        $self->{COMMANDS}->{"gstatement"} = { "privilege" => "anon", "code" => ""};
        $self->{COMMANDS}->{"glsproject"} = { "privilege" => "anon", "code" => ""};
        $self->{COMMANDS}->{"glsquote"}   = { "privilege" => "anon", "code" => ""};
        $self->{COMMANDS}->{"gusage"}     = { "privilege" => "anon", "code" => ""};
        $self->{COMMANDS}->{"glsaccount"} = { "privilege" => "anon", "code" => ""};
        $self->{COMMANDS}->{"glstxn"}     = { "privilege" => "anon", "code" => ""};
        $self->{COMMANDS}->{"glsres"}     = { "privilege" => "anon", "code" => ""};
        $self->{COMMANDS}->{"glsjob"}     = { "privilege" => "anon", "code" => ""};
        $self->{COMMANDS}->{"glsalloc"}   = { "privilege" => "anon", "code" => ""};
        $self->{COMMANDS}->{"glsuser"}    = { "privilege" => "anon", "code" => ""};
        #init user commands
        $self->{COMMANDS}->{"gtransfer"}  = { "privilege" => "user", "code" => ""};
        #init admin commands 
        $self->{COMMANDS}->{"goldsh"}     = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"gchquote"}   = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"gmkmachine"} = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"grmaccount"} = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"gchaccount"} = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"gchres"}     = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"gmkproject"} = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"grmalloc"}   = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"gchalloc"}   = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"gchuser"}    = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"gmkuser"}    = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"grmmachine"} = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"gdeposit"}   = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"grmproject"} = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"gwithdraw"}  = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"gchmachine"} = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"gquote"}     = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"grmquote"}   = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"gchpasswd"}  = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"grefund"}    = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"grmres"}     = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"gchproject"} = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"grmaccount"} = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"gmkaccount"} = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"greserve"}   = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"grmuser"}    = { "privilege" => "admin", "code" => ""};
        $self->{COMMANDS}->{"gcharge"}    = { "privilege" => "admin", "code" => ""};


        $self->{ADMIN_COMMANDS} = {};
        $self->{USER_COMMANDS} = {};

        foreach my $key (keys (%{$self->{COMMANDS}}))
        {
             my $priv = $self->{COMMANDS}->{$key}->{"privilege"};

            ($priv eq "admin") and ($self->{ADMIN_COMMANDS}->{$key} = \$self->{COMMANDS}->{$key}->{"code"}) and next;
            ($priv eq "user")  and ($self->{USER_COMMANDS}->{$key} = \$self->{COMMANDS}->{$key}->{"code"}) 
        }
        
};

my $_authenticateAdmin = sub 
{

    my $config = AliEn::Config->new();
     
    my $ldap = Net::LDAP->new( $config->{LDAPHOST}) 
                or return "Error in banking service: Can't connect to ldap server $config->{LDAPHOST}";
	    
    $ldap->bind();
    
    my $base = $config->{LDAPDN};
     
    my $mesg;
    my $entry;
    my $subject;
    my $bankAdmin;
    my @adminList;

    $config->{BANK_ADMIN_LIST} and @adminList = @{$config->{BANK_ADMIN_LIST}};
 
    if ( @adminList )
	 {
		foreach $bankAdmin (@adminList) 
        {
            
            #try to match DN first
            if ( (lc($bankAdmin) eq lc($ENV{SSL_CLIENT_I_DN})) or ( lc($bankAdmin) eq lc($ENV{SSL_CLIENT_S_DN})) )
            {
                $ldap->unbind();
                return 1;
            }

 			#get the cert subject for $bankAdmin user
			$mesg = $ldap->search (
                                    base   => "ou=People, $base",
                                    filter => "(&(objectclass=pkiUser)(uid=$bankAdmin))" 
                                  );

			$mesg->count or next;
			$entry = $mesg->entry(0);
			$subject = $entry->get_value('subject');

			# see if the DN of the user from LDAP matches $ENV{SSL_CLIENT_I_DN} or $ENV{SSL_CLIENT_S_DN}
			( (lc($subject) eq lc($ENV{SSL_CLIENT_I_DN})) or ( lc($subject) eq lc($ENV{SSL_CLIENT_S_DN}) )) or next;
			$ldap->unbind;
			return 1;
			
		}
	    $ldap->unbind;      
	    return "Error: User is not a bank admin.";
	}
	
	$ldap->unbind;
	return "No bank admins defined in LDAP.";
};

#
# Loads the help and man pages for bank command
my $_loadHelp = sub
{
    my $command = shift;

    # load help
    system("$ENV{ALIEN_ROOT}/bin/alien-perl -T $ENV{ALIEN_ROOT}/bin/$command --help > /tmp/bankHelp 2>&1");
    open HELP, "< /tmp/bankHelp";
    my $help = join ("", <HELP>);
    $self->{COMMANDS}->{$command}->{"help"} = $help;
    close HELP;

    # load man
    system("$ENV{ALIEN_ROOT}/bin/alien-perl -T $ENV{ALIEN_ROOT}/bin/$command --man > /tmp/bankMan 2>&1");
    open MAN, "< /tmp/bankMan";
    my $man = join ("", <MAN>);
    $self->{COMMANDS}->{$command}->{"man"} = $man;
    close MAN;
};

#
# Loads bank command from file 
my $_loadCommand = sub 
{
    my $command = shift;
    
    $_loadHelp->($command);

    open FILE, "< $ENV{ALIEN_ROOT}/bin/$command" or return "Error in banking service: Failed to open $ENV{ALIEN_ROOT}/bin/$command: $!";
    my $tmp;

        
    while ($tmp = <FILE>)
    {
         # strip comments out 
         $tmp =~ /^\#.*/ and next;   

         # stop when __END__ is reached
         $tmp =~ /^__END__/ and return 1;

         $self->{COMMANDS}->{$command}->{"code"} .= $tmp;
    }
    


    return 1;
};



#
# Executes the command
my $_exec = sub
{       
        my $showError = shift;
        my $command = shift;
           @ARGV = @_;
        my $argvStr = join (" ", @ARGV);

        $ENV{GOLD_HOME} || ($ENV{GOLD_HOME} = $ENV{ALIEN_ROOT});

				my $codeRef =  \$self->{COMMANDS}->{$command}->{"code"};				
				
        # Load the command if necessary
        if ($$codeRef eq "") 
        {
            my $stat = $_loadCommand->($command);

            #check if the command code has been loaded 
            ($stat eq 1) or ($self->info ($stat) and return $stat);
        }
        
        # check if help needs to be returned 
        ($argvStr =~ /--man/)  and return $self->{COMMANDS}->{$command}->{"man"};
        ($argvStr =~ /--help/) and return $self->{COMMANDS}->{$command}->{"help"};

				pipe (FROM_CHILD_OUT, TO_PARENT_OUT) or die "Can not open pipe on server: $!";
				pipe (FROM_CHILD_ERR, TO_PARENT_ERR) or die "Can not open pipe on server: $!";

				# fork to execute actual bank command
				my $pid = fork();

				if ($pid == 0) #child
				{
						close FROM_CHILD_OUT;
						close FROM_CHILD_ERR;
						close STDERR;
						open (STDERR, ">&TO_PARENT_ERR");
						select STDERR; $|=1;
						select (TO_PARENT_OUT); $|=1;

			  		my $fun = sub { eval $$codeRef; };
      			my $retCode = $fun->();  

						close TO_PARENT_OUT;
						close TO_PARENT_ERR;

						# we have to kill the child, otherwise the parent will never return	
						`kill -9 $$`;	       
				}
	
				die "Can not fork $!" unless defined $pid;

				close TO_PARENT_OUT;
				close TO_PARENT_ERR;

				waitpid ($pid, 0); # wait untill child finishes 				

				my $out; 
				while (my $line = <FROM_CHILD_OUT>)
				{	$out .= $line; }

				my $err; 
				while (my $line = <FROM_CHILD_ERR>)
				{	$err .= $line; }

				close FROM_CHILD_OUT;	
				close FROM_CHILD_ERR;	

				$out and (chomp $out);
				$err and (chomp $err);

			
        $showError and return $out."\n".$err."\n";
        return $out;
};

#
# Retrieves the Bank account ID of the user 
my $_getAccountIdFromAccountName = sub
{
    my $goldAccountName = shift;        
    my $id = $_exec->(0, 'glsaccount', ('-n', $goldAccountName,'--show', 'Id', '--quiet'));

    return $id;
};
#
# retrieves the bank account of the user usibg $ENV{SSL_CLIENT_I_DN}
my $_getUserBankAccount = sub
{
    my $config = AliEn::Config->new();
     
    my $ldap = Net::LDAP->new( $config->{LDAPHOST}) 
                or return (0,"Error in banking service: Can't connect to ldap server $config->{LDAPHOST}");
	    
    $ldap->bind();
    
    my $base = $config->{LDAPDN};
     
    my $msg;
    my $entry;
    my $account;

	#get the bank account name using $ENV{SSL_CLIENT_I_DN} for finding user entry in LDAP
	$msg = $ldap->search (
                           base   => "ou=People, $base",
                           filter => "(&(objectclass=pkiUser)(subject=$ENV{SSL_CLIENT_I_DN}))" 
                         );

     # check if the user with the provided subject exists in LDAP
	 if ( !$msg->count) 
     {
        $ldap->unbind();
        return (0, "Can not find user entry in LDAP with certificate subject $ENV{SSL_CLIENT_I_DN}");
     }

	$entry = $msg->entry(0);
	$account = $entry->get_value('accountName');
    my $user;

    # check if the bank account is defined for the user 
    if (! $account)
    {
        $ldap->unbind();
        $user = $entry->get_value ('uid');
        return (0, "No bank account is defined in LDAP for user $user (cert subject $ENV{SSL_CLIENT_I_DN})");
    }

     # return account name 
	$ldap->unbind;
	return (1, $account);
};





#
# Checks if the user is allowed to execute '$command @ARGV'
my $_authenticateUser = sub 
{
    my $command = shift;
    my @ARGV = @_;
    my $argvStr = join (" ", @ARGV);

    my $isAdmin = $_authenticateAdmin->();
    ($isAdmin == 1) and return 1;

    my ($stat, $userAccount) = $_getUserBankAccount->();

    # check if account was found, return error message otherwise 
    $stat or return $userAccount;

    # if --fromAccount (account ID) is given
    $argvStr =~ /\.*--fromAccount\s+(\w+)\.*/;
    my $accountID = $1;
        
        # see if the ID corresponds to the ID provided in request
        my $allowedAccountId = $_getAccountIdFromAccountName->($userAccount);
 
        if ($accountID)
         {
            ( $accountID == $allowedAccountId) and return 1;
            return "User is not allowed to transfer funds from the account with ID = $accountID.\nUser has access to the account with ID = $allowedAccountId ";
         }
     
      
    # if -i is given (allocation ID )
    $argvStr =~ /\.*-i\s+(\w+)\.*/;
    my $allocationId = $1;

        # see if the user can use the given allocation
        my (@allowedAllocationId) = split ("\n", $_exec->(0, "glsalloc", ("-a", $allowedAccountId, "--show", "Id", "--quiet")));

        foreach my $id (@allowedAllocationId)
        {
            ($id == $allocationId) and return 1;
        }

    return "Invalid command syntax. Please specify either '--fromAccount <account_id>' or '-i <allocation_id>' ";
};

############################################################################
##                       Service Initialization function                  ##
############################################################################

sub initialize {
  $self     = shift;
  my $options =(shift or {});
  
  $DEBUG and $self->debug(1, "In initialize initializing service LBSG" );
  $self->{SERVICE}="LBSG";
  $self->{SERVICENAME}="LBSG";
  
   
  ## Taken from Manager/Job.pm, needed, since we are connecting to the same DB
  ## as the Job Manager

  my $name = "LBSG";
 
 ($self->{HOST}, $self->{PORT})=
            split (":", $self->{CONFIG}->{"${name}_ADDRESS"});


  $self->{LISTEN}=1;
  $self->{PREFORK}=5;
 
    
#  if ($ENV{'ALIEN_N_MANAGER_JOB_SERVER'}  ) {
#      $self->{PREFORK} = $ENV{'ALIEN_N_MANAGER_JOB_SERVER'};
#  }
     
    # initialize command hashes
    $_initCommands->();

  $_exec->(0, 'gmkproject', ('ALICE'));

  return $self;
}

############################################################################
##                             Public functions                           ##
############################################################################
sub bank  {
  shift;
  my $command = shift;
  my @ARGV = @_;

  if ( ! defined ($self->{COMMANDS}->{$command}) )
  {
       return "Error: Command '$command' is unknown.";  
  }
  elsif ( defined ( ${$self->{USER_COMMANDS}->{$command}}) )
  {

      # Check if the has the privilege to execute '$command @ARGV'
      my $isUser = $_authenticateUser->($command, @ARGV); 
      if ( $isUser != 1 )
      {
            $self->info ("Access denied for $ENV{SSL_CLIENT_S_DN} to execute $command @ARGV. Error is $isUser" );
            return "Error: User is not allowed to do $command @ARGV. Authentication error is: $isUser.";
      }


  } 
  elsif ( defined  ($self->{ADMIN_COMMANDS}->{$command}) )
  {

        # Check if the user has admin privileges
        my $isAdmin = $_authenticateAdmin->(); 
        if (  $isAdmin != 1)
        { 
            $self->info ("Access denied for $ENV{SSL_CLIENT_S_DN} to execute $command @ARGV. Error is $isAdmin" );
            return "You have to be bank administrator to execute $command. Authentication error is: $isAdmin\n";
        }

  }

	
  $self->info ("Doing '$command @ARGV' for ".$ENV{SSL_CLIENT_S_DN} );
	my $ret = $_exec->("0", $command, @ARGV);
		
	$self->info ("returning $ret");
	
	return $ret;
}

#
# checks the existence of the account and the user, creates account and user if necessary
sub checkUserAccount 
{
    shift;
    my $user = shift;
    my $account = shift;
    
    $self->info ("In 'checkUserAccount'. User is: \'$user\' account is: \'$account\'");
		
    my $isAdmin = $_authenticateAdmin->();
    if ( $isAdmin != 1) 
    {
            $self->info("Access denied for $ENV{SSL_CLIENT_S_DN} to execute 'checkUserAccount'. Error is $isAdmin");
            return "\nYou have to be bank administrator to execute 'checkUserAdmin'. Authentication error is: $isAdmin";

    }

    # check if user exists
    my $existsUser = $_exec->(0, 'glsuser', ('-u', $user,'--show', 'Name', '--quiet'));
    $existsUser or $_exec->(0, 'gmkuser', ($user));

    # check if account exists 
    my $accountId = $_exec->(0, 'glsaccount', ('-n', $account, '--show', 'Id', '--quiet'));
    if (!$accountId)
    {
        $_exec->(0, 'gmkaccount', ('-n', $account, '-u','NONE' ,'-m', 'ANY'));
        $accountId = $_getAccountIdFromAccountName->($account);
      	$self->info ("In 'checkUserAccount'. Account: \'$account\' did not exsit. Created $account with Id \'$accountId\'");
    }

		$accountId =~ /(\d+)/g;
		$accountId = $1;

    # check is the user belongs to the account 
    my @users = split (',', $_exec->(0, 'glsaccount', ('-n', $account, '--show', 'User', '--quiet')) );

   foreach my $belongingUser (@users)
   {
 				chomp $belongingUser;
	      $belongingUser =~ s/\s+//g;
				($belongingUser eq $user) and (return $accountId); 
	 } 


    # add user to the account and deposit some money (to create allocation)
		$self->info ("Adding \'$user\' to account: \'$accountId\'");
    $_exec->(0, 'gchaccount', ('-a' , $accountId, '--addUsers' , $user) );    

		$self->info ("Depositing funds on account \'$accountId\' to create allocation");
    $_exec->(0, 'gdeposit', ('-a', $accountId, '-z', '1'));

    return $accountId;
}

#
# checks the existence of the machine, creates the machine if necessary
sub checkMachineAccount
{
    shift;
    my $machine = shift;
    my $account = shift;

    $self->info ("In 'checkMachineAccount'. Machine is: $machine");

    my $isAdmin = $_authenticateAdmin->();
    if ( $isAdmin != 1) 
    {
            $self->info ("Access denied for $ENV{SSL_CLIENT_S_DN} to execute 'checkMachine'. Error is $isAdmin");
            return "\nYou have to be bank administrator to execute 'checkMachine'. Authentication error is: $isAdmin";

    }

    # check if machine exists 
    my $existsMachine = $_exec->(0, 'glsmachine', ('-m', $machine, '--show', 'Name', '--quiet')); 
    $existsMachine or $_exec->(0, 'gmkmachine', ($machine));

    # check if account exists 
    my $accountId = $_exec->(0, 'glsaccount', ('-n', $account, '--show', 'Id', '--quiet'));
    if (!$accountId)
    {
        $_exec->(0, 'gmkaccount', ('-n', $account, '-u','NONE' ,'-m', 'ANY'));
        $accountId = $_getAccountIdFromAccountName->($account);
        #cleanup accountId
        $accountId =~ s/\s*//g;
        # make deposit to create allocation 
        $_exec->(0,'gdeposit', ('-i', $accountId, '-z', '1'));
    }

    $accountId =~ s/\s*//g;

    return $accountId;
}

#
# checks the existence of the account, creates the account if necessary
sub checkAccount
{
		shift;
    my $account = shift;

    $self->info ("In 'checkAccount'. Tax account is: $account");

    my $isAdmin = $_authenticateAdmin->();
    if ( $isAdmin != 1) 
    {
            $self->info ("Access denied for $ENV{SSL_CLIENT_S_DN} to execute 'checkMachine'. Error is $isAdmin");
            return "\nYou have to be bank administrator to execute 'checkMachine'. Authentication error is: $isAdmin";

    }

   # check if account exists 
    my $accountId = $_exec->(0, 'glsaccount', ('-n', $account, '--show', 'Id', '--quiet'));
    if (!$accountId)
    {
        $_exec->(0, 'gmkaccount', ('-n', $account, '-u','NONE' ,'-m', 'ANY'));
        $accountId = $_getAccountIdFromAccountName->($account);
        #cleanup accountId
        $accountId =~ s/\s*//g;
        # make deposit to create allocation 
        $_exec->(0,'gdeposit', ('-i', $accountId, '-z', '1'));
    }

    $accountId =~ s/\s*//g;

    return $accountId;
}
 
1;


