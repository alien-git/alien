 #####################################################
#  <the famous> Proof interactive Analysis Service  #
#  (C) Andreas-J. Peters @ CERN                     #
#  mailto: Andreas.Peters@cern.ch                   #
#####################################################

# to do : create session Id 0 in the session table
# to enable first startup

package AliEn::Service::Proof;

use AliEn::Database::TaskQueue;
use AliEn::Database::Proof;
use AliEn::UI::Catalogue;

use strict;

use AliEn::Service;
use AliEn::ProxyRouter;
use AliEn::ProofMaster;

use Socket;
use Carp;

use vars qw(@ISA);

@ISA=("AliEn::Service");

my $self = {};

#############################################################################
sub initialize {
    $self=shift;
    my $options={};

    print "Starting up....\n";

    #####################################################
    # this is the personal PROOF Database
    #####################################################

    print "Connecting the Proof database ...\n";
    $self->{DB} = new AliEn::Database::Proof(
        {
            DB     => $self->{CONFIG}->{PROOF_DATABASE},
            HOST   => $self->{CONFIG}->{PROOF_DB_HOST},
            DRIVER => $self->{CONFIG}->{PROOF_DRIVER},
            DEBUG  => $self->{DEBUG},
			ROLE   => "admin"
        }
    ) 
		or $self->{LOGGER}->error("Proof","Error creating Proof database module instance")
		and return;

    print "Connecting the Queue database ...\n";
    $self->{QUEUEDB} = new AliEn::Database::TaskQueue(
        {
            DB     => $self->{CONFIG}->{QUEUE_DATABASE},
            HOST   => $self->{CONFIG}->{QUEUE_DB_HOST},
            DRIVER => $self->{CONFIG}->{QUEUE_DRIVER},
            DEBUG  => $self->{DEBUG},
			ROLE   => "admin"
        }
    )
		or $self->{LOGGER}->error("Proof","Error creating TaskQueue database module instance")
		and return;

    my $stat;
    #$stat = $self->{DB}->validateUser("admin");
    #($stat) or print STDERR "Error connecting" and exit;

    #$stat = $self->{QUEUEDB}->validateUser("admin");
    #($stat) or print STDERR "Error connecting" and exit;

    print "Creating Proof Tables ...\n";

    #####################################################
    # create the session table
    #####################################################

    $self->{DB}->createSessionsTable;

    #####################################################
    # create the reserved proofd table
    #####################################################

    $self->{DB}->createReservedTable;

    $self->{PORT}=$self->{CONFIG}->{'PROOF_PORT'};
    $self->{HOST}=$self->{CONFIG}->{'PROOF_HOST'};
    $self->{MULTIPLEXERTIMEOUT} = $self->{CONFIG}->{'PROOF_TIMEOUT'};
    $self->{PROOFMASTERPORT} = $self->{CONFIG}->{'PROOF_MASTER_PORT'};

    $self->{SERVICE}="Proof";
    $self->{SERVICENAME}="Proof";
    $self->{LISTEN}=1;

    print "Creating the ProofMaster   ...\n";
    # run a PROOF master
    $self->{PROOFMASTER} = new AliEn::ProofMaster();
    $self->{PROOFMASTER}->init() or return;
    $self->{PROOFMASTER}->execute() or return;

    # we want to do sequential work ....
    $self->{PREFORK}=1;
    $self->{FORKCHECKPROCESS}=1;
    $self->{HOSTNAME} = $ENV{'ALIEN_HOSTNAME'}.".".$ENV{'ALIEN_DOMAIN'};
    chomp $self->{HOSTNAME};
    #####################################################
    # connect to the catalogue
    #####################################################

    $options->{role} = "admin";
    $self->{CATALOGUE} = AliEn::UI::Catalogue->new($options);
    ($self->{CATALOGUE} )
      or $self->{LOGGER}->error( "Proof", "Error creating userinterface" )
	and return;

	return $self;
}


sub KillMasterProxy {
  my $this              = shift;
  my $user              = shift;
  my $sessionId         = shift;

  my $result  = $self->{DB}->getFieldFromSessionsEx("muxPid","WHERE user='$user' and sessionId='$sessionId'");

  defined $result
    or $self->{LOGGER}->error( "Proof", "Error fetching muxPid for user $user and sessionId $sessionId") 
	and return 0;

  @$result 
    or $self->{LOGGER}->info( "Proof", "Session $sessionId for user $user doesn't exists")
	and return 1;

  $self->{LOGGER}->info("Proof","Killing the MUX <pid=$result->[0]> of session <$sessionId> from user <$user>");

  ### stop the MUX
  kill(1,$result->[0]);
  return 1;
}

sub SetUpMasterProxy {
  my $this              = shift;
  my $user              = shift;
  # we expect an array of hashes with
  # ->{'SITE'}   = Sitename
  # ->{'NPROOF'} = #of desired proofd's
  my $serializer        = shift;
  my @mssprochasharray;
  my $mssprochash;
  my $assignedsitelist="";
  my @muxarray;

  #####################################################
  #### convert the serializer string into a hasharray .
  #####################################################

  my @splitserializer = split ";",$serializer;

  foreach (@splitserializer) {
    my ($spsite,$spnb) = split "###", $_;
    printf "$spsite:$spnb\n";
    push @mssprochasharray, {MSS   => $spsite,
			      NPROOF => $spnb,};
  }

  printf "We were called here in SetUpMasterProxy\n";
  
  #####################################################
  # create a new session ID ...
  #####################################################
  
  my $sessionId = $self->{DB}->getLastSessionId
      or $self->{LOGGER}->error( "Proof", "Error doing fetching last session Id") and return 0;
  
  $sessionId++;
  
  my $nMux=0;
  
  print "Assigned Session ID <$sessionId>\n";
  
  $self->{DB}->insertIntoSessions({sessionId=>$sessionId})
      or $self->{LOGGER}->error( "Proof", "Error inserting new session");
  
  $self->{LOGGER}->info("Proof","SetUpMasterProxy: Request from $user assigned to session <$sessionId>");
  
  my $i;
  
  printf "|@mssprochasharray|";
  
  #####################################################
  # create a new Proof table for this session
  # with the requested proofd locations  under
  # <P$sessionId>
  #####################################################
  
  $self->{DB}->checkProofTable($sessionId)
      or $self->{LOGGER}->error( "Proof", "Error creating P$sessionId table");
  
  #####################################################
  # loop over all desired mss ...
  #####################################################
  
  foreach $mssprochash (@mssprochasharray) {
      print "Mssprochash $mssprochash->{'MSS'},$mssprochash->{'NPROOF'}\n";
      
      # extract only the organisation and the site as site name
      my ($sorg,$ssite,$sunit) = split '::', uc($mssprochash->{'MSS'});
      
      my $site   = "$sorg\:\:$ssite";
      my $nproof = $mssprochash->{'NPROOF'};
      
      
      print "Site: $site nproof: $nproof\n";
      if (! (defined $site && defined $nproof) ) {
	  print $self->{LOGGER}->error( "Proof", "Error: Illegal <mssprochasharray> in SetUpMasterProxy" );
	  return (-1,"Illegal for SetUpMasterProxy arguments site=|$site| and $nproof=|$nproof|");
      }
      
      #####################################################
      # add an entry to the P$sessionID table
      printf "Proof Session Id $sessionId, $site, $nproof"; 
      $self->{DB}->insertIntoProof($sessionId,{
	  site=>$site,
	  mss=>$mssprochash->{'MSS'},
	  nrequested=>$nproof,
	  nassigned=>'0'})
	  or $self->{LOGGER}->error( "Proof", "Error inserting data into P$sessionId") and return 0;
      #####################################################
      printf "Survied DB\n";
  }
  
  #####################################################
  # call the scheduling algorithm to assign a time slot for the session
  # and assign proofd's to the requested site's in the P$sessionId table
  
  my $validity   = $self->GetUserTimeSlot($user,$sessionId);
  
  if (!$validity) {
      $self->{LOGGER}->info("Proof","User Timeslot not available. Ciao!\n");
      return;
  }
  
  my $assigntime = time;
  print "I got $validity and $assigntime\n";
  #####################################################
  
  #####################################################
  my $randomnumber = rand;
  my $thistime     = time;
  my $ProofMasterConfFileName = "$ENV{'HOME'}/.alien/proofd/config/proof.$sessionId.conf.$thistime-$randomnumber";
  my $ProofMasterConfFileNameRelativ = ".alien/proofd/config/proof.$sessionId.conf.$thistime-$randomnumber";
  
  open (PMCF,">$ProofMasterConfFileName");
  # write this into a proof.conf file for this session
  print PMCF "########################################\n";
  print PMCF "# created by the AliEn Proof Service   #\n";
  print PMCF "#                                      #\n";
  printf PMCF "# Session-Id: %4d                     #\n", $sessionId;
  print PMCF "########################################\n\n\n";
  print PMCF "# ------------------------------------ #\n\n";
  print PMCF "# Master Host                          #\n";
  print PMCF "node localhost                          \n";
  print PMCF "# ------------------------------------ #\n\n";

  
  #   # loop again over all desired sites ...
  foreach $mssprochash (@mssprochasharray) {
      my $mss    = $mssprochash->{'MSS'};
      my $i = 0;
      my $nSiteMux=0;
      #####################################################
      # get the assigned number for each mss
      print "Getting assigned for session $sessionId and mss $mss\n";
      my $result  = $self->{DB}->getFieldsFromProofByMss($sessionId,$mss,"site,nassigned,muxhost,muxport");

      defined $result
	  or $self->{LOGGER}->error( "Proof", "Error fetching nassigned,muxhost,muxport from P$sessionId for mss $mss")
	  and return 0;
      
      @$result
	  or $self->{LOGGER}->info( "Proof", "There is no data in P$sessionId for mss $mss")
	  and next;
      my $nsite;
      print "Doint the site loop @$result\n";
      for $nsite ( @$result ) {
	  my $site    = $nsite->{site};
	  my $nproof  = $nsite->{nassigned};
	  my $muxhost = $nsite->{muxhost};
	  my $muxport = $nsite->{muxport};
	  print "$site $nproof $muxhost $muxport\n";
#	$assignedsitelist .= "$site##$nproof####";

	  if ($nproof == 0 ) {
	      next;
	  }

	  $assignedsitelist .= "$mss##$site###$nproof####";

	  # feed the MUX array with the assigned proofd locations
	  # push it to a list 
	  for $i ( 1 .. $nproof ) {
	      push @muxarray, {HOST => $muxhost,
			       PORT => $muxport};
	      print PMCF "# ------------------------------------ #\n";
		  print PMCF "# MUX $nMux                            \n";
	      print PMCF "node $muxhost \n";
	      print PMCF "slave $muxhost port=$muxport ce=$mss;$nSiteMux mss=$mss;$nSiteMux\n";
	      print PMCF "# ------------------------------------ #\n\n";
		  $nMux++;
	      $nSiteMux++;
	  }
	  
	  # log the reservation in the reserved table
	  $self->{DB}->insertIntoReserved(
					  {sessionID=>$sessionId,
					   site=>$site,
					   nassigned=>$nproof,
					   assigntime=>$assigntime,
					   validitytime=>$validity,
					   expired=>"0"}) or
					   $self->{LOGGER}->error( "Proof", "Error inserting into reserved");    
	  
      }
  }     
  
  close PMCF;
      
  #####################################################
  # if we cannot find any free proofd, we don't need
  # to start a master server ...
  if ($nMux==0) {
    return (-1,"Sorry, no resources left for your request! Try later again!\n");
  }


  #####################################################
  # find a free port for our master

  $self->{MULTIPLEXERPORTRANGE} = $self->{CONFIG}->{'PROOF_MUX_PORT_RANGE'};
  my ($lowportbound, $highportbound) = split "-",$self->{MULTIPLEXERPORTRANGE};
  
  if (! (defined $lowportbound && defined $highportbound) ) {
    return (-1,"Illegal configuration for proofMuxPortRange [format like: 10000-10050]");
  }

  my $port;
  my $proto = getprotobyname('tcp');
  my $found = 0;
  for $port ($lowportbound .. $highportbound) {
    # try to bind this port ....
    if ( (socket(Server, PF_INET, SOCK_STREAM, $proto) && (setsockopt(Server, SOL_SOCKET, SO_REUSEADDR, pack("l",1)) ) && (bind(Server, sockaddr_in($port, INADDR_ANY))))) {
      $self->{LOGGER}->info("Proof","SetUpMasterProxy: Selecting port $port for the MUX in session <$sessionId>");
      $found = $port;
      last;
    }
  }

  if (!$found) {
    $self->{LOGGER}->error("Proof","Could not find a free port in the range $self->{MULTIPLEXER}->{PORTRANGE}!");
    return (-1,"Could not find a free port in the range $self->{MULTIPLEXER}->{PORTRANGE}!");
  }
  
  $self->{MULTIPLEXERPORT} = $found;
  $self->{MULTIPLEXERHOST} = $self->{CONFIG}->{HOST};
  chomp $self->{MULTIPLEXERHOST};
 

  #####################################################
  # now we fork a Multiplexer for this session ID
  # with the corresponding setup ....
  
  $self->{MULTIPLEXERPID} = fork();
  
  (defined $self->{MULTIPLEXERPID}) or print STDERR "Error Forking the Multiplexer Process\n" and return (-1,"Error forking the Multiplexer Process for your Session");
  
  if ($self->{MULTIPLEXERPID} == 0) {
    #####################################################
    # this will become our Multiplexer Process ....
    
    $self->{MULTIPLEXER} = new AliEn::ProxyRouter;
    $self->{MULTIPLEXER} or print STDERR "Error: Cannot start Multiplexer!\n" and return;

    $self->{MULTIPLEXER}->{PORT}        = $self->{MULTIPLEXERPORT};
    $self->{MULTIPLEXER}->{TIMEOUT}     = $self->{MULTIPLEXERTIMEOUT};
    $self->{MULTIPLEXER}->{DEBUG}       = 0;
    
    #####################################################
    # now we push a forward to the PROOF master to the MUX
    
    # now we push our forward to the proof master to the MUX

    $self->{MULTIPLEXER}->AddForwardSocket($self->{HOST}, $self->{PROOFMASTERPORT});
#    my $muxaddr;
#    foreach $muxaddr ( @muxarray ) {
#      $self->{MULTIPLEXER}->AddForwardSocket($muxaddr->{'HOST'},$muxaddr->{'PORT'});
#      }

    $self->{MULTIPLEXER}->PrintForwardAdresses();

    #####################################################
    # the child starts the Multiplexer and stays inside 
    # forever, until no further forward exists ...

    $self->{MULTIPLEXER}->Multiplexer();

    #####################################################
    # we probably don't come here ;-)
    #####################################################
    exit(-1);
  }
  
  #####################################################
  # this is the father process, which will register the 
  # MUX process in the table <proof.sessions>
  #####################################################

  printf "The father does the DB update ...\n";

  $self->{DB}->updateSessions({sessionId=>$sessionId,muxPid=>$self->{MULTIPLEXERPID},user=>$user,muxPort=>$self->{MULTIPLEXERPORT},assigntime=>$assigntime,validitytime=>$validity,expired=>'0'},"sessionId='$sessionId'")
    or $self->{LOGGER}->error( "Proof", "Error updating sessions") and return 0;

  #####################################################
  # get all the setup information for the client    
  # like the login + passwd + url + config file
  #####################################################

  # we return $ProofMasterConfFileNameRelativ; for the config file
  
  # get the "secret" client passwd;
  open (SCPWD, "$ENV{'HOME'}/.alien/proofd/client.plain.pwd");
  my $ProofClientPassword = <SCPWD>;
  if (!$ProofClientPassword) {
    $self->{LOGGER}->error( "Proof", "Error reading the Proof Client password") and 
      return (-1,"I have a problem, reading your client password!Sorry!\n");
  }
  close (SCPWD);
  
  # get the client login;
  my $ProofClientLogin = $ENV{'USER'};
  
  # build the Master URL for the client through the private MUX

  my $ProofMasterUrl = "proof://$self->{MULTIPLEXERHOST}:$self->{MULTIPLEXERPORT}";

  return {'SESSIONID',$sessionId, 'NMUX', $nMux, 'MUXHOST',$self->{MULTIPLEXERHOST},'MUXPORT',$self->{MULTIPLEXERPORT},'LOGINUSER',$ProofClientLogin,'LOGINPWD',$ProofClientPassword,'CONFIGFILE',$ProofMasterConfFileNameRelativ,'MASTERURL',$ProofMasterUrl,'SITELIST',$assignedsitelist};
}

sub DESTROY {
    my $this = shift;
    $self->{DB} and $self->{DB}->destroy();
    $self->{QUEUEDB} and $self->{QUEUEDB}->destroy();
}



#####################################################
# assign the proofd sites in the P$sessionId table
# according to availability
#####################################################

sub GetUserTimeSlot {
    my $self      = shift;
    my $user      = shift;
    my $sessionId = shift;
    my $bookingtime     = shift;
    my $bookingduration = (shift or "300");

    #####################################################
    # select all sites ....
    #####################################################

    my $result  = $self->{DB}->getAllFromProof($sessionId,"site,mss,nrequested");

    defined $result
      or $self->{LOGGER}->error( "Proof", "Error fetching site,nrequested from table P$sessionId")
      and return 0;

    #####################################################
    # get all site TcpRouter from the IS 
    #####################################################
    
    my $response =
	SOAP::Lite->uri("AliEn/Service/IS")
	->proxy("http://$self->{CONFIG}->{IS_HOST}:$self->{CONFIG}->{IS_PORT}")
	->getAllServices("TcpRouter");
    
    if (! ($response) ) {
	printf STDERR "Error asking IS for TcpRouter Names ...\n";
	return 0;
    }

    $response = $response->result; 

    if ( (defined $response) && ( $response eq "-1")) {
	printf STDERR "Error, there are no CE Names returned from the IS!\n";
	return 0;
    }
    
    my @hosts;
    my @ports;
    my @names;
    
    @hosts = split ":",$response->{HOSTS};
    @ports = split ":",$response->{PORTS};
    @names = split "###",$response->{NAMES};

    #####################################################
    # loop over all sites
    #####################################################

    foreach (@$result) {
	my $cnt=-1;
	print "Looping over $_->{'site'} @hosts @names\n";  
      # find out, what can be assigend ..... 

	# number of proofd still to ask for
	my $nleft = $_->{nrequested};
	# loop over them
	my $newsite;
	my $fixednewsite;
	my $stopforeach = 0;
	foreach $newsite (@names) {
	    if (!$stopforeach) {
		$cnt++;
		print "Dealing with $newsite $names[$cnt] $cnt\n"; 
		if ($names[$cnt] =~/.*SUBSYS/) {
		    # match the correct sites:
		    my ($sorg,$ssite,$sunit,$subsys) = split '::', uc($names[$cnt]);
		    $fixednewsite = uc "$sorg\:\:$ssite\:\:$sunit";
		    if ( uc($_->{'site'}) ne "$sorg\:\:$ssite" ) {
			printf "Comparing  $_->{'site'} neq $sorg\:\:$ssite ... skip\n ";
			
		    } else {
			printf "Comparing  $_->{'site'} eq $sorg\:\:$ssite ... \n ";  
			
			# get as many as possible proofd assigned
			
			#####################################################
			# add an entry to the P$sessionID table
			$self->{DB}->insertIntoProof($sessionId,
						     {site=>"$fixednewsite",
						      mss=> $_->{'mss'},
						      muxhost=>$hosts[$cnt],
						      muxport=>$ports[$cnt],
						      nrequested=>'0',
						      nassigned=>'0'})
			    or $self->{LOGGER}->error( "Proof", "Error inserting data into P$sessionId") and return 0;
			#####################################################
			
			
			my $nassigned = $self->GetFreeSiteSlots($fixednewsite,$nleft);
			
			$self->{DB}->updateProof($sessionId,{nassigned=>$nassigned},"site='$fixednewsite'")
			    or $self->{LOGGER}->error( "Proof", "Error updating table P$sessionId");
			$nleft -= $nassigned;
			if ($nleft <= 0) {
			    $stopforeach = 1;
			}
		    }
		}
	    }
	}
    }

    return $bookingduration;
}

sub GetAvailableProofds() {
  my $self       = shift;
  my $site       = shift;
  #my $query = "select queueID from QUEUE where jdl like '%Command::PROOFD%/bin/proofd%' and status='IDLE' and site='$site'";
  #my @result  = $self->{QUEUEDB}->query($query);
  #my $navailable = $#result;
  #if ($navailable<=0) {
  #  return 0;
  #} else {
  #  return ($navailable+1);
  #}
  my $no = $self->{QUEUEDB}->getNumberAvailableProofs($site);
  
  defined $no
    or $self->{LOGGER}->error("Proof","Error fetching number of available proofs");

  return $no;
}

sub GetPrebookedProofds() {
  my $self       = shift;
  my $site       = shift;

  my $prebooked  = $self->{DB}->getNumberPrebookedProofs($site);

  defined $prebooked
    or $self->{LOGGER}->error("Proof","Error fetching number of prebooked proofs for site $site");

  return $prebooked;
}

sub GetProofSites() {
  my $self       = shift;
  
  my $sites      = $self->{QUEUEDB}->getProofSites();
  defined $sites
    or $self->{LOGGER}->error("Proof","Error fetching the sites wich run proofds");
  
  return $sites;
}

sub GetFreeSiteSlots() {
  my $self       = shift;
  my $site       = shift;

  my $nrequested = shift;

  my $navailable  = $self->GetAvailableProofds($site);
  my $nprebooked  = $self->GetPrebookedProofds($site);
  my $left = ($navailable - $nprebooked);

  $self->{LOGGER}->info("Proof","Slots: AVAIL $navailable PREBOOKED $nprebooked LEFT $left");

  my $assigned;
  if ( $left > $nrequested) {
    $assigned = $nrequested;
  } else {
    if ($left>=0) {
      $assigned = $left;
    } else {
      $assigned = 0;
    }
  }
  $self->{LOGGER}->info("Proof","Slots: Assigned $assigned SLOTS.");
  return $assigned;
}

#####################################################
# this is the session status query function
#####################################################

sub QueryStatus(){
  my $this              = shift;
  my $sessionId         = shift or return;
  
  $self->{LOGGER}->info("Proof", "QueryStatus for session $sessionId");

  ### query information from the sessions table ###
  my $result  = $self->{DB}->getFieldsFromSessionsEx("expired,user,muxPid,muxPort,assigntime,validitytime","WHERE sessionId='$sessionId'");

  defined $result
    or $self->debug(1,"Error fetching sessionId $sessionId from sessions")
	and return;
  my ($expired, $user, $muxPid, $muxPort, $assigntime, $validitytime) = (@$result[0]->{expired}, @$result[0]->{user}, @$result[0]->{muxPid}, @$result[0]->{muxPort}, @$result[0]->{assigntime}, @$result[0]->{validitytime});
  
  ### query information from the P$sessionId table ###
  $result = $self->{DB}->getAllFromProof("$sessionId");

  defined $result
    or $self->debug(1,"Error fetching all information form p$sessionId")
      and return;

  my $totalassigned  = 0;
  my $totalrequested = 0;
  my $status;

  foreach (@$result) {
    my ($site,$muxhost,$muxport,$nrequested,$nassigned) = ($_->{site},$_->{muxhost},$_->{muxport},$_->{nrequested},$_->{nassigned});
    $totalassigned  += $nassigned;
    $totalrequested += $nrequested;
  }

  # set the status in text format
  if ($expired == 0) {
    if ( ($muxPid) && (kill($muxPid,0)) ) {
      $status = "Active";
    } else {
      $status = "Waiting";
    }
  }
  
  if ($expired) {
    if ($expired==4) {
      $status = "Killed";
    } else {
      $status = "Expired";
    }
  }

  return {'SESSIONID',$sessionId, 'NMUX', $totalassigned, 'MUXHOST',$self->{HOSTNAME},'MUXPORT',$muxPort,'NREQUESTED',$totalrequested,'NASSIGNED',$totalassigned,'STATUS',$status, 'SCHEDULEDTIME',$assigntime,'VALIDITYTIME',$validitytime,'USER',$user};
}

#####################################################
# this is the session cancellation function
#####################################################

sub CancelSession(){
  my $this              = shift;
  my $user              = shift;
  my $sessionId         = shift or return;
  $self->{LOGGER}->info("Proof", "CancelSession for session $sessionId");

  my $expired = 4;

  # change the expired flag in the session tables
  $self->{DB}->updateSessions({expired=>$expired},"sessionId='$sessionId' and user='$user'")
    or  $self->{LOGGER}->error( "Proof", "Error updating status to '$expired' for session $sessionId in table sessions") and return;
  
  # change the expired flag in all reserved sites
  $self->{DB}->updateReserved({expired=>$expired},"sessionId='$sessionId'")
    or  $self->{LOGGER}->error( "Proof", "Error updating status to '$expired' for session $sessionId in table reserved") and return;

  return {'SESSIONID',$sessionId};
}

#####################################################
# this is the session listing function
#####################################################

sub ListSessions() {
  my $this             = shift;
  my $user             = shift;
  $self->{LOGGER}->info("Proof","ListSessions");
  my $result  = $self->{DB}->getFieldFromSessionsEx("sessionId","WHERE expired='0'");
  my $resultserializer="";
  defined $result
    or $self->debug(1,"Error fetching unexpired entries from sessions")
      and return;

  if ((scalar @$result ) ==0 ) {
    $resultserializer .= '#LINEBREAK#';
  }

  foreach (@$result) {
    # get the information for each session with QueryStatus
    my $sessioninfo = $self->QueryStatus($_);
    $resultserializer .= $sessioninfo->{SESSIONID} . '###'; 
    $resultserializer .= $sessioninfo->{NMUX} . '###';
    $resultserializer .= $sessioninfo->{NREQUESTED} . '###';
    $resultserializer .= $sessioninfo->{NASSIGNED} . '###';
    if ($user eq $sessioninfo->{USER}) {
      $resultserializer .= $sessioninfo->{MUXHOST} . '###';
      $resultserializer .= $sessioninfo->{MUXPORT} . '###';
    } else {
      $resultserializer .= "-------" . '###';
      $resultserializer .= "0" . '###';
    }
    $resultserializer .= $sessioninfo->{STATUS} . '###';
    $resultserializer .= $sessioninfo->{SCHEDULEDTIME} . '###';
    $resultserializer .= $sessioninfo->{VALIDITYTIME} . '###';
    $resultserializer .= $sessioninfo->{USER} . '###';
    $resultserializer .= '#LINEBREAK#';
  }
  return $resultserializer;
}

#####################################################
# this is the sites proofd listing function
#####################################################

sub ListDaemons() {
  my $this             = shift;
  $self->{LOGGER}->info("Proof","ListDaemon");
  my $result  = $self->{QUEUEDB}->getProofSites();
  my $resultserializer ="";
  defined $result
    or $self->debug(1,"Error fetching proof sites from the queue db")
      and return;

  # send an empty line, if they are no daemons
  if ((scalar @$result) == 0 ) {
    $resultserializer .= '#LINEBREAK#';
  }

  foreach (@$result) {
    ($_ eq "") and next;
     
    # get the busy and available proofds
    my $available = ($self->{QUEUEDB}->getNumberAvailableProofs($_) or '0');
    my $busy      = ($self->{QUEUEDB}->getNumberBusyProofs($_) or '0');
    my $total = int($available) + int($busy);
    $resultserializer .= "$_" . '###' . "$available" . '###' . "$busy" . '###'. "$total";
    $resultserializer .= '#LINEBREAK#';
  }
  return $resultserializer;
}

#####################################################
# this is the forked Session Checker
#####################################################

# it kicks out expired sessions and reserved hosts

sub checkWakesUp {
  my $self=shift;
  my $actime = time;
  $self->{LOGGER}->info("Proof", "==================================================");
  $self->{LOGGER}->info("Proof", "Session Checker woken up at $actime ...");

  my $result  = $self->{DB}->getFieldsFromSessionsEx("sessionId,muxPid,assigntime,validitytime","WHERE expired='0'");

  defined $result
    or $self->debug(1,"Error fetching unexpired entries from sessions")
	and return;

  my $expired;
  foreach (@$result) {
    my ($sessionId, $muxPid, $assigntime, $validitytime) = ($_->{sessionId},$_->{muxPid},$_->{assigntime},$_->{validitytime});
    my $lifetime = $actime-$assigntime;
    if ( ($lifetime) > $validitytime) {
      $expired = 2;
    }else {
      $expired = 0;
    }

    # check if the MUX did a timeout, because he was not asked in the TIMEOUT period ...
    if (!(kill (0,$muxPid))) {
      $expired = 1;
    }

    if ($expired == 2) {
      # remove the process 
      kill (15, $muxPid);
      if ((kill (0, $muxPid))) {
	print STDERR "Error: the MUX $muxPid did not disappear!\n";
      }
    }

    $self->{LOGGER}->info( "Proof", "Session <$sessionId>: PID $muxPid ASSIGNED $assigntime Validity $validitytime EXPIRED $expired LIFETIME $lifetime");
    
    # now bring the tables in consistency state ...
    # change the expired flag in the session tables
    $self->{DB}->updateSessions({expired=>$expired},"sessionId='$sessionId'")
    or  $self->{LOGGER}->error( "Proof", "Error updating status to '$expired' for session $sessionId in table sessions") and next;

    # change the expired flag in all reserved sites
    $self->{DB}->updateReserved({expired=>$expired},"sessionId='$sessionId'")
    or  $self->{LOGGER}->error( "Proof", "Error updating status to '$expired' for session $sessionId in table reserved");
  }

  # now we check again the reserved proofds, if they are expired ....
  $result = $self->{DB}->getFieldsFromReservedEx("sessionId,site, assigntime,validitytime","WHERE expired='0'");
  defined $result
    or $self->debug(1,"Error fetching expired entries from reserved")
      and return;

  foreach (@$result) {
    my ($sessionId,$site, $assigntime,$validitytime) = ($_->{sessionId},$_->{site},$_->{assigntime},$_->{validitytime});
    if ( $validitytime > 0) {
      if ( ($actime-$assigntime) > $validitytime) {
	$self->{LOGGER}->info( "Proof", "Session <$sessionId>: Site <$site>: reservation timed out");
	$self->{DB}->updateReserved({expired=>3},"sessionId='$sessionId' and site='$site'")
	or $self->{LOGGER}->error( "Proof", "Error updating status to '$expired' for reserved site $site in session $sessionId");
      }
    }
  }
  
 $result  = $self->{DB}->getFieldsFromSessionsEx("sessionId,muxPid,assigntime,validitytime","WHERE expired='0'");

  defined $result
    or $self->debug(1,"Error fetching unexpired entries from sessions")
      and return;
  
  foreach (@$result) {
    my ($sessionId, $muxPid, $assigntime, $validitytime) = ($_->{sessionId},$_->{muxPid},$_->{assigntime},$_->{validitytime});
    if ( $validitytime > 0) {
      if ( ($actime-$assigntime) > $validitytime) {
	$expired = 3;
	$self->{DB}->updateReserved({expired=>$expired},"sessionId='$sessionId'")
	  or  $self->{LOGGER}->error( "Proof", "Error updating status to '$expired' for session $sessionId in table reserved");
      }
    }
  } 
  
  $self->{LOGGER}->info("Proof", "==================================================");
  
  return;
}

return 1;
