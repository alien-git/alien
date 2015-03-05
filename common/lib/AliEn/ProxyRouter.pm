 ###################################################################
 # PACKAGE ProxyRouter V1.1 - 12.08.2003                           #
 ###################################################################
 # This is a multiplexing ProxyRouter class.                       #
 # It forwards incoming connections roundrobin to defined          #
 # forward hosts. If a forward connection can not be established,  #
 # it tries to find another one, which can be established, until   #
 # no forward connection is anymore defined.                       #
 ###################################################################
 # (C) A.J.Peters (CERN) - Andreas.Peters@cern.ch                  #
 ###################################################################

package AliEn::ProxyRouter;

use strict;
use POSIX;
use IO::Select;
use IO::Socket;
 use Net::hostent; 
 use AliEn::Config;

use vars qw($VERSION);

use POSIX ":sys_wait_h";
# $SIG{CHLD} = \&REAPER;                  # install *after* calling waitpid

sub REAPER {
    my $stiff;
    while (($stiff = waitpid(-1, &WNOHANG)) > 0) {
        # do something with $stiff if you want
    }
    printf "[$$] OK: catching terminating child\n";
    $SIG{CHLD} = \&REAPER;                  # install *after* calling waitpid
}

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self = {};

    bless ($self,$class);
    
    $self->{MASTERSOCKET} = 0;
    $self->{PORT}        = 9990;
    $self->{LISTEN}      = 100;
    $self->{REUSE}       = 1;
    $self->{DEBUG}       = 0;
    $self->{CONFIG}=new AliEn::Config;
    $self->{HOST}        = $self->{CONFIG}->{HOST};
    chomp $self->{HOST};
    print "Hostname is $self->{HOST}\n";
    $self->{TIMEOUT}     = 120;
    $self->init() or return;
    return $self;
}

sub init {
    print "Calling wrong init function!\n";
    $SIG{CHLD} = \&REAPER;
    return 1;
}

sub ShallDie {
  my $self = shift;
  my $gotsomething;

  if ( ($self->{TIMEOUT}) && (!$gotsomething) ) {
    return 1;
  } else {
    return 0;
  }
}

sub AddForwardSocket {
    
    my $self = shift;
    my $host = shift;
    my $port = shift;
    if ( ($host) && ($port) ) {
    push @{$self->{FWSOCKETS}},
      {
      HOST => $host,
      PORT => $port
      };
    return 1;
  }
    return;
}


sub Multiplexer {
    my $self = shift;
    my $handles;
    my $newsocket;
    my $socket;
    my @new_handles;
    my $buf;
    my @pending;

    $SIG{PIPE} = 'IGNORE';
    my $lhostname = $self->{HOST};
    $self->{MASTERSOCKET} = new IO::Socket::INET(
                LocalHost => $lhostname,				
                LocalPort => $self->{PORT},
				Proto     => 'tcp',
				Listen    => $self->{LISTEN},
				Reuse     => $self->{REUSE},
			    );

    die "Couldn't establish socket: $!" unless $self->{MASTERSOCKET};

    my $localhostinfo = gethostbyaddr($self->{MASTERSOCKET}->peeraddr);

    print "[$$] OK: Starting Multiplexer :\n";
    print "------------------------------------------------------------------------------\n";
    print "                          Host    = $self->{HOST}\n";
    print "                          Port    = $self->{PORT}\n";
    print "                          Listen  = $self->{LISTEN}\n";
    print "                          Reuse   = $self->{REUSE}\n";
    print "                          TimeOut = $self->{TIMEOUT}\n";
    print "------------------------------------------------------------------------------\n";
    
    $handles = new IO::Select();
    $handles->add($self->{MASTERSOCKET});
    
    # multiplexing loop
    print "[$$] OK: Entering Multiplexing Loop\n";
    my $gotsomething = 0;
    while (1) {
	# read from sockets .... do a regular time out to do some other things
	(@new_handles) = $handles->can_read($self->{TIMEOUT});
	if (!@new_handles) {
	  if ($self->ShallDie($gotsomething)) {
	    printf "[$$] OK: I am having my timeout after $self->{TIMEOUT} sec.\n";
	    exit(0);
	  }
	} else {
	  $gotsomething = 1;
	}
	    
	$self->{DEBUG} and print "[$$] OK: Selected Readeable sockets\n";
	# loop over the ready sockets
	foreach $socket (@new_handles) {
	    if ($socket == $self->{MASTERSOCKET}) {
		# add new incoming connections
		$self->{DEBUG} and print "[$$] OK: Accepting New Socket\n";
		$newsocket = $socket->accept();
		fcntl($newsocket,F_SETFL(),O_NONBLOCK);
		$handles->add($newsocket);
		my $hostinfo = gethostbyaddr($newsocket->peeraddr);
		my $host = $newsocket->peerhost;
		my $rhostport ;
		$rhostport = sprintf "%s:%s",$newsocket->peerhost,$newsocket->peerport;
		printf "[$$] OK: Connection from %s:%s\n", $hostinfo->name || $host,$newsocket->peerport;
		my $forwardestablished=0;
		my $fwhost;
		my $fwport;
		my $id;
		my $newforwardsocket;

		do {
		    # get a new socket for a forward
		    ($fwhost, $fwport, $id) = $self->GetNewForwardAddress();
		    # create a connection 
		    if (! (($fwhost) && ($fwport) ) ) {
		      print STDERR "[$$] ERROR: There is no more forward address, drop connection!\n";
		      $handles->remove($newsocket);
		      close $newsocket;
		      next;
		    }
		    
		    print "[$$] OK: Establishing Forward Connection to $fwhost $fwport\n";
		    my ($portstart,$portstop) = split "-",$fwport;

		    if (!$portstop) {
		      $portstop = $portstart;
		    }
		    
		    print "[$$] OK: Checking Portrange $portstart - $portstop\n";
		    my $checkport;
		    for $checkport ( $portstart .. $portstop ) {
              $newforwardsocket = IO::Socket::INET->new(
                Proto    => "tcp",
                PeerAddr => $fwhost,
                PeerPort => $checkport
              );
		      fcntl($newforwardsocket,F_SETFL(),O_NONBLOCK);
		      if ($newforwardsocket) {
			print "[$$] OK: Port $checkport accepted!\n";
			$fwport = $checkport;
			last;
		      } else {
			print "FT: Port $checkport rejected!\n";
			}
		    }
		    
		    
		    if (!$newforwardsocket) {
		      $handles->remove($newsocket);
		      print STDERR "[$$] ERROR: Removing new connection, cannot do the forward connection to $fwhost:$fwport\n";
		      $self->FaultyID($id);
		      # try with the next one
		    } else {
		      $forwardestablished=1;
		    }
		  } while (!$forwardestablished);
	      
		my $whostport = "$fwhost:$fwport";
		
		print "[$$] OK: Established Forward Connection to $fwhost $fwport\n";
        push @{$self->{MUX}},
          {
          rsocket         => $newsocket,
          rsockethostport => $rhostport,
          wsocket         => $newforwardsocket,
          wsockethostport => $whostport,
          ID              => $id
          };
		# push the socket to the list of selectable sockets;
		$handles->add($newforwardsocket);
		$self->DumpForwardSockets();

	    } else {
		# client socket with data available
		$self->{DEBUG} and print "[$$] OK: Reading Data\n";
		my $nread = sysread($socket,$buf,1024*32);
		$self->{DEBUG} and print "[$$] OK: I read $nread\n";
#		$buf = <$socket>;
		if (defined $nread) {
		    if ($nread !=0) {
#			print "$buf\n";
			# find forward socket
			my $wsocket = $self->FindForwardSocket($socket);

			if ($wsocket) {
			    my $nwrite = 0;
			    my $length = $nread;
			    do {
				my $retry = 0;
				my $lnwrite;
				while ($retry < 10000) {
				    $lnwrite = syswrite($wsocket,$buf,$length,$nwrite);
				    if (defined $lnwrite) {
				
					last;
				    }
				    print STDERR "[$$] ERROR: Socket write failed ... doing rewrite ...\n";
				    $retry++; 
				}

				if (! defined $lnwrite) {
				    print STDERR "[$$] ERROR: Socket write failed ... closing connection ...\n";
				    $self->RemoveForwardSocket($socket);
				    $self->DumpForwardSockets();
				    $handles->remove($wsocket);
				    $handles->remove($socket);
				    close($socket);
				    next;
				}
				
				if ($lnwrite >=0 ) { 
				    $nwrite += $lnwrite;
				}
				
				if ($lnwrite != $length) {
				    # we could not put the complete data
				    print STDERR "[$$] ERROR: Socket write incomplete ... $nwrite/$nread\n";
				    $length -= $lnwrite;
				}
			    } while ($nwrite != $nread);
#			    } while (0);
#			    if ($nwrite != $nread) {
#				# keep the buffer and try again later
#				push @pending, $wsocket;
#				$self->{'$wsocket'}->{buf}    = $buf;
#				$self->{'$wsocket'}->{nread}  = $nread;
#				$self->{'$wsocket'}->{nwrite} = $nwrite;
#				$self->{'$wsocket'}->{retry}  = 10;
#			    }
				
			}
		    } else {
			# client socket was closed
			print STDERR "[$$] OK: Client socket was closed\n";
			$handles->remove($socket);
			my $wsocket = $self->FindForwardSocket($socket);
			if ($wsocket) {
			    print STDERR "[$$] OK: Socket closed ... removing partner\n";
			    $handles->remove($wsocket);
			    close($wsocket);
			}
			$self->RemoveForwardSocket($socket);
			$self->DumpForwardSockets();
		    }
		} else {
		    $self->{DEBUG} and print STDERR "[$$] ERROR: Read failed !\n";
		    if ($! == EAGAIN()) {
			next;
		    } else {
			$self->RemoveForwardSocket($socket);
			$self->DumpForwardSockets();
			
			$handles->remove($socket);
			my $wsocket = $self->FindForwardSocket($socket);
			if ($wsocket) {
			    print STDERR "[$$] OK: Socket closed ... removing partner\n";			    
			    $handles->remove($wsocket);
			    
			    close($wsocket);
			}
		    }
		}
	    }
	}

#	foreach (@pending) {
#	    if $self->{'$_'}->{$
#	    my $nwrite = syswrite($self->{'$_'}->{buf},$self->{'$_'}->{$nread},$self->{'$_'}->{$nwrite});
#	    if (defined $nwrite) {
		
	    
    }
}    
    

sub PrintForwardAdresses {
  my $self = shift;
  my $fwaddr;
  print  "------------------------------------------------------------------------------\n";
  print  "- FW ADRESS LIST -------------------------------------------------------------\n";
  print  "------------------------------------------------------------------------------\n";

  foreach $fwaddr (@{$self->{FWSOCKETS}}) {
    printf "[$$] OK: FW-Addr: %s:%s\n",$fwaddr->{HOST}||'undef',$fwaddr->{PORT}||'undef';
  }
  printf "------------------------------------------------------------------------------\n";
}

sub GetNewForwardAddress {
    my $self=shift;
    my $id = rand;

    my $fwaddr = pop @{$self->{FWSOCKETS}};
    printf "[$$] OK: FW-Addr: %s:%s\n",$fwaddr->{HOST}||'undef',$fwaddr->{PORT}||'undef';
    if ( !($fwaddr->{HOST})) {
      print STDERR "[$$] ERROR: No forward ... forcing to die\n";
      die;
    }
    return ($fwaddr->{HOST},$fwaddr->{PORT},$id);
}


sub DumpForwardSockets {
    my $self=shift;
    my $dumpsocket;
    foreach $dumpsocket (@{$self->{MUX}}) {
	print "------------------------------------------------------------------------------\n";
	print " $dumpsocket->{'ID'}: $dumpsocket->{'rsockethostport'} <=> $dumpsocket->{'wsockethostport'} \n";
	print "------------------------------------------------------------------------------\n";
    }
}

sub RemoveID {
  my $self = shift;
  my $queueId = shift;
  return 1;
}

sub FaultyID {
  my $self = shift;
  my $queueId = shift;
  return 1;
}

sub RemoveForwardSocket {
    my $self   = shift;
    my $socket = shift;
    my $testsocket;
    my $cnt=0;
    $self->{DEBUG} and print "[$$] OK: RemoveFowardSocket\n";
    foreach $testsocket (@{$self->{MUX}}) {
	if ( ($testsocket->{'wsocket'} == $socket ) || ($testsocket->{'rsocket'} == $socket) ) {
	    splice(@{$self->{MUX}},$cnt,1);
	    $cnt--;
	}
	$cnt++;
    }
}

sub FindForwardSocket {
    my $self = shift;
    my $rsocket = shift;
    my $wsocket;
    foreach $wsocket (@{$self->{MUX}}) {
	if ( $wsocket->{'rsocket'} == $rsocket ) {
	    $self->{DEBUG} and print "-> Forward Proxy\n";
	    return $wsocket->{'wsocket'};
	}
	if ( $wsocket->{'wsocket'} == $rsocket ) {
	    $self->{DEBUG} and print "-> Backward proxy\n";
	    return $wsocket->{'rsocket'};
	}
    }
    return;
}

return 1;

# my $multiplexer = new ProxyRouter;
# $multiplexer->AddForwardSocket("pcepaip19","22");
# $multiplexer->AddForwardSocket("localhost","10000");
# $multiplexer->AddForwardSocket("pcepaip19","22");
# $multiplexer->AddForwardSocket("localhost","10000");
# $multiplexer->Multiplexer();
# exit (0);
