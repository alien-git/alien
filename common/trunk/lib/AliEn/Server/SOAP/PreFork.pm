package AliEn::Server::SOAP::PreFork;

use Log::TraceMessages qw(t d);
Log::TraceMessages::check_argv();

use SOAP::Lite  on_fault => sub { return; };;

use IO::Socket;
use Symbol;
use POSIX;

@ISA = qw(SOAP::Server);

my %children = ();     # keys are current child process IDs
my $children = 0;      # current number of children

use strict;

sub dispatch_and_handle {
    my $self   = shift;
    my $target = shift;

    t 'PreFork <dispatch_and_handle>: $target = ' . d($target);

   # Fork off our children.

    for ( 1 .. $self->{Prefork} ) {
        $self->make_new_child($target);
    }

    # Install signal handlers.
    $SIG{CHLD} = \&REAPER;
    $SIG{INT}  = \&HUNTSMAN;

    # And maintain the population.
    while (1) {
        sleep;    # wait for a signal (i.e., child's death)

        for ( my $i = $children ; $i < $self->{Prefork} ; $i++ ) {
            $self->make_new_child($target);    # top up the child pool
        }
    }
}

sub REAPER {                                   # takes care of dead children
    #print "$$ In REAPER...\n";

    for (keys %children) { # collect all of them!
      #print "$$ ** Trying to wait for $_...\n";
      if (waitpid($_, &WNOHANG) > 0) {
        $children--;
        delete $children{$_};
        #print "$$ ** Picked up $_.\n";
      }
    }
  
    $SIG{CHLD} = \&REAPER;
}

sub HUNTSMAN {                          # signal handler for SIGINT

    local ( $SIG{CHLD} ) = 'IGNORE';    # we're going to kill our children
    kill 'INT' => keys %children;
    exit;                               # clean up with dignity
}

sub make_new_child {
    my $self   = shift;
    my $target = shift;

    my $pid;
    my $sigset;

    # block signal for fork
    $sigset = POSIX::SigSet->new(SIGINT);
    sigprocmask( SIG_BLOCK, $sigset )
      or die "Can't block SIGINT for fork: $!\n";

    die "fork: $!" unless defined( $pid = fork );

    if ($pid) {

        # Parent records the child's birth and returns.
        sigprocmask( SIG_UNBLOCK, $sigset )
          or die "Can't unblock SIGINT for fork: $!\n";
        $children{$pid} = 1;
        $children++;
        return;
    }
    else {
        # Child can *not* return from this subroutine.
        $SIG{INT} = 'DEFAULT';    # make SIGINT kill us as it did before
                                  # unblock signals
        $SIG{CHLD} = 'DEFAULT';
        sigprocmask( SIG_UNBLOCK, $sigset )
          or die "Can't unblock SIGINT for fork: $!\n";

        # handle connections until we've reached $MAX_CLIENTS_PER_CHILD

        for ( my $i = 0 ; $i < $self->{Listen} ; $i++ ) {

            # do something with the connection

            $self->dispatch_to($target)
                 ->options({compress_threshold => 10000})
                 ->handle;
       }

        # tidy up gracefully and finish
        # this exit is VERY important, otherwise the child will become
        # a producer of more and more children, forking yourself into
        # process death.
        exit;
    }
}

sub handle {
    my $self = shift->new;

    my $timeout = $self->{Timeout}*3;
    my $module  = "SOAP::Transport::".$self->{Transport}."::".$self->{Handler}."::handle";

    t 'PreFork <handle>: ' . d($module);

    eval {
	while (my $c = $self->accept) {
            $self->{my_socket}=$c;
            $SIG{'ALRM'} = sub {die ("alarm")};
            alarm($timeout);
            while (my $r = $c->get_request) {
		$self->request($r);
		$self->$module;
		$c->send_response($self->response);
	    }
            $SIG{'ALRM'} = 'IGNORE'; alarm(0);
	    UNIVERSAL::isa($c, 'shutdown') ? $c->shutdown(2) : $c->close();
            undef $c;
            undef $self->{my_socket};
        }
    };
    if ($@) {
	die unless $@ eq "alarm";   # propagate unexpected errors
	t 'PreFork <handle>: alarm!';
	die ("The connection timed out");
    }
}


1;

