package AliEn::Server::SOAP::NoFork;

use Log::TraceMessages qw(t d);
Log::TraceMessages::check_argv();

use strict;
use vars qw(@ISA);
use SOAP::Lite  on_fault => sub { return; };


sub dispatch_and_handle {
    my $self   = shift;
    my $target = shift;

    t 'NoFork <dispatch_and_handle>: $target = ' . d($target);

    # establish SERVER socket, bind and listen.

    while (1) {
         $self->dispatch_to($target)
         ->options({compress_threshold => 10000})
         ->handle;
    }
}


sub handle {
    my $self = shift->new;

    my $timeout = $self->{Timeout}*3;
    my $module  = "SOAP::Transport::".$self->{Transport}."::".$self->{Handler}."::handle";

    t 'NoFork <handle>: ' . d($module);

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
        t 'NoFork <handle>: alarm!';
        die ("The connection timed out");
    }
}

1;
