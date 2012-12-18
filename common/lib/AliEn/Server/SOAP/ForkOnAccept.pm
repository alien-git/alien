package AliEn::Server::SOAP::ForkOnAccept;

use Log::TraceMessages qw(t d);
Log::TraceMessages::check_argv();

use strict;

use SOAP::Lite on_fault => sub { return; };


sub dispatch_and_handle {
    my $self   = shift;
    my $target = shift;

    t 'ForkOnAccept <dispatch_and_handle>: $target = ' . d($target);

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

  t 'PreFork <handle>: ' . d($module);

  CLIENT:
  eval {
      while (my $c = $self->accept) {
      my $pid = fork();

      # We are going to close the new connection on one of two conditions
      #  1. The fork failed ($pid is undefined)
      #  2. We are the parent ($pid != 0)
      unless( defined $pid && $pid == 0 ) {
        UNIVERSAL::isa($c, 'shutdown') ? $c->shutdown(2) : $c->close();
        next;
      }
      # From this point on, we are the child.

      $self->close;  # Close the listening socket (always done in children)

      # Handle requests as they come in
      while (my $r = $c->get_request) {
        $self->{my_socket}=$c;
        $SIG{'ALRM'} = sub {die ("alarm")};
        alarm($timeout);
        $self->request($r);
        $self->$module;
        $c->send_response($self->response);
      }
      $SIG{'ALRM'} = 'IGNORE';
      alarm(0);
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
