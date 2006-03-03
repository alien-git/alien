package AliEn::Server::SOAP::ForkAfterProcessing;


# Idea and implementation of Peter Fraenkel (Peter.Fraenkel@msdw.com)

use Log::TraceMessages qw(t d);
Log::TraceMessages::check_argv();

use strict;
use SOAP::Lite  on_fault => sub { return; };;



sub dispatch_and_handle {
    my $self   = shift;
    my $target = shift;

    t 'ForkAfterProcessing <dispatch_and_handle>: $target = ' . d($target);

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
  CLIENT:
  t 'ForkAfterProcessing <handle>: ' . d($module);
  eval { 
    while (my $c = $self->accept) {
      my $first = 1;
      while (my $r = $c->get_request) {
        $self->{my_socket}=$c;
        $SIG{'ALRM'} = sub {die ("alarm")};
        alarm($timeout);
        $self->request($r);
        $self->$module;
        if ($first && fork) { $first=0; $c->close; next CLIENT }
        $c->send_response($self->response)
      }
      $SIG{'ALRM'} = 'IGNORE';
      alarm(0);
      UNIVERSAL::isa($c, 'shutdown') ? $c->shutdown(2) : $c->close();
      undef $c;
      undef $self->{my_socket};
      t 'ForkAfterProcessing <hand;e>: alarm!';
    }
  };
  if ($@) {
      die unless $@ eq "alarm";   # propagate unexpected errors
      t 'PreFork <handle>: alarm!';
      die ("The connection timed out");
  }
}

1;
