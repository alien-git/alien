# ======================================================================
#
# Copyright (C) 2000-2001 Paul Kulchenko (paulclinger@yahoo.com)
# SOAP::Lite is free software; you can redistribute it
# and/or modify it under the same terms as Perl itself.
#
# $Id: HTTPS.pm 3993 2006-03-20 13:21:13Z psaiz $
#
# ======================================================================

package SOAP::Transport::HTTPS;

use strict;
use vars qw($VERSION);
#$VERSION = sprintf("%d.%s", map {s/_//g; $_} q$Name$ =~ /-(\d+)_([\d_]+)/);
$VERSION = "1.3";
use SOAP::Lite on_fault => sub { return; };

# ======================================================================

package SOAP::Transport::HTTPS::Server;

use SOAP::Transport::HTTP;

use vars qw(@ISA $COMPRESS);

@ISA = qw(SOAP::Transport::HTTP::Server);

sub product_tokens { join '/', 'SOAP::Lite', 'Perl', SOAP::Transport::HTTPS->VERSION }

# ======================================================================

package SOAP::Transport::HTTPS::Daemon;

use Carp ();
use vars qw($AUTOLOAD @ISA);

@ISA = qw(SOAP::Transport::HTTPS::Server);

sub DESTROY { SOAP::Trace::objects('()') }

sub new { require HTTPS::Daemon; 
  my $self = shift;

  unless (ref $self) {
    my $class = ref($self) || $self;

    my(@params, @methods);
    while (@_) { $class->can($_[0]) ? push(@methods, shift() => shift) : push(@params, shift) }
    $self = $class->SUPER::new;
    $self->{_daemon} = HTTPS::Daemon->new(@params) or Carp::croak "Can't create daemon: $!";
    $self->myuri(URI->new($self->url)->canonical->as_string);
    while (@methods) { my($method, $params) = splice(@methods,0,2);
      $self->$method(ref $params eq 'ARRAY' ? @$params : $params) 
    }
    SOAP::Trace::objects('()');
  }
  return $self;
}

sub AUTOLOAD {
  my $method = substr($AUTOLOAD, rindex($AUTOLOAD, '::') + 2);
  return if $method eq 'DESTROY';

  no strict 'refs';
  *$AUTOLOAD = sub { shift->{_daemon}->$method(@_) };
  goto &$AUTOLOAD;
}

sub handle {
  my $self = shift->new;
  while (my $c = $self->accept) {
    while (my $r = $c->get_request) {
      $self->request($r);
      $self->SUPER::handle;
      $c->send_response($self->response)
    }
    UNIVERSAL::isa($c, 'shutdown') ? $c->shutdown(2) : $c->close(); 
    undef $c;
  }
}

# ======================================================================

1;

