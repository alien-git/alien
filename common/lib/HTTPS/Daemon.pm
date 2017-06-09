package HTTPS::Daemon;

use strict;

use vars qw($VERSION @ISA $PROTO $DEBUG);

$VERSION = sprintf("%d.%02d", q$Revision: 3993 $ =~ /(\d+)\.(\d+)/);

use IO::Socket qw(AF_INET INADDR_ANY inet_ntoa);

use IO::Socket::SSL;

use HTTPS::Daemon::ClientConn;

@ISA=qw(IO::Socket::SSL);

$PROTO = "HTTP/1.1";

sub new
{
    my($class, %args) = @_;
    $args{Proto}  ||= 'tcp';
    my $self = $class->SUPER::new(%args);
    IO::Socket::SSL::context_init(%args);
    return($self);
}

sub accept
{
    my $self = shift;
    my $pkg = shift || "HTTPS::Daemon::ClientConn";
    my ($sock, $peer) = $self->SUPER::accept($pkg);
    if ($sock) {
        ${*$sock}{'httpd_daemon'} = $self;
          my $subject_name = $sock->peer_certificate("subject");
          my $issuer_name =  $sock->peer_certificate("issuer");
          return wantarray ? ($sock, $peer) : $sock;
    } else {
        return;
    }
}

sub url
{
    my $self = shift;
    my $url = "https://";
    my $addr = $self->sockaddr;
    if ($addr eq INADDR_ANY) {
 	require Sys::Hostname;
 	$url .= lc Sys::Hostname::hostname();
    }
    else {
	$url .= gethostbyaddr($addr, AF_INET) || inet_ntoa($addr);
    }
    my $port = $self->sockport;
    $url .= ":$port" if $port != 80;
    $url .= "/";
    $url;
}


sub product_tokens
{
    "libwww-perl-daemon/$HTTPS::Daemon::VERSION";
}

1;
