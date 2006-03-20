package HTTPS::Daemon::ClientConn;

use vars qw(@ISA $DEBUG);

use IO::Socket ();
use IO::Socket::SSL;
use HTTP::Daemon;

@ISA=qw(IO::Socket::SSL HTTP::Daemon::ClientConn);

1;
