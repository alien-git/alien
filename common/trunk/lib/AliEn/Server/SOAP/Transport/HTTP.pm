package AliEn::Server::SOAP::Transport::HTTP;

use Log::TraceMessages qw(t d);
Log::TraceMessages::check_argv();

use SOAP::Transport::HTTP;

use base qw(SOAP::Transport::HTTP::Daemon);

# global variables

my %DEFAULTS = (
    'Prefork'   => 3,    # number of children to maintain
    'Listen'    => 20,   # number of clients each child should process
    'Timeout'   => 180,
    'Transport' => 'HTTP',
    'Handler'   => 'Server',
    'Reuse'     => 1,
    'Mode'      => 'PreFork',
    'LocalAddr' => '', 
    'LocalPort' => '',
    );

use strict;

sub new {
    my $self = shift;
    unless ( ref $self ) {
        my $class  = ref($self) || $self;
        my $args   = shift;
        my $mode   = $args->{Mode} || $DEFAULTS{Mode};
        my $module = "AliEn::Server::SOAP::".$mode;
        eval "require $module";
        eval "use base qw(SOAP::Transport::HTTP::Daemon $module)";

        my @params;  

        foreach my $key ( keys %DEFAULTS ) {
              push ( @params, $key, $args->{$key} || $DEFAULTS{$key} );
        }

        t d(@params);

        $self = $class->SUPER::new(@params)
          or die ("Couldn't establish listening socket for SOAP server");

        foreach my $key ( keys %DEFAULTS ) {
	    $self->{$key} =  $args->{$key} || $DEFAULTS{$key};
        }
       
    }
    return $self;
}

1;
