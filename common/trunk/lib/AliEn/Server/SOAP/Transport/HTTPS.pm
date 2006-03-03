package AliEn::Server::SOAP::Transport::HTTPS;

use Log::TraceMessages qw(t d);
Log::TraceMessages::check_argv();

use SOAP::Transport::HTTPS;

#use IO::Socket::SSL;

use base qw(SOAP::Transport::HTTPS::Daemon);

# global variables

my $certDir= $ENV{X509_CERT_DIR} || "/etc/grid-security/certificates";

my %DEFAULTS = (
    'Prefork'   => 3,    # number of children to maintain
    'Listen'    => 20,   # number of clients each child should process
    'Timeout'   => 200,
    'Transport' => 'HTTPS',
    'Handler'   => 'Server',
    'Reuse'     => 1,
    'Mode'      => 'PreFork',
    'LocalAddr' => '', 
    'LocalPort' => '',
    #'SSL_cipher_list' => 'EXP-RC4-MD5',
    'SSL_server'      => 1,
    'SSL_server'      => 1,
    'SSL_use_cert'    => 1,
    'SSL_verify_mode' => 0x01,
    'SSL_key_file'    => 'key.pem',
    'SSL_cert_file'   => 'cert.pem',
    'SSL_ca_path'     => "$certDir/",
    'SSL_ca_file'     => "$certDir/ce48cc73.0",
    'SSL_client_cert' =>"",
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
    eval "use base qw(SOAP::Transport::HTTPS::Daemon $module)";
    
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
