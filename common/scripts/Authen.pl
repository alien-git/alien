#!/usr/bin/perl -w

use AliEn::Services::Authen;

use AliEn::Server::SOAP::Transport::HTTP;

use AliEn::Config;
use Getopt::Long ();

{
    $options = {
        "password" => "",
        "debug"    => 0,
        "token"    => "",
        "role"     => "admin"
    };
    (
        Getopt::Long::GetOptions(
            $options, "help", "password=s", "token=s", "debug=n"
        )
      )
      or exit;
}

my $ini = new AliEn::Config($options);
($ini) or print STDERR "Error: Initial configuration not found!!\n" and exit;

if ( !( $options->{password} ) ) {
    print "Please enter the password:\n";
    chomp( $options->{password} = <STDIN> );
}

my $host = $ini->{'AUTH_HOST'};
my $port = $ini->{'AUTH_PORT'};

#my $certfile = "/var/log/Alien/CERTS/authencert.pem";
#my $certkey  = "/var/log/Alien/CERTS/authenkey.pem";
#my $capath   = "/opt/alien/etc/alien-certs/certificates";
#my $cafile   = "$capath/c35c1972.0";

AliEn::Services::Authen->initialize($options)
  or print STDERR "Error initializing the daemon\n"
  and exit;

print "Starting on $host :: $port (as AliEn::Services) \n";

my $daemon = AliEn::Server::SOAP::Transport::HTTP->new(
    LocalAddr => "$host",
    LocalPort => $port,
    Prefork   => 5,
    #	    SSL_server => 1,
    #	    SSL_use_cert => 1,
    #	    SSL_verify_mode => 0x01,
    #	    SSL_key_file => $certkey,
    #	    SSL_cert_file => $certfile,
    #	    SSL_ca_path => $capath,
    #	    SSL_ca_file => $cafile
)->dispatch_and_handle("AliEn::Services::Authen")->handle;

