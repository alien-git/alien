package Dummy;

use strict;
sub hello_world {
  print "I've been called!!\n";
  return "hello!!";
}


# If we require IO::Socket::SSL before doing the first soap call, 
# this test is going to fail. We have to make sure that there are no
# require or use of that module in any of our stuff...

#use AliEn::SOAP;
#use AliEn::Service;
#use IO::Socket::SSL;

use SOAP::Lite;
use  SOAP::Transport::HTTPS;
#$IO::Socket::SSL::DEBUG=2;

$ENV{HTTPS_CERT_FILE}="$ENV{HOME}/certs/host.cert.pem";
$ENV{HTTPS_KEY_FILE}="$ENV{HOME}/certs/host.key.pem";
(-e $ENV{HTTPS_CERT_FILE})
  or $ENV{HTTPS_CERT_FILE} = "$ENV{HOME}/.alien/globus/usercert.pem";
(-e $ENV{HTTPS_KEY_FILE})
  or $ENV{HTTPS_KEY_FILE} = "$ENV{HOME}/.alien/globus/userkey.pem";
#$ENV{X509_USER_CERT}=$ENV{HTTPS_CERT_FILE};
#$ENV{X509_USER_KEY}=$ENV{HTTPS_KEY_FILE};

my $CAdir=$ENV{HTTPS_CERT_FILE};

(-f $ENV{HTTPS_KEY_FILE}) or print "The file $ENV{HTTPS_KEY_FILE} doesn't exist\n" and exit(-2);
(-f $ENV{HTTPS_KEY_FILE}) or print "The file $ENV{HTTPS_KEY_FILE} doesn't exist\n" and exit(-2);
my $CAfile=$ENV{HTTPS_CERT_FILE};
(-f $CAfile) or print "The file $CAfile doesn't exist\n" and exit(-2);



my $port=9008;
my $pid=fork();
defined $pid or print "Error doing the fork!!\n" and exit(-2);

if (! $pid) {
  print "Starting the Server\n";
  my $d= SOAP::Transport::HTTPS::Daemon
    ->new(
	  'LocalAddr'=>'localhost',
	  'LocalPort'=>$port,
	  SSL_server      => 1,
	  SSL_use_cert    => 1,
	  SSL_verify_mode => 0x01 | 0x02 | 0x04,
	  SSL_key_file    => $ENV{HTTPS_KEY_FILE},
	  SSL_cert_file   => $ENV{HTTPS_CERT_FILE},
	  SSL_ca_path     =>  $CAdir,
	  SSL_ca_file     => $CAfile,
	  'Transport' => 'HTTPS',
	  'Reuse'=>1,
	  Listen        => 5, 

)
      ->dispatch_to("Dummy")
	->handle() or print("Couldb't establish listening socket");
  exit(0);
}
sleep(5);

Connect() or kill 9, $pid and exit(-2);

Connect() or kill 9, $pid and exit(-2);

kill 9, $pid;
print "YUHUU!!\n";
exit(0);

sub Connect{
  my $d;
  print "Connecting to the server...";
  eval {
    $d=SOAP::Lite->uri("Dummy")
      ->proxy("https://localhost:$port")
	->hello_world()
	  ->result;
  };
  if ($@) {
    print $@;
    return;
  }
  ( $d) or return;
  print "ok\nGOT $d\n";
  return 1;
}
