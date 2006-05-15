package Dummy;

use strict;
sub hello_world {
  print "I've been called!!\n";
  return "hello!!";
}
my $callback_called=0;

# If we require IO::Socket::SSL before doing the first soap call, 
# this test is going to fail. We have to make sure that there are no
# require or use of that module in any of our stuff...

use AliEn::SOAP;

use AliEn::Server::SOAP::Transport::HTTPS;
use Net::SSLeay qw(die_now die_if_ssl_error);

#$Net::SSLeay::trace = 4;
#$IO::Socket::SSL::DEBUG=2;

$ENV{HTTPS_CERT_FILE}="$ENV{HOME}/certs/host.cert.pem";
$ENV{HTTPS_KEY_FILE}="$ENV{HOME}/certs/host.key.pem";
my $CAdir="$ENV{ALIEN_ROOT}/etc/alien-certs/certificates";
my $port=9008;
my $i=0;

#$ENV{HTTPS_CERT_FILE}="/tmp/x509up_u3902.CERT";
#$ENV{HTTPS_KEY_FILE}="/tmp/x509up_u3902.KEY";

system("$ENV{ALIEN_ROOT}/bin/grid-proxy-destroy");
#$ENV{LD_LIBRARY_PATH}="/opt/globus/lib:$ENV{LD_LIBRRARY_PATH}";
#$ENV{PATH}="/opt/globus/bin:$ENV{PATH}";
system("which openssl");
system("openssl version");



while ($i<2){
  print "\n\n";
  $i or print "Connecting with the certificate\n";
  $i and print "Connecting with the proxy\n";
  my $pid=fork();
  defined $pid or print "Error doing the fork!!\n" and exit(-2);
  if (! $pid) {
    print "OK, let's put our authentication subroutine...\n";

   $IO::Socket::SSL::GLOBAL_CONTEXT_ARGS->{SSL_verify_callback}=\&alien_verify;


    print "Starting the Server\n";
    my $d= AliEn::Server::SOAP::Transport::HTTPS
      ->new({
	     'LocalAddr'=>'localhost',
	     'LocalPort'=>$port,
	     SSL_server      => 1,
	     SSL_use_cert    => 1,
	     SSL_verify_mode => 0x01 | 0x02 | 0x04,
	     SSL_key_file    => $ENV{HTTPS_KEY_FILE},
	     SSL_cert_file   => $ENV{HTTPS_CERT_FILE},
	     SSL_ca_path     =>  "$CAdir",
	     SSL_ca_file     => "$CAdir/ce48cc73.0",
	     'Transport' => 'HTTPS',
	     'Reuse'=>1,
	     Listen        => 5, 
	     #
	     #
	     #Variables needed by 
	     
	    }
	   )
	->dispatch_to("Dummy")
	  ->handle()or die("Couldn't start the server !");
    exit(0);
  }
  sleep(5);
#  $i and exit();
  
  my $soap=new AliEn::SOAP;
  my $r;
  
  
  $soap->Connect({uri=>"Dummy", address=> "https://localhost:$port",
		  name=>"TEST"});
#  delete $ENV{HTTPS_CA_FILE};
  $ENV{HTTPS_CA_FILE}="/tmp/x509up_u3902.CA";
#  $IO::Socket::SSL::GLOBAL_CONTEXT_ARGS->{SSL_ca_file}= $ENV{HTTPS_CA_FILE};

#$Net::SSLeay::trace = 4;

  system("env |grep HTTPS");
  $r=$soap->CallSOAP("TEST", "hello_world");
  kill 9, $pid;
  $r or print "Error doing the soap call!!!\n" and exit(-2);
  $i++;
  $ENV{X509_USER_CERT}=$ENV{HTTPS_CERT_FILE};
  $ENV{X509_USER_KEY}=$ENV{HTTPS_KEY_FILE};
  system("$ENV{ALIEN_ROOT}/bin/grid-proxy-init");
#exit();
  #let's give some time for the port to free..
#  $port++;
#  sleep(5);

}
print "YUHUUU\n";
exit;

sub alien_verify {
  my ($ok, $x509_store_ctx) = @_;
  print "**** AliEn verify called ($ok)\n";
  my $x = Net::SSLeay::X509_STORE_CTX_get_current_cert($x509_store_ctx);
  if ($x) {
    print "Certificate:\n";
    my $subject=Net::SSLeay::X509_NAME_oneline(
				       Net::SSLeay::X509_get_subject_name($x));
    print "  Subject Name: $subject \n";
    if ($subject =~ /\/CN=proxy/ ) {
      print "This is a proxy certificate...\n";
    }
    print "  Issuer Name:  "
      . Net::SSLeay::X509_NAME_oneline(
				       Net::SSLeay::X509_get_issuer_name($x))
	. "\n";
    return 1;
  }
  print "The client did not present a certificate!!!\n";
  return 0;
}
