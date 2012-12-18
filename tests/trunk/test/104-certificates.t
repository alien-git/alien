use strict;
use AliEn::X509;
print "Checking if we have host certificate\n";

my $error;
my $certDir="/root/certs";

for my $file ("host.cert", "host.key", "usercert", "userkey"){
  (-f "$certDir/$file.pem") or $error.="\tfile $file doesn't exist\n\t";
}
$error and print "Error: $error\n" and exit(-2);

my $x509=AliEn::X509->new() or exit(-2);;

$x509->load("$certDir/host.cert.pem") or exit(-2);

my $subject1=$x509->getSubject() or exit(-2);

$x509->load("$certDir/usercert.pem") or exit(-2);

my $subject2=$x509->getSubject() or exit(-2);

print "Got $subject1 and $subject2\n";

($subject1 eq $subject2) and print "Error: you need two different certificates in $certDir/host.cert.pem and $certDir/usercert.pem\n" and exit(-2);


print "ok!!\n";
