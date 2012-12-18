select(STDERR);
$| = 1;    # make unbuffered
select(STDOUT);
$| = 1;    # make unbuffered
use strict;

use AliEn::Config;

use Crypt::OpenSSL::Random;
use Crypt::OpenSSL::RSA;

use AliEn::Authen::IIIkey;
use IO::Socket::SSL;
use AliEn::Config;
use Getopt::Long ();

my $ini = new AliEn::Config( { "NOPACKINSTALL", 1 } );

$ini or print STDERR "Error getting the configuration\n" and exit;
my $passwd;

my $options = { 'user' => "$ENV{ALIEN_USER}" };

( Getopt::Long::GetOptions( $options, "user=s", ) ) or exit;

my $username = $options->{user};

my $org = "\L$ini->{ORG_NAME}\E";
print "Changing/Creating Alien SSH-Keys for: $username\n";

print "In order to update SSH-keys, you must specify your CERN AFS-password\n";
print "Enter password:";
system("stty -echo");
chomp( $passwd = <STDIN> );
system("stty echo");
print("\n");

my $KEY       = "AliGatorMasterKey";
my $y         = new AliEn::Authen::IIIkey;
my $encpasswd = $y->crypt( $passwd, $KEY );

if ( !( -d "$ENV{HOME}/.alien/identities.$org" ) ) {
    print "Making directory .............................";
    mkdir "$ENV{HOME}/.alien/identities.$org", 0700;
    print "DONE\n";
}

print "Generating keys (2048 bit)....................";
Crypt::OpenSSL::RSA->import_random_seed();
my $rsa = Crypt::OpenSSL::RSA->generate_key( 2048 ); 
$rsa->use_pkcs1_oaep_padding();

print "DONE\n";
print "Writing private key to disc...................";
my $filenamePRIV = $ENV{HOME} . "/.alien/identities.$org/sshkey.$username";
if ( !open( PRIVKEY, ">$filenamePRIV" ) ) {
    print "FAILED\n";
    exit;
}

print PRIVKEY $rsa->get_private_key_string;
close PRIVKEY;
chmod 0600, $filenamePRIV;
print "OK\n";

print "Writing public key to disc....................";
my $filenamePUB =
  $ENV{HOME} . "/.alien/identities.$org/sshkey.$username.public";
if ( !open( PRIVKEY, ">$filenamePUB" ) ) {
    print "FAILED\n";
    exit;
}
print PRIVKEY $rsa->get_public_key_string;
close PRIVKEY;
chmod 0644, $filenamePUB;
print "OK\n";

print "Sending public key to authentication server...";
my $done;# = SOAP::Lite->uri('AliEn/Service/Authen')->proxy(
#)->insertKey( $username, $encpasswd, $rsa->get_public_key_string );

($done)
  or print "FAILED\n Could not connect to authentication server.\n"
  and exit;

if ( !$done->result() ) {
    my $error=$done->paramsout;
    print "FAILED\n";
    print "SSH Key not updated on server ($error)\n";
    exit;
}
print "OK\n";
if ( $username eq $ini->{CLUSTER_MONITOR_USER} ) {
    print "This is the production user\n";
    my @return = $done->paramsout;
    my $key    = $return[0];
    $passwd or $passwd = $KEY;
    my $decrypt = $y->decrypt( $key, $passwd );
    print "Writing the ssh key in the file...............";
    my $PRIVKEY;
    if ( !open(  $PRIVKEY, ">","$filenamePRIV" ) ) {
        print "FAILED\n";
        exit;
    }
    $decrypt =~ s/ /\n/g;
    $decrypt =~ s/BEGIN\nRSA\nPRIVATE\nKEY/BEGIN RSA PRIVATE KEY/;
    $decrypt =~ s/END\nRSA\nPRIVATE\nKEY/END RSA PRIVATE KEY/;
    print $PRIVKEY "$decrypt";
    close $PRIVKEY;
    chmod 0600, $filenamePRIV;
    my $public = $return[1];
    $decrypt = $y->decrypt( $public, $passwd );
    print "OK\nWriting the public key in the file............";
	my $PUBKEY;
    if ( !open( $PUBKEY, ">","$filenamePUB" ) ) {
        print "FAILED\n";
        exit;
    }
    $decrypt =~ s/ /\n/g;
    $decrypt =~ s/BEGIN\nRSA\nPUBLIC\nKEY/BEGIN RSA PUBLIC KEY/;
    $decrypt =~ s/END\nRSA\nPUBLIC\nKEY/END RSA PUBLIC KEY/;
    print $PUBKEY "$decrypt";
    close $PUBKEY;
    chmod 0644, $filenamePUB;
    print "OK\n";
}
print "DONE\n";
print "************** SSHKEYS UPDATED ***************\n";
print " Your private key is stored in:\n$filenamePRIV\n";
print " Your public key is stored in:\n$filenamePUB\n";

__END__

=head1 NAME

createKeys.pl - Script for Alien-system to create Alien SSH-identity keypairs

=head1 USAGE

createKeys [username]

=head1 DESCRIPTION

This script is used for createing SSH public/private keypairs. It take the user to create keys for as an  optional argument. If no user is specified, keys re created for the current user.

If user is aliprod, this script behaves a little different. Instead of createing an SSH public and private key, it asks the central authentication server to send you back the aliprod private key in a secure way (encrypted).

=head1 SEE ALSO

This script is part of the Alien authentication suite. 

L<Authen::Authen>, L<Authen::SSH> 

=cut


