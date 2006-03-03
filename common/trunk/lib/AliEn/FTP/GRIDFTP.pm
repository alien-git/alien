###############################################################################
#                    This package implements the GRIDFTP 
###############################################################################
package AliEn::FTP::GRIDFTP;

use strict;
use vars qw(@ISA);
use AliEn::FTP;
@ISA = ( "AliEn::FTP" );

sub initialize {
  my $self   = shift;
  my $options = shift;

  $self->{DESTHOST} = $options->{HOST};
  return $self;
}


sub dirandfile {
    my $fullname = shift;
    $fullname =~ /(.*)\/(.*)/;
    my @retval = ( $1, $2 );
    return @retval;
}

sub put {
    my $self       = shift;
    my $localfile  = shift;
    my $remotefile = shift;
    my $options    = ( shift or "" );
    #my $command = "$options; put $localfile $remotefile";
    $remotefile =~ /^gsiftp:\/\// or $remotefile="gsiftp://$self->{DESTHOST}$remotefile";
    my $command = "file://$localfile $remotefile";
    return $self->transfer($command);
}

# This function is intended to form the command excluding the options

sub get {
  my $self       = shift;
  my $localfile  = shift;
  my $remotefile = shift;
  my $options    = ( shift or "" );

  $remotefile =~ /^gsiftp:\/\// or $remotefile="gsiftp://$self->{DESTHOST}$remotefile";
   # Command portion except options is being assigned in $command variable
  my $method="file";
  $localfile=~ /^castor/ and $method="gsiftp";

  my $command = "$remotefile ${method}://$self->{CONFIG}->{HOST}$localfile";
  #print "Making sure that the directory of $localfile exist...\n";
  my $dir=$localfile;
  $dir =~ s/\/[^\/]*$//;
  require AliEn::MSS::Castor;
  ( -d $dir) or AliEn::MSS::Castor::mkdir($self, $dir);

  return $self->transfer($command);
}
sub transfer {
  my $self = shift;
  my $command = shift;
  # Command is being executed

#  my $dir=$ENV{ALIEN_ROOT};
  $self->{LOGGER}->info("FTP", "LET'S USE /opt/globus/!!!");
  $ENV{LD_LIBRARY_PATH}.=":/opt/globus/lib";
  my $dir="/opt/globus/";

  $ENV{X509_CERT_DIR}="$ENV{ALIEN_ROOT}/etc/alien-certs/certificates/";
  system("env |grep X509");
  $self->{LOGGER}->info("FTD", "Transferring file with $dir/bin/globus-url-copy -p 5 -tcp-bs 5000 $command\n");
    my $error = system("$dir/bin/globus-url-copy -p 5 -tcp-bs 5000 $command");
  $ENV{LD_LIBRARY_PATH}=~ s/:\/opt\/globus\/lib$//;

   $error = $error / 256;
   $self->{LOGGER}->info("FTD", "File transfer using GRIDFTP with error: $error\n");
   return $error;
}
sub startListening {
  my $self=shift;
  my $error = system("$ENV{ALIEN_ROOT}/sbin/in.ftpd -S -p2811");
  
  if ( !($error) ) {
    $self->info("GRIDFTP started" );
  }
  else {
    my $err = $error / 256;
    $self->info("GRIDFTPD started with error (Maybe its alredy running?)" );
  }
  return 1;
}
return 1;

=head1 NAME

AliEn::FTP::GRIDFTP

=head1 DESCRIPTION

This module implements GRIDFTP in AliEn.GRIDFTP is a high-performance, secure, reliable data transfer protocol optimized for high-bandwidh wide-area network. The GRIDFTP protocol is based on FTP, the highly popular Internet file transfer protocol.

=head1 SYNOPSIS

get($file, $remotefile, $options)

put($file, $remotefile, $options)

transfer($command)

=head1 METHODS

=item C<get>

This function is called from doTransfer of AliEn::Service::FTD module when $direction is get. Inside the get the transfer function of AliEn::FTP::GRIDFTP is called and the file is transfered from the remote host to local host.

=item C<put>

This function is called from doTransfer of AliEn::Service::FTD module when $direction is put. Inside the put transfer function of AliEn::FTP::GRIDFTP is called and the file is transfered to the remote host from local host.

=item C<transfer>

The file is transferred by the help of globus client tool globus-url-copy.

