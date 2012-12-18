#
#  The communications package for Authen-AliEnSASL
#
# Author Jan-Erik Revsbech
#
# It writes a status message and a buffer in a network safe manner.
#
#   The protocol is a follows:
#  
#  The buffer is Base64 encoded and a string consisting of status and 
# and buffer is created with :: as a delimiter (: is not in the base64 alfabet
# and hopefully not in the status!! (Should be checked though)
# 
# The resulting string is the converted to network byteorder, and the length of
# the buffer is stored in the first four bytes (In network order). 
# Everything is then sent to the socket.
#

package AliEn::Authen::Comm;
use strict;
require Storable;
use MIME::Base64;
use IO::Socket;
use IO::Select;

sub write_buffer {
    my $sock   = shift;
    my $status = shift;
    my $buffer = shift;
    my $bufLen = (shift || 0);

    my $data;

    if ( $bufLen != 0 ) {
        $data = $status . "::" . encode_base64($buffer);
    }
    else {
        $data = $status;
    }

    #Make a reference for the data
    my $ref = \$data;

    # Freeze it into network byte order
    my $encodedMsg = Storable::nfreeze($ref);
    my ($encodedSize) = length($encodedMsg);

    # Now print everything to the socket and flush it
    if (   !$sock->print( pack( "N", $encodedSize ), $encodedMsg )
        || !$sock->flush() )
    {
        die "Error while writing socket: $!";
    }

    return 1;
}

sub read_buffer {
    my $sock = shift;

    my ( $encodedSize, $readSize, $blockSize );
    $readSize    = 4;
    $encodedSize = '';

    # Read the total package size from first four bytes of network
    while ( $readSize > 0 ) {
        my $result =
          $sock->read( $encodedSize, $readSize, length($encodedSize) );
        if ( !$result ) {
            return undef if defined($result);
            die "Error while reading socket: $!";
        }
        $readSize -= $result;
    }

    #Revert it to real size (integer)
    $encodedSize = unpack( "N", $encodedSize );
    $readSize    = $encodedSize;

    my $msg = '';
    my $rs  = $readSize;

    # Now read until end of file
    while ( $rs > 0 ) {
        my $result = $sock->read( $msg, $rs, length($msg) );
        if ( !$result ) {
            die "Unexpected EOF" if defined $result;
            die "Error while reading socket: $!";
        }
        $rs -= $result;
    }

    # Now return to machinee dependent byte order.
    my $uncompressed = Storable::thaw($msg);

    # Split into status and message (Remeber $uncomressed is now a reference!)
    # We have to dereference it.
    my ( $status, $buffer ) = split ( "::", $$uncompressed );
    my $val;
    if ($buffer) {
        $val = decode_base64($buffer);
    }
    else {
        $val = "";
    }
    my $len = length($val);

    return ( $status, $val, $len );
}
1;

