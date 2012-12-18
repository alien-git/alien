##++
##     IIIkey.pm v1.0
##     Last modified: March 9th, 2000
##
##     Copyright (c) 2000 by Trans-Euro I.T Ltd
##     All Rights Reserved
##
##     E-Mail: tigger@marketrends.net
##
##     Permission  to  use,  copy, and distribute is hereby granted,
##     providing that the above copyright notice and this permission
##     appear in all copies and in supporting documentation.
##--
package        AliEn::Authen::IIIkey;
require Exporter;
@ISA = qw(Exporter);

#@EXPORT    = qw(crypt decrypt new version);
#@EXPORT_OK = qw(crypt decrypt new version);

use strict;

sub new {
    my $object = {};
    bless $object;
    return $object;
}

sub version {
    return "1.00";
}

sub crypt {
    shift;
    my ($_s) = @_;
    shift;
    my ($_k1) = @_;
    my $_b   = "";
    my $_d   = 0;
    my $_k2  = "";
    my $_k3  = "";
    my @_k_2 = ();
    my @_t   = ();
    my $_e   = "";
    my $_r1  = "";
    my $_r2  = "";
    my $_r3  = "";

    $_b = ord( substr( $_k1, 0, 1 ) );
    $_k2 = $_b**7;
    @_k_2 = split ( /''/, $_k2 );
    $_k2 = join '', @_k_2;
    $_k3 = reverse($_k1);
    $_k3 =~ tr /[a-m][n-z]/[n-z][a-m]/;
    $_k3 =~ tr /[A-M][N-Z]/[N-Z][A-M]/;
    $_r1 = _xor( $_s,  "$_k1" );
    $_r2 = _xor( $_r1, "$_k2" );
    $_r3 = _xor( $_r2, "$_k3" );

    for ( $_d = 0 ; $_d < length($_r3) ; $_d++ ) {
        $_t[$_d] = sprintf( "%02x", ord( substr( $_r3, $_d, 1 ) ) );
    }

    $_e = join '', @_t;
    $_e =~ s/a/\./g;
    $_e =~ s/b/-/g;
    $_e =~ s/c/\+/g;
    $_e =~ s/d/\!/g;
    $_e =~ s/e/\=/g;
    $_e =~ s/f/\^/g;

    return reverse($_e);
}

sub decrypt {
    shift;
    my ($_s) = @_;
    shift;
    my ($_k1) = @_;
    my $_b   = "";
    my $_d   = "";
    my $_k2  = "";
    my $_k3  = "";
    my @_k_2 = ();
    my $_r1  = "";
    my $_r2  = "";
    my @_w1  = ();
    my $_w2  = "";
    my $_o   = "";

    $_s = reverse($_s);
    $_s =~ s/\./a/g;
    $_s =~ s/-/b/g;
    $_s =~ s/\+/c/g;
    $_s =~ s/\!/d/g;
    $_s =~ s/\=/e/g;
    $_s =~ s/\^/f/g;

    $_b = ord( substr( $_k1, 0, 1 ) );
    $_k2 = $_b**7;
    @_k_2 = split ( /''/, $_k2 );
    $_k2 = join '', @_k_2;
    $_k3 = reverse($_k1);
    $_k3 =~ tr /[a-m][n-z]/[n-z][a-m]/;
    $_k3 =~ tr /[A-M][N-Z]/[N-Z][A-M]/;

    for ( $_d = 0 ; $_d < length($_s) ; $_d = $_d + 2 ) {
        $_w1[$_d] = chr( hex( substr( $_s, $_d, 2 ) ) );
    }

    $_w2 = join '', @_w1;
    $_r1 = _xor( $_w2, "$_k3" );
    $_r2 = _xor( $_r1, "$_k2" );
    $_o  = _xor( $_r2, "$_k1" );

    return $_o;
}

sub _xor {
    my ($_P1) = @_;
    shift;
    my ($_K1) = @_;

    my @_p = ();
    my @_k = ();
    my @_e = ();
    my $_l = "";
    my $_i = 0;
    my $_r = "";

    while ( length($_K1) < length($_P1) ) { $_K1 = $_K1 . $_K1; }

    $_K1 = substr( $_K1, 0, length($_P1) );

    @_p = split ( //, $_P1 );
    @_k = split ( //, $_K1 );

    foreach $_l (@_p) {
        $_e[$_i] = chr( ord($_l) ^ ord( $_k[$_i] ) );
        $_i++;
    }

    $_r = join '', @_e;

    return $_r;
}

1;

__END__

=head1 NAME

	IIIkey - Perl module to encrypt a string against a key.

=head1 SYNOPSIS

	use IIIkey;

	$y =  new IIIkey;

	$s = "1111 2222 5454 7777";
	$t = $y->crypt($s,"A key");
	$u = $y->decrypt($t,"A key");

	print "The source string is  $s\n";
	print "The encrypted string  $t\n";
	print "The original string   $u\n";

	exit;

=head1 DESCRIPTION

	This module can be used to encrypt and decrypt
	character strings. Using an xor operation.
	As long as the same 'key' is used, the original
	string can always be derived from its encryption.
	The 'key' may be any length although keys longer
	than the string to be encrypted are truncated.
        The string is encrypted 3 times against 3 
        variations of the initial key.

=head1 COPYRIGHT INFORMATION


	Copyright (c) 2000 Marketrends Productions,
                           Trans-Euro I.T Ltd.

 	Permission to use, copy, and  distribute  is  hereby granted,
 	providing that the above copyright notice and this permission
 	appear in all copies and in supporting documentation.

=cut


