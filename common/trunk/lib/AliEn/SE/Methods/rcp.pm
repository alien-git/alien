package AliEn::SE::Methods::rcp;

use AliEn::SE::Methods::Basic;

use strict;
use vars qw( @ISA );
@ISA = ("AliEn::SE::Methods::Basic");

use strict;

sub initialize {
    my $self = shift;
}

sub get {
    my $self = shift;

    my $error =
      system( "rcp", "$self->{HOST}:$self->{PATH}", "$self->{LOCALFILE}" );

    ($error)
      and print STDERR
      "Error: not possible to copy file $self->{HOST}:$self->{PATH}!!\n"
      and return;
    ( -f $self->{LOCALFILE} )
      or print STDERR "Error: file not copied!!\n"
      and return;

    ( $self->{DEBUG} > 0 )
      and print STDERR
      "File $self->{HOST}:$self->{PATH} copied in $self->{LOCALFILE}\n";
    return $self->{LOCALFILE};
}

return 1;
