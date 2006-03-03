package AliEn::SE::Methods::rfio;

use AliEn::SE::Methods::Basic;

use strict;
use vars qw( @ISA );
@ISA = ("AliEn::SE::Methods::Basic");

use strict;

sub initialize {
    my $self = shift;
    $self->{SILENT}=1;

}

sub get {
    my $self = shift;
    ( $self->{DEBUG} > 3 )
      and print STDERR "DEBUG LEVEL 3\t\tIn RFIO get  with @_...\n";

    open SAVEOUT, ">&STDOUT";
    open SAVEOUT, ">&STDOUT";
    open STDOUT,  ">/dev/null";

    if ( $self->{SILENT} ) {
        open SAVEERR, ">&STDERR";
        open SAVEERR, ">&STDERR";
        open STDERR,  ">/dev/null";
    }

    my $fullName="$self->{PARSED}->{HOST}:$self->{PARSED}->{PATH}";
    ( $self->{DEBUG} > 3 )
      and print STDERR
	"DEBUG LEVEL 3\t\tIn RFIO Executing rfcp $fullName} $self->{LOCALFILE}";
    my $error =
      system( "rfcp", $fullName, $self->{LOCALFILE} );
    close STDOUT;
    open STDOUT, ">&SAVEOUT";
    if ( $self->{SILENT} ) {
      close STDERR;
      open STDERR, ">&SAVEERR";
    }
    
    if ($error) {
      ( $self->{SILENT} )
	or print STDERR
          "Error: not possible to copy file $fullName!!\n";
      return;
    }

    ( $self->{DEBUG} > 0 )
      and print STDERR
	"File $fullName in $self->{LOCALFILE}\n";
    return $self->{LOCALFILE};
}

sub getSize {
    return 1;
}
return 1;
