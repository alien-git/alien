package AliEn::SE::Methods::virtual;

use AliEn::SE::Methods::Basic;

use strict;
use vars qw( @ISA );
@ISA = ("AliEn::SE::Methods::Basic");


use AliEn::CE;
sub initialize {
    my $self = shift;
}

sub get {
    my $self = shift;

    $self->{LOGGER}->info ("Methods/virtual", "In virtual copying $self->{PATH} to $self->{LOCALFILE}");


    my $arguments=$self->{VARS_ARGS};

    print "TENEMOS $self->{PATH} con $arguments\n";

    my $ce=AliEn::CE->new();
    $ce or $self->{LOGGER}->error("Methods/virtual", "Error getting the CE") and return;
    print "Submitting the jdl\n";
 
    $ce->submitCommand($self->{PATH},split (/ /, $arguments) )
      or  $self->{LOGGER}->error("Methods/virtual", "Error submitting the command") and return;

     $self->{LOGGER}->info ("Methods/virtual", "Virtual data is being generated...");


    $self->{LOGGER}->info ("Methods/virtual", "Now, I should update the entry in the database");
    return $self->{LOCALFILE};
}

sub getSize {
    my $self = shift;

    return 1;
}
return 1;

