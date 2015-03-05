package AliEn::Catalogue::Env;

sub loadEnvironment {
    my $self = shift;

#    $self->{DISPPATH} = "/";

#    ( $self->{DEBUG} > 2 )
#      and print "In ENVInterface: Loading the environment\n";

#    my $env = $self->{DATABASE}->getEnv($self->{DATABASE}->{ROLE});

#    ($env)
#      or print STDERR "Warning! not able to load the previous environment!\n"
#      and return;
#    my %env = split " ", $env;
#
#    ( $env{pwd} ) and $self->{DISPPATH} = $env{pwd};
    $self->{DISPPATH}=$self->GetHomeDirectory();
#    ( $self->{DEBUG} > 2 ) and print "In ENVInterface: path $env{pwd}\n";
    return 1;
}

sub saveEnvironment {
    my $self = shift;

#    ( $self->{DEBUG} > 2 )
#      and print
#	"DEBUG LEVEL 2\tIn ENVInterface: Saving the environment $self->{DATABASE_FIRST}->{USER}\n";

#    $self->{DATABASE_FIRST}->insertEnv($self->{DATABASE_FIRST}->{ROLE}, $self->{DISPPATH})
#		or return;

#    ( $self->{DEBUG} > 2 )
#      and print "DEBUG LEVEL 2\tIn ENVInterface: ENV saved!!\n";

	return 1;
}

return 1;
