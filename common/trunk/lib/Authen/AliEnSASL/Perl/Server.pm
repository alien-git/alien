# This is the Perl implementation of Authen::AliEnSASL::Server
#
#

package Authen::AliEnSASL::Perl::Server;

use vars qw($VERSION @ISA );
use strict;
use Authen::AliEnSASL::Server;
use Authen::AliEnSASL::Perl::SASLCodes;
use Carp;

$VERSION = 0.1;
@ISA     = qw (Authen::AliEnSASL::Server Authen::AliEnSASL::Perl::SASLCodes);

my @mechs;

sub new {
    my $type  = shift;
    my $class = ref $type || $type;

    #Call superclass's constructor in order to define callbacks and other stuff.
    my $self = $class->SUPER::new(@_);

    # ******************  Initialize list of mechs. **********************
    # 
    # We search through all inlcude directorires in search for a directory
    # which could include SASL mechanisms.
    #
    #  If a directory is found, we look for all .pm files and 
    # tjeck if it can be loaded, and if it has the correct security
    # properties

    my $mechsclass = __PACKAGE__;
    my $mechsdir   = $mechsclass;
    my @unordered;
    $mechsdir =~ s/::/\//g;
    my $basedir;
    foreach $basedir (@INC) {

        #print "Tjecking in $basedir/$mechsdir\n";
        if ( !( opendir DIR, "$basedir/$mechsdir" ) ) {

            #print "Unabble to open mechs dir\n";
        }
        else {

            print "  Checking for mechs in $basedir/$mechsdir\n";
            my @files = grep { /\.pm$/ } readdir(DIR);
            my $class;

            my $temp;
            foreach $class (@files) {
#                $/ = ".pm";
#                chomp($class);
		$class =~ s/\.pm$//;

                $temp = $mechsclass . "::" . $class;
                print "     * Testing $temp ...";
                if (   ( eval "require $temp" )
                    && ( $temp->_seclevel() >= $self->{sec_level} ) )
                {
                    print "OK\n";
                    push ( @unordered, $class );
                }
                else {
                    print "NOK\n";
                }
            }
            closedir DIR;
        }
    }
    if ( !(@unordered) ) {
        print "Not able to find any suitable mechs\n";
        print "Try lowering the security demands in call to server_new\n";
        exit;
    }

    # Well now we have the list, we want to sort it by security level!!
    @mechs = sort order @unordered;
    return $self;
}

sub order {
    my $mechsdir   = "Authen/AliEnSASL/Perl/Server";
    my $mechsclass = $mechsdir;
    $mechsclass =~ s/\//::/g;
    my $tempa = $mechsclass . "::" . $a;
    my $tempb = $mechsclass . "::" . $b;
    my $asec  = $tempa->_seclevel;
    my $bsec  = $tempb->_seclevel;

    if ( $asec > $bsec ) {

        #print "Big\n";
        return -1;
    }
    else {

        #print "Less\n";
        return 1;
    }

}

sub start {
    my $self     = shift;
    my $mech     = shift;
    my $intok    = shift;
    my $intoklen = shift;

    print "No mech specified\n" and return if !($mech);

    #$self->{mechs} = ("PLAIN");
    my $mechclass = __PACKAGE__ . "::$mech";

    eval "require $mechclass"
      && ( $mechclass->_secflags(@SUPER::sec) == @SUPER::sec )
      or croak "No SASL mechanism found\n";

    $self->{mechClass} = "$mechclass"->new( $self->{callback} );
    $self->{mechClass}->start( $intok, $intoklen );
}

sub listmech {

    # This method lists all the mechs that the server supports.
    my $self      = shift;
    my $delimiter = shift
      || " ";    # The delimiter which is inserted bewteen all methods
    my $leftsep  = shift || "";    # The left separator in the return string
    my $rightsep = shift || "";    # The right separator

    my $retstring = $leftsep . join ( $delimiter, @mechs ) . $rightsep;
    return $retstring;

}

sub mechanism {
    my $self = shift;
    $self->{mechClass}->mechanis(@_);
}

sub step {
    my $self = shift;
    $self->{mechClass}->step(@_);
}

sub getUsername {
    my $self = shift;
    return $self->{mechClass}->{username};
}

sub getRole {
    my $self = shift;
    return $self->{mechClass}->{role};
}

sub getSecret {
    my $self = shift;
    return $self->{mechClass}->{secret};
}

sub encode {
    my $self = shift;
    $self->{mechClass}->encode(@_);
}

sub decode {
    my $self = shift;
    $self->{mechClass}->decode(@_);
}

sub blocksize {
    my $self = shift;
    $self->{mechClass}->blocksize(@_);
}
1;

