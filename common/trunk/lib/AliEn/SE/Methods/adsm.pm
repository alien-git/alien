package AliEn::SE::Methods::adsm;

use AliEn::SE::Methods::Basic;

use strict;
use vars qw( @ISA );
@ISA = ("AliEn::SE::Methods::Basic");

#METHOD adsm. 
# THE PFN SHOULD LOOK LIKE:
#  adsm://lxb006.gsi.de/aliprod/alice/simulation/2002-06/v3.08.rev.03/00086/00026/phos.digits.root
#
# The first part of the path is the ARCHIVE (aliprod)
# The second part is the directory, and finally, the  File
#
# Everyhing has to be in lowercase
#
#



sub initialize {
    my $self = shift;

    $self->{ARCHIVEPATH} = $self->{PATH};

    $self->{ARCHIVEPATH} =~ s/^\/([^\/]*)// and $self->{ARCHIVE} = $1;


    #We have to remove the name of the file from the ARCHIVEPATH. 
    #The archive path has to be only the directory
    $self->{ARCHIVEPATH} =~ s/\/([^\/]*)$//   and $self->{FILE}    = $1;
 
    #If the local file is not specified, it will be automatically generated, 
    # something like /tmp/phos.digits.root.234234 (with a number after the name)
    #In case of adsm, we have to get rid of the last number (the name of the 
    #local copy has to be the same as the name in the archive)
    $self->{LOCALFILE}   =~ s/\.\d*$//;

    $self->{ARCHIVE} or print "WARNING!! the name of the archive is missing!!\n";

    return $self;
}

sub get {
    my $self = shift;
    ( $self->{DEBUG} > 2 )
      and print
"DEBUG LEVEL 2\t\tIn $self->{METHOD} getting file from $self->{ARCHIVE} ( $self->{ARCHIVEPATH} and  $self->{PATH})\n";

    my @cmd = (
        "adsmcli", "retrieve", "$self->{LOCALFILE}", $self->{ARCHIVE},
        $self->{ARCHIVEPATH}, "stage=no"
    );

    my $error = system(@cmd);

    if ($error) {
        my $mess = ( $_ or "" );
        print STDERR
"ERROR getting the file from the robot!\nTrying to do @cmd and got $mess\n";
        return;
    }
    return $self->{LOCALFILE};
}

sub getSize {
    my $self = shift;

    ( $self->{DEBUG} > 2 )
      and print
"DEBUG LEVEL 2\t\tIn $self->{METHOD} getting size of file $self->{PATH}\n";
    my @cmd = (
        "/u/aliprod/bin/adsmsizeof.bash",
        $self->{FILE}, $self->{ARCHIVE}, $self->{ARCHIVEPATH}
    );

    return ( system(@cmd) );
}

return 1;

