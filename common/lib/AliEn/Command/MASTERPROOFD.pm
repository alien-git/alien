#/**************************************************************************
# * Copyright(c) 2001-2002, ALICE Experiment at CERN, All rights reserved. *
# *                                                                        *
# * Author: The ALICE Off-line Project / AliEn Team                        *
# * Contributors are mentioned in the code where appropriate.              *
# *                                                                        *
# * Permission to use, copy, modify and distribute this software and its   *
# * documentation strictly for non-commercial purposes is hereby granted   *
# * without fee, provided that the above copyright notice appears in all   *
# * copies and that both the copyright notice and this permission notice   *
# * appear in the supporting documentation. The authors make no claims     *
# * about the suitability of this software for any purpose. It is          *
# * provided "as is" without express or implied warranty.                  *
# **************************************************************************/

package AliEn::Command::MASTERPROOFD;

select(STDERR);
$| = 1;    # make unbuffered
select(STDOUT);
$| = 1;    # make unbuffered

use AliEn::Command;
use AliEn::UI::Catalogue::LCM

@ISA = (AliEn::Command);

use strict;

use AliEn::UI::Catalogue::LCM;
use AliEn::Logger;

sub Initialize {
    my $self = shift;

    my $version = $self->{version};
    if (! $version)
      {
	$ENV{ALIEN_PACKAGES}=~ /ROOT::(\S*)/ and $version=$1;
	$version or $version=
	  $self->{CONFIG}->{PACKMAN}->GetLatestVersion("ROOT");
      }

    $version =~ s/^v//i;
    $self->{"require"}->{ROOT} = $version;

    $self->SUPER::Initialize() or print STDERR "NOP!" and return;

    $self->JobEnv or return;

    ######################################################################################

    return 1;
}

sub Help {
    print "Usage: PROOFD.pl";

    exit(1);
}

sub JobEnv {
    my $self = shift;

    return 1;
}

sub Execute {
    my $self   = shift;

    my $rootsys = $ENV{'ROOTSYS'};

    my @args = ( "proofd","-f",@_ ,"$rootsys");
    my $scalarargs = join " ",@args;
    
    print "Executing @args \n";

    if (((defined $self->{debug}) && ( $self->{debug} > 0 ))) {
        print "Doing the system call with @args...\n";
    }
#    my $rc;# = system(@args);
    exec $scalarargs;
#    $self->Clean;

#    return ($rc);
}

return 1;
