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

package AliEn::Command::SaveFile;

select(STDERR);
$| = 1;    # make unbuffered
select(STDOUT);
$| = 1;    # make unbuffered

use AliEn::Command;
@ISA = (AliEn::Command);

use strict;

use AliEn::UI::Catalogue::LCM;
use AliEn::Logger;

sub Initialize {
    my $self = shift;

    $self->SUPER::Initialize() or print STDERR "NOP!" and return;

    return 1;
}

sub Help {
    printf "Usage: AliRoot.pl [--help]\n";
    printf "                  [--round <name>][--run <#>]\n";
    printf
      "                  [--event <#>][--config <file>][--comment <string>]\n";
    printf "                  [--debug]\n";
    exit(1);
}

sub Execute {
    my $self = shift;

    print "\n\nPrinting something in a file";

    if ( !open( FILE, ">$self->{WORK_DIRECTORY}/message.out" ) ) {
        print STDERR
          "ERROR: opening file $self->{WORK_DIRECTORY}/message.out\n$!\n";
        return;
    }
    print FILE "This is a test\n";
    close FILE;

    if ( !open( FILE, ">$self->{WORK_DIRECTORY}/Message2.out" ) ) {
        print STDERR
          "ERROR: opening file $self->{WORK_DIRECTORY}/Message2.out\n$!\n";
        return;
    }
    print FILE "File with capital letters\n";
    close FILE;

    $self->{output}="message.out,Message2.out";
    $self->{LOGGER}->debugOn;
    $self->Save;

    return (0);
}


return 1;

