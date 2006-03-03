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

package AliEn::Command::ROOT;

select(STDERR);
$| = 1;    # make unbuffered
select(STDOUT);
$| = 1;    # make unbuffered

use AliEn::Command;
@ISA = (AliEn::Command);

use strict;


sub Initialize {
    my $self = shift;

    my $version = $self->{version};
    if (! $version)
      {
	$ENV{ALIEN_PACKAGES}=~ /ROOT::(\S*)/ and $version=$1;
	$version or $version=
	  $self->{CONFIG}->{PACKMAN}->GetLatestVersion("ROOT");
      }

    my $item = shift;
    my $first = 1;
    my $count = 0;
    printf("Item is $item\n");
    while ( $item && ($item ne "") ) {
	$count++;
	printf("Item is $item\n");
	if ($item eq "--output" ) {
	    printf(" -> $item\n");
	    my $outputfile = shift;
	    printf(" -> $outputfile\n");
	    if ($first) {
		$self->{output} .= $outputfile;
		$first = 0;
	    } else {
		$self->{output} .= ',';
		$self->{output} .= $outputfile;
	    }
	    $item = shift;
	} else {
	    $item = shift;
	}
	if ($count > 100) {
	    last;
	}
    }
    print "The output is $self->{output}\n";
    $version =~ s/^v//i;
    $self->{"require"}->{ROOT} = $version;

    $self->SUPER::Initialize() or print STDERR "NOP!" and return;

    $self->JobEnv or return;
    return 1;
}

sub Help {
    printf "Usage: ROOT.pl 
               ";
    exit(1);
}

sub JobEnv {
    my $self = shift;

    return 1;
}

sub Execute {
    my $self = shift;

    print "\n\nExecuting ROOT\n";
    my @args = ( "root", "-q", "-b",@_);

    if ( $self->{debug} > 0 ) {
        print "Doing the system call with @args...\n";
    }
    my $rc = system(@args);

    $self->Save;
#    $self->Clean;

    return ($rc);
}

return 1;
