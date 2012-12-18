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

package AliEn::Command::RunAlgorithm;

select(STDERR);
$| = 1;    # make unbuffered
select(STDOUT);
$| = 1;    # make unbuffered

use AliEn::Command;
@ISA = (AliEn::Command);

use strict;

use AliEn::UI::Catalogue::LCM::Computer;
use AliEn::Logger;
use AliEn::Package;
sub Initialize {
  my $self = shift;


  $self->SUPER::Initialize() or print STDERR "NOP!" and return;

  print "This command installs and executes an algorithm\n";
  $self->{PACKAGE} or print "Error: no package specified\n" and return;
  print "Trying to install $self->{PACKAGE}\n";


  my $package={};
  $package->{path}="auto";
  ($package->{name}, $package->{version})=split ("::", $self->{PACKAGE});
  $self->{CONFIG}->{PACKINSTALL}=1;
  $package->{PACKMAN}=$self->{CONFIG}->{PACKMAN};
  my @list=();
  $package->{require}=\@list;
  my $p=AliEn::Package->new($package);
  $p or print "Error creating the package!!\n" and return;
  print "Package installed in $p->{path}!!\n";

  $self->{EXECUTABLE} or print "Error: missing the name of the executable\n" and return;
  $self->{EXECUTABLE}="$p->{path}/$self->{EXECUTABLE}";
  print "Executable $self->{EXECUTABLE}\n";
#  $self->{UI}=new AliEn::UI::Catalogue::LCM::Computer or return;
  
  return 1;
}

sub Help {
  printf "Usage: MergeJobs.pl [--help]\n";
  printf "                  [--round <name>][--run <#>]\n";
  printf
    "                  [--event <#>][--config <file>][--comment <string>]\n";
  printf "                  [--debug]\n";
  exit(1);
}

sub Execute {
  my $self = shift;
  print "Doing the execution\n";
  my @system=($self->{EXECUTABLE});
  if ($self->{ARGUMENTS}) {
    $self->{ARGUMENTS}=~ s/&&&/ /g;
    $self->{ARGUMENTS}=~ s/\\&/&/g;
    
    push @system,split (",", $self->{ARGUMENTS});
  }
  print "Calling @system\n";

  my $rc=system(@system);
  print "Got $rc\n";
  return 1;
}


return 1;

