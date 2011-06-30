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
package AliEn::Database::LCM;

use strict;
use AliEn::Database::TXT;

use vars qw(@ISA);
@ISA = qw( AliEn::Database::TXT );

sub initialize {
  my $self = shift;
  $self->{HOST} = $ENV{'ALIEN_HOSTNAME'} . "." . $ENV{'ALIEN_DOMAIN'};
  chomp $self->{HOST};

  $self->{DIRECTORY} = "$self->{CONFIG}->{CACHE_DIR}/LCM.db";

  $self->{TABLES}->{LOCALGUID} = "guid char(50), localpfn char(200),size int(11), md5sum char(32)";

  return $self->SUPER::initialize();

}

sub insertEntry {
  my $self = shift;

  my $file = shift;
  my $guid = shift;
  $self->debug(2, "Adding the entry $file and $guid to the localcopies");
  my $size   = -s $file;
  my $md5sum = AliEn::MD5->new($file);
  return $self->insert(
    "LOCALGUID",
    { guid     => $guid,
      localpfn => $file,
      size     => $size,
      md5sum   => $md5sum
    }
  );

}

1;

