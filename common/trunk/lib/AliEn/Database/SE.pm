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
package AliEn::Database::SE;

use AliEn::Database;
use strict;

use vars qw(@ISA);

@ISA = ("AliEn::Database");

sub preConnect {
  my $self = shift;

  ($self->{HOST}, $self->{DRIVER}, $self->{DB}) = split(m{/}, $self->{CONFIG}->{SEMASTER_DATABASE});

  return 1;
}

sub initialize {
  my $self = shift;

  return $self->checkTable(
    "SE_VOLUMES",
    "volumeId",
    { volumeId   => "int(11) NOT NULL auto_increment PRIMARY KEY",
      seName     => "char(255) collate latin1_general_ci NOT NULL",
      volume     => "char(255) NOT NULL",
      mountpoint => "char(255)",
      usedspace  => "bigint",
      freespace  => "bigint",
      size       => "bigint",
      method     => "char(255)",
    },
    "volumeId",
    [ 'INDEX (volume)', 'INDEX(seName)' ]
  );
}

1;

