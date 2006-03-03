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
package AliEn::Database::SQLite::LVM;

use strict;
use DBI;

use AliEn::Config;
use AliEn::Logger;
use AliEn::Database::SQLite;
use AliEn::Database::Lvm;

use vars qw(@ISA);

@ISA=('AliEn::Database::SQLite', 'AliEn::Database::Lvm');


sub getDatabaseDSN{
  my $self=shift;
  $self->{LOGGER}->debug("SQLite", "Returning the dsn of a SQLite database");
  return "DBI:SQLite:dbname=$self->{DIRECTORY}/LVM.db";
}


1;

