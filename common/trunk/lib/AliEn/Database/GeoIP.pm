#/**************************************************************************
# * Copyright(c) 2001-2004, ALICE Experiment at CERN, All rights reserved. *
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

package AliEn::Database::GeoIP;

use AliEn::Database;
use AliEn::Config;

use strict;

use vars qw(@ISA);
@ISA=("AliEn::Database");

sub new {
  my $proto = shift;
  my $self  = (shift or {});

  my $class = ref($proto) || $proto;
  bless ($self, $class);

  $self->{CONFIG} = new AliEn::Config();
  $self->{CONFIG}
    or return;

  $self->{CONFIG}->{G_CONTAINER}->{GEO_I_P_DATABASE} =~ /^(.+)\/(\w+)\/(\w+)$/;
  $self->{HOST} = $1;
  $self->{DRIVER} = $2;
  $self->{DB} = $3;

  return $self->SUPER::new($self);
}

sub recreateTables {
  my $self = shift;

  if ($self->existsTable("GeoIP")) {
    $self->dropTable("GeoIP")
      or return;
  }

  return $self->createTable("GeoIP", "(startip CHAR(15), endip CHAR(15), startnumber BIGINT, endnumber BIGINT, country char(2), latitude DOUBLE, longitude DOUBLE, INDEX(startnumber, endnumber))");
}
