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
package AliEn::Database::TXT::SE;

use strict;
use AliEn::Database::TXT;

use vars qw(@ISA);
@ISA = qw( AliEn::Database::TXT );

sub initialize {
    my $self = shift;

    $self->{DIRECTORY}="$self->{CONFIG}->{LOG_DIR}/SE.db";

    $self->{TABLES}->{LOCALFILES}="pfn char(255),localCopy char(255),size int,transferid int";

    $self->{TABLES}->{FTPSERVERS}="port int,pid int,pfn char(255),time int,user char(255)";

    return $self->SUPER::initialize();
}

1;

