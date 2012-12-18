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
package AliEn::Database::TXT::ClusterMonitor;

use strict;

use AliEn::Database::TXT;

use vars qw(@ISA);
@ISA = qw( AliEn::Database::TXT );

sub initialize {
    my $self = shift;

    $self->{DIRECTORY}= "$self->{CONFIG}->{LOG_DIR}/ClusterMonitor.db";

    $self->{TABLES}->{PROCESSES}="queueId INTEGER, port INTEGER, nodeName Char(20),started INTEGER, finished INTEGER, command Char(255), received INTEGER, status Char(20), queue Char(20) , runtime Char(20), runtimes INTEGER, cpu REAL , mem REAL , cputime INTEGER, rsize INTEGER , vsize INTEGER , ncpu INTEGER , cpufamily INTEGER , cpuspeed INTEGER , cost REAL , maxrsize INTEGER, maxvsize INTEGER";

    $self->{TABLES}->{MESSAGES}="ID int, Message varchar(100), MessageArgs varchar(100),Status char(15), Executed int";

    $self->{TABLES}->{EXCLUDEDHOSTS}="name varchar(100), excludetime INTEGER";
#    $self->{TABLES}->{JOBJDLS}="queueId INTEGER, jdl varchar(1000), workernode char(50)";

    return $self->SUPER::initialize();
}

1;

