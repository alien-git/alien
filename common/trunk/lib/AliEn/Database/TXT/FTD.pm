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
package AliEn::Database::TXT::FTD;

use strict;

use AliEn::Database::TXT;

use vars qw(@ISA);
@ISA = qw( AliEn::Database::TXT );

sub initialize {
    my $self = shift;

    $self->{DIRECTORY}= "$self->{CONFIG}->{LOG_DIR}/FileTransferDaemon.db";


    unlink("$self->{DIRECTORY}/CURRENTTRANSFERS");

    $self->{TABLES}->{CURRENTTRANSFERS}="CURRENT INTEGER";

    $self->{TABLES}->{FILETRANSFERSNEW}="ID INTEGER,filename varchar(200),sourceURL varchar(200), size INTEGER, inserted INTEGER, status varchar(20),  finaldestURL varchar(200), email varchar(200), started INTEGER, finished INTEGER, direction char(5),retrys INTEGER, message varchar(200)";

    return $self->SUPER::initialize();

}

1;

