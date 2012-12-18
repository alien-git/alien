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
package AliEn::Database::Util;
use AliEn::Database::TXT;
use strict;
use vars qw(@ISA);
@ISA = ("AliEn::Database::TXT");

sub initialize {
	my $self = shift;
	$self->{DIRECTORY} = "$self->{CONFIG}->{LOG_DIR}/Util.db";
	$self->{TABLES}->{CACHE} = "name varchar(40), 
                            value varchar(60),
                            timestamp int";
	return $self->SUPER::initialize();
}

sub setCache {
	my $self = shift;
	my $var  = shift;
	my $data = shift;
	$self->delete( "CACHE", "name='$var'" );
	return
		$self->insert( "CACHE",
									 { name => $var, value => $data, timestamp => time } );
}

sub returnCache {
	my $self      = shift;
	my $data      = shift;
	my $timestamp = time;
	$timestamp -= 3600;
	return $self->queryValue(
				 "select value from CACHE where name='$data' and timestamp>$timestamp");
}
##############################################################################
##############################################################################
1;
