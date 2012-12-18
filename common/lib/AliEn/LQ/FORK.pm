package AliEn::LQ::FORK;

@ISA = qw( AliEn::LQ );

use AliEn::LQ;

use strict;

sub initialize() {

	return 1;
}
sub getDefaultStatus {
	return '';
}

# return always the dummy status QUEUED
sub getStatus {
    return 'QUEUED';
}
sub getNumberRunning {
  return 0;
}
return 1

