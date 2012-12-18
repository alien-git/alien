use strict;

use AliEn::Config;
$ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/alienmaster/AliEn/t";
eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`; 
includeTest("user_basic/021-se") or exit(-2);


startService("CE", {nolisten=>1}) or exit(-2);
