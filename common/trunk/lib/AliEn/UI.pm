package AliEn::UI;

use vars qw($catalog $logger);
my $SIGCount = 0;					# counts number of pressed CTRL C, to prevent blocking application if f_quit fails
$SIG{INT} = \&catch_zap;            # best strategy

sub catch_zap {
  my $signame = shift;
  print STDERR "Somebody sent me a SIG$signame. Arhgggg......\n";
  die if (++$SIGCount >= 3);
  $logger and $logger->debug("UI", "We got a signal to die $@");

  if ($catalog) {
    $catalog->f_quit();
    undef $catalog;
  }
  die;
}

1
