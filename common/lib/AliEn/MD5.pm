package AliEn::MD5;

use Digest::MD5;
use AliEn::Logger::LogObject;
use vars qw (@ISA $DEBUG);
push @ISA, 'AliEn::Logger::LogObject';
$DEBUG=0;

use strict;

my $self;
sub new{
  my $proto=shift;
  if (!$self) {
    $self={};
    my $class = ref($proto) || $proto;
    bless($self, $class);
    $self->SUPER::new() or return;
  }
  my $pfn=shift;
  $DEBUG and $self->debug(2, "Calculating the md5sum of $pfn");
  $pfn=~ s{^file://[^/]*/}{/} and $DEBUG
    and $self->debug(3, "Got a pfn instead of a file");

  if ($pfn=~ s{^soap://([^/]*)/}{/} ) {
    $pfn =~ s{\?.*$}{};
    $DEBUG and $self->debug(3, "Got a soap pfn instead of a file");
  }

  open(FILE, "<$pfn") or $self->info("Error opening the file $pfn while calculating its md5") and return;
  binmode(FILE);
  my $md5sum=Digest::MD5->new->addfile(*FILE)->hexdigest();
  close FILE;
  $DEBUG and $self->debug(2, "RETURNING $md5sum");
  return $md5sum;
}
1
