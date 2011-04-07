# PERL wrapper class for the C++ guidtool command
# AJP 03/2004
# ----------------------------------------------------------
# example:
# ----------------------------------------------------------
# use AliEn::GUID;
# my $guid = new AliEn::GUID;
# my $newguid = $guid->CreateGuid();
# print "Guid: $newguid ",$guid->GetBinGuid()," ",$guid->GetTxtGuid()," ",$guid->GetIP()," ",$guid->GetCHash()," ",$guid->GetHash(),"\n";
# ----------------------------------------------------------

package AliEn::GUID;
use AliEn::Logger;

use vars qw (@ISA $DEBUG);
push @ISA, 'AliEn::Logger::LogObject';

use strict;
use OSSP::uuid;

sub new {
  my $proto = shift;
  my $GUID  = (shift or "");
  my $self  = {};
  bless($self, (ref($proto) || $proto));
  $self->SUPER::new() or return;

  $self->{NAMESPACE} = $ENV{'ALIEN_HOSTNAME'};
  $self->{GENERATOR} = new OSSP::uuid;

  if ($GUID ne "") {
    $self->{TXT} = $GUID;
  }

  return $self;
}

sub CreateGuid {
  my $self = shift;
  $self->{GENERATOR}->make("v1");
  $self->{TXT} = $self->{GENERATOR}->export("str");
  return $self->{TXT};
}

#sub SetTxtGuid {
#    my $self = shift;
#    $self->{TXT} = (shift or "");
#    return;
#}

#sub GetTxtGuid {
#    my $self = shift;
#    return $self->{TXT};
#}

#sub GetBinGuid {
#    my $self = shift;
#    my $guid = (shift or $self->{TXT} or return);
#    open INPUT, "$self->{LDPATH};$ENV{'ALIEN_ROOT'}/bin/guidtool -p $guid |";
#    while (<INPUT>) {
#	$self->{BIN} = $_;
#	chomp $self->{BIN};
#	last;
#    }
#    close INPUT;
#    return $self->{BIN};
#}

#sub GetIP {
#    my $self = shift;
#    my $guid = (shift or $self->{TXT} or return);
#    open INPUT, "$self->{LDPATH};$ENV{'ALIEN_ROOT'}/bin/guidtool -w $guid |";
#    while (<INPUT>) {
#	$self->{IP} = $_;
#	chomp $self->{IP};
#	last;
#    }
#    close INPUT;
#    return $self->{IP};
#}

sub GetCHash {
  my $self = shift;
  my $guid = shift;
  $guid =~ s/-//g;
  my $csum = 0;
  foreach my $char (split(//, $guid)) {
    my $num = hex $char;
    $csum += $num;
  }
  $csum &= 0xF;
  return $csum;
}

sub GetHash {
  my $self = shift;
  my $guid = shift;
  $guid =~ s/-//g;

  my ($c0, $c1) = (0, 0);
  foreach my $char (split(//, $guid)) {
    my $num = hex $char;
    $c0 += $num;
    $c1 += $c0;

  }
  $c0 &= 0xFF;
  $c1 &= 0xFF;
  my $x    = $c1 % 255;
  my $y    = ($c1 - $c0) % 255;
  my $hash = ($y << 8) + $x;

  return $hash;
}

#sub GetCHash {
#    my $self = shift;
#    my $guid = (shift or $self->{TXT} or return);
#    $self->CreateHashes($guid);
#    return $self->{CHASH};
#}

#sub GetHash {
#    my $self = shift;
#    my $guid = (shift or $self->{TXT} or return);
#    $self->CreateHashes($guid);
#    return $self->{HASH};
#}

#sub CreateHashes {
#    my $self = shift;
#    my $guid = (shift or $self->{TXT} or return);
#    open INPUT, "$self->{LDPATH};$ENV{'ALIEN_ROOT'}/bin/guidtool -H $guid |";
#    while (<INPUT>) {
#	$_=~/([0-9]*)\s([0-9]*)/;
#	$self->{CHASH} = $1;
#	$self->{HASH}  = $2;
#	chomp $self->{CHASH};
#	chomp $self->{HASH};
#	last;
#    }
#    close INPUT;
#    return 1;
#}

return 1;
