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
package AliEn::Logger::Local;

no warnings 'deprecated';

sub new {
    my $proto = shift;
    my $class = ref $proto || $proto;

    my %params = @_;

    my $self;
    {
        no strict 'refs';
        $self = bless {}, $class;
    }
    if (!$self->{logagent}) {
      require Log::Dispatch::Output;
      push @ISA,  qw(Log::Dispatch::Output);
    }
    $self->_basic_init(%params);
    if ($params{modules}) {
      $self{modules}=$params{modules};
    }

    return $self;
}

sub log_message {
  my AliEn::Logger::Local $self = shift;
  my %params = @_;

  # Do something with message in $params{message}
  if (  ( $params{level} eq "warning" )
	|| ( $params{level} eq "error" )
	|| ( $params{level} eq "critical" )
	|| ( $params{level} eq "alert" ) ){
    print STDERR "$params{message}";
  } else {
    print STDOUT "$params{message}";
  }

}
1;

