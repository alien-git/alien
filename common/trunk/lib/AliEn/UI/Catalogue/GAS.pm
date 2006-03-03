package AliEn::UI::Catalogue::GAS;

use strict;

use AliEn::UI::Catalogue;

use vars qw(@ISA $DEBUG);

push @ISA, "AliEn::UI::Catalogue";

$DEBUG=0;

my %GAS_commands=("cd"=>['$self->cd',0]);
sub initialize {
  my $self    = shift;
  my $options = shift;
 $options->{NO_DATABASE}=1; 
  $self->SUPER::initialize($options) or return;
  print "initializing the gas\n";

print "VAMOS ALLA\n";

  $self->AddCommands(%GAS_commands);




}

sub cd {
print "Calling hte right cd";
}

return 1;
