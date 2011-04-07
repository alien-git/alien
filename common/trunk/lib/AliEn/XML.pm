package AliEn::XML;

use XML::Generator;
use XML::Parser;
use XML::Parser::EasyTree;
use XML::Simple;
use Data::Dumper;
use AliEn::Logger::LogObject;

use vars qw(@ISA);

push @ISA, 'AliEn::Logger::LogObject';

sub new {
  my ($this) = shift;
  my $class = ref($this) || $this;
  my $self = (shift or {});

  bless $self, $class;
  $self->SUPER::new() or return;

  eval { $self->{PARSER} = new XML::Parser(Style => 'EasyTree'); };

  if ($@) {
    $self->{LOGGER}->error("XML", "Error creating the parser $@");
    return;
  }

  $self->debug(1, "XML created!!");

  my $conformance = 'strict';
  defined $self->{conformance} and $conformance = $self->{conformance};

  #   $self->{GENERATOR}=XML::Generator->new(
  #					  namespace=>$self->{namespace},
  #					  escape => 'always',
  #					  pretty => 2,
  #					  conformance => $conformance
  #					 );
  #    $self->{GENERATOR} or $self->{LOGGER}->error("XML", "Error getting the generator") and return;
  return $self;

}

sub parse {
  my $self = shift;
  my $xml  = shift;

  $xml or $self->{LOGGER}->info("XML", "Not enough arguments in parse. Missing xml") and return;
  my $tree;
  eval { $tree = $self->{PARSER}->parse($xml) };

  if ($@) {
    $self->{LOGGER}->info("XML", "Error doing the parse\n $@");
    return;
  }
  return $tree;
}

sub parse2 {
  my $self = shift;
  my $xml  = shift;

  $xml or $self->{LOGGER}->info("XML", "Not enough arguments in parse. Missing xml") and return;
  my $tree;
  eval { $tree = XML::Simple::XMLin($xml) };

  if ($@) {
    $self->{LOGGER}->info("XML", "Error doing the parse\n $@");
    return;
  }

  return $tree;
}

sub getSubEntryList {
  my $self    = shift;
  my $listRef = shift;

  my @list;

  if (ref($listRef) eq "ARRAY") {
    @list = @$listRef;
  } elsif ($listRef) {
    push @list, $listRef;
  }

  return @list;
}

#getXMLElement
# Input: name of the element to look for, and a list of elements
# Output: elements with that name
# It expects the list in the XML::Parser::EasyTree format
sub getXMLElement {
  my $self = shift;
  my $name = shift;
  $self->debug(1, "Looking for a $name");
  my $opt = "";
  if ($_[0] eq "-s") {
    $self->debug(1, "We only want the list of strings");
    $opt = shift;
  }

  my @list = @_;

  UNIVERSAL::isa($list[0], "ARRAY")
    and $self->debug(1, "Got a list")
    and return $self->getXMLElement($name, $opt, @{$list[0]});

  my @return = ();
  foreach my $element (@list) {

    if ($element->{name}) {
      $self->debug(1, "Checking '$element->{name}' and '$name'");

      if ($element->{name} =~ /^(.*\:)?$name$/) {
        $self->debug(1, "Found a '$name'");
        if ($opt =~ /s/) {
          my @list = ();
          $element->{content} and @list = @{$element->{content}};
          if ($list[0]) {
            $element = $list[0]->{content};
          } else {
            $element = "";
          }
        }

        @return = (@return, $element);
      }
    }

    (UNIVERSAL::isa($element->{content}, "ARRAY"))
      and @return = (@return, $self->getXMLElement($name, $opt, @{$element->{content}}));
  }
  return @return;
}

sub getXMLFirstValue {
  my $self = shift;
  my $name = shift;

  $self->debug(1, "Getting the first $name");
  my @list = $self->getXMLElement($name, "-s", @_);
  @list or return "";
  while (@list) {
    my $v = shift @list;
    if (defined $v) {
      $self->debug(1, "First value of $name is $v");
      return $v;
    }
  }
  return "";
}

sub printList {
  my $self   = shift;
  my $offset = shift;
  my @list   = @_;
  my $i      = 0;
  foreach my $element (@list) {
    print "\n$offset Element $i\n";
    $self->printHash($offset, $element);
    $i++;

  }
  return 1;

}

sub printHash {
  my $self    = shift;
  my $offset  = shift;
  my $element = shift;
  foreach my $key (keys %{$element}) {

    print "$offset $key  =>  $element->{$key}\n";
    (UNIVERSAL::isa($element->{$key}, "ARRAY"))
      and $self->printList("$offset\t", @{$element->{$key}});
    (UNIVERSAL::isa($element->{$key}, "HASH"))
      and $self->printHash("$offset\t", $element->{$key});

  }
}

#sub generate {
#  my $self=shift;
#  $self->{GENERATOR}->@_;
#
#}
return 1;
