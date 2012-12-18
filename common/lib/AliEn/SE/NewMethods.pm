package AliEn::SE::NewMethods;

use strict;

use vars qw(@ISA);

use AliEn::Config;
use AliEn::Logger;

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = ( shift or {} );
    ( UNIVERSAL::isa( $self, "HASH" ))
	or $self={"PFN", $self};
    bless( $self, $class );

    $self->{PFN} or print STDERR "Error: no file specified\n" and return;

    $self->{DEBUG} or $self->{DEBUG} = 0;
    $self->{CONFIG} = new AliEn::Config();
    ( $self->{CONFIG} )
      or print STDERR "Error getting the configuration\n"
      and return;

    $self->{LOGGER}= new AliEn::Logger;

    $self->parsePFN();


    my $mss="AliEn::MSS::$self->{METHOD}";
    my @possibleMSS=("AliEn::MSS::$self->{METHOD}", 
		     "AliEn::MSS::\u$self->{METHOD}",
		     "AliEn::MSS::\U$self->{METHOD}\E"); 

    my $test;
    while ($test=shift @possibleMSS)
      {
	print "TRYING $test\n";
	if (eval "require $test"){
	  $self->{MSS_NAME}=$test;
	  print "FUNCIONA!!\n";
	  @possibleMSS=();
	}
	print "$! and $@ and $?\n";
      }

    if (! $self->{MSS_NAME}) {	
      my $name = "AliEn::SE::Methods::$self->{METHOD}";
      eval "require $name"
	or print STDERR "Error requiring the package $name\n$@\nDoes the method $self->{METHOD} exist?\n"
	  and return;
      
      @ISA = ( $name, @ISA );
    }


    my $name;
    $self->{PATH} =~ /\/([^\/]*)$/ and $name = $1;
    $self->{LOCALFILE}
      or $self->{LOCALFILE} = "$self->{CONFIG}->{CACHE_DIR}/$name.$$";

    $self->initialize() or return;

    my $tempdir = $self->{LOCALFILE};
    $tempdir =~ s/\/[^\/]*$//;

    $self->{DEBUG} and print "Creating directory $tempdir\n";
    if ( !( -d $tempdir ) ) {
        my $dir = "";
        foreach ( split ( "/", $tempdir ) ) {
            $dir .= "/$_";
            mkdir $dir, 0777;
        }
    }
    print "TODO BIEN\n";
    return $self;
}


sub parsePFN {
    my $self = shift;

    $self->{LOGGER}->debug("Method", "Getting method of $self->{PFN}...");

    $self->{PFN} =~ s/^([^:]*):\/([^\/])/$1:\/\/\/$2/;
    $self->{PFN} =~ /^([^:]*):\/\/([^\/]*)(\/[^?]*)\??(.*)$/;
    $self->{METHOD} = ( $1 or "" );
    $self->{HOST}   = ( $2 or "" );
    $self->{PATH}   = ( $3 or "" );
    $self->{VARS}   = ( $4 or "" );

    $self->{PORT} = "";

    #    ($self->{HOST}=~ s/($[^:]*):(.*)^/$1/) and ($self->{PORT}=$2);
    ( $self->{HOST} =~ s/\:(.*)// ) and ( $self->{PORT} = $1 );

    $self->{LOGGER}->debug("Method", "the list includes $self->{VARS}");
    my @list=split ( /[=\?]/, $self->{VARS} );

    while (@list){
      my ($key, $value)= (shift @list, shift @list);

      ($key and $value) or last;
      $key="\U$key\E";
      $self->{LOGGER}->debug("Method", "Putting variable VARS_$key as $value");
      $self->{"VARS_$key"}=$value;
    }
    $self->{LOGGER}->debug ("Method", "Parsed info: $self->{METHOD} $self->{HOST} $self->{PORT} $self->{PATH} $self->{VARS}");

}

sub host{
    my $self=shift;
    return $self->{HOST};
}
sub path{
    my $self=shift;
    return $self->{PATH};
}
sub port{
    my $self=shift;
    return $self->{PORT};
}
sub scheme {
    my $self=shift;
    return $self->{METHOD};
}

sub get {
  my $self=shift;
  print "AQUI\n";
  if ($self->{MSS_NAME})
    {
      print "TRYING THE NEW WAY\n";
      $self->{MSS_NAME}->cp( $self->{PATH},  $self->{LOCALFILE})
	and print STDERR "Error: not possible to copy file $self->{PATH}!!\n"
	  and return;
      ( -f $self->{LOCALFILE} )
	or print STDERR "Error: file not copied!!\n"
	  and return;
      ( $self->{DEBUG} > 0 )
	and print
	  "DEBUG LEVEL 1\t\t In castor: File $self->{PATH} copied in $self->{LOCALFILE}\n";
      return $self->{LOCALFILE};
    }
  else 
    {
      print "DOING IT THE OLD WAY\n";
      return $self->SUPER::get(@_);
    }
}
sub getSize {
  my $self=shift;

  if ($self->{MSS_NAME})
    {
      print "TRYING THE NEW WAY CON @_\n";
      my $size=$self->{MSS_NAME}->sizeof( $self->{PATH});
      print "TENGO $size d\n";
      return $size;
    }
  else 
    {
      print "DOING IT THE OLD WAY\n";
      return $self->SUPER::getSize(@_);
    }
}

sub initialize{ 
  my $self=shift;
  
  $self->{MSS_NAME} and return 1;
   return $self->SUPER::initialize(@_);
}
return 1;
