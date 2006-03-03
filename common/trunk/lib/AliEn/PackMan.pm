package AliEn::PackMan;

use strict;

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = ( shift or {} );
    $self->{CONFIG} or print STDERR "Error: no config in PackMan\n" and return;

    bless( $self, $class );

    return $self;
}

sub DESTROY {
    my $self = shift;
    undef $self;
}

sub Configure {
    my $self  = shift;
    my $name  = shift;
    my $ORG   = $self->{CONFIG}->{ORG_NAME};
    my $name2 = "AliEn::" . $ORG . "::Packages::$name";
    eval "require $name2"
      or print STDERR "Error requiring $name2\n\t$!\nTry with common Package ...\n" and
	  $name2 = "AliEn::Package::$name" and
	      eval "require $name2" or
		  print STDERR "Error requiring common Package $name2\n\t$!\n" 
		      and return;
    my $version = ( shift or $name2->DefaultVersion );

    ( $self->{PACKAGES}->{$name}->{$version} )
      or print STDERR
      "Error: Package $name in version $version is not installed\n"
      and return;

    $self->{PACKAGES}->{$name}->{$version}->Configure();

}

sub List {

    my $self = shift;
    my $pack;
    my $version;
    my $packref  = $self->{PACKAGES};
    my @packages = keys(%$packref);
    foreach $pack (@packages) {
        $packref = $self->{PACKAGES}->{$pack};
        my @versions = keys(%$packref);
        foreach $version (@versions) {
            my $info = $self->{PACKAGES}->{$pack}->{$version}->Provide;
            print STDERR "$info->{package} V$info->{version}\n";
        }

    }
}

sub Add {
    my $self    = shift;
    my $package = ( shift or "" );

    ( UNIVERSAL::isa( $package, "HASH" ) )
      or print STDERR
      "ERROR: Adding package, hash expected, and '$package' received\n"
      and return;

    my $name    = $package->{name};
    my $version = $package->{version};
    ($name)
      or print STDERR "Error package '$package' does not have a name\n"
      and return;
    ($version)
      or print STDERR
      "Error package $name (from '$package') does not have a name\n"
      and return;

    $package->{PACKMAN} = $self;

    ( $self->{PACKAGES}->{$name} ) or ( $self->{PACKAGES}->{$name} = {} );
    ( $self->{PACKAGES}->{$name}->{$version} ) and return 1;

    my $ORG      = $self->{CONFIG}->{ORG_NAME};
    my $fullName = "AliEn::" . $ORG . "::Packages::${name}";

    eval "require $fullName"
	or 
	    print STDERR
		"Error: Package $fullName does not exist in your organisation\n$!\nTry a common package ...\n" and
		    $fullName = "AliEn::Package::${name}"  and
			eval "require $fullName" or
			    print STDERR
				"Error: Package $fullName does not exist as a common package \n$!\n"
				    and return;

    $self->{PACKAGES}->{$name}->{$version} = $fullName->new($package);

    $self->{PACKAGES}->{$name}->{$version} or return;

    return 1;
}

sub RemoveAll {
    my $self = shift;
    $self->{PACKAGES} = {};
    return 1;

}

sub GetLatestVersion{
  my $self=shift;
  my $package=shift;

  ( $self->{CONFIG}->{DEBUG} > 0 )
    and print "Getting the version of '$package'...";

  $self->{PACKAGES}->{$package} 
    or print  STDERR "Error no versions of $package installed\n" and return;

  my @versions= sort keys %{$self->{PACKAGES}->{$package}};
  my $latest=$versions[$#versions];

  ( $self->{CONFIG}->{DEBUG} > 0 )
    and print "$latest\n";

  return $latest;
}

return 1;

