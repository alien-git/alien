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
package AliEn::Package;

use strict;

use AliEn::UI::Catalogue::LCM;

sub Require {
    my $self = shift;
    return $self->{"require"};
}

sub DESTROY {
    my $self = shift;
    undef $self;
}

sub Provide {
    my $self = shift;

    return $self->{provide};
}

sub new {
    my $proto = shift;
    my $class = ref($proto) || $proto;
    my $self  = ( shift or {} );
    bless( $self, $class );

    ( $self->{PACKMAN} )
      or print STDERR
      "Error creating the package: Package manager not received\n"
      and return;

    ( $self->{version} )
      or print STDERR "Error creating the package: Version not received\n"
      and return;

    $self->{provide} = {};

    $self->{provide}->{"package"} = $self->{name};
    $self->{provide}->{version} = $self->{version};
    $self->{debug} or $self->{debug} = 0;

    ( $self->{path} ) or print STDERR "Error: missing path\n" and return;

    $self->Initialize() or return;

    $self->CheckRequirements() or return;

    if ( $self->{path} eq "auto" ) {
        ( $self->{PACKMAN}->{CONFIG}->{DEBUG} > 0 )
          and print "Automatic package " . $self->{provide}->{"package"} . "\n";
        $self->{path} =
"$ENV{ALIEN_HOME}/packages/$self->{provide}->{package}/$self->{provide}->{version}";
        if ( !$self->{PACKMAN}->{CONFIG}->{PACKINSTALL} ) {
            ( $self->{PACKMAN}->{CONFIG}->{DEBUG} > 0 )
              and print "Skip installation\n";
            return $self;
        }
        if ( !$self->Install ) {
            print STDERR "Error installing the package\n";
            return;
        }
    }
    return $self;
}

sub CheckRequirements {
    my $self = shift;

    my @requirements = ();
    $self->{require} and @requirements=@{ $self->{"require"} };


    $self->{PACKMAN}->{CONFIG}->{DEBUG} > 0
      and print
"Requirements for $self->{provide}->{package}::$self->{provide}->{version} '@requirements'...";

    my $pack;

    foreach $pack (@requirements) {
        my $reqVer = "current";
        $pack =~ s/::(.*)$// and ( $reqVer = $1 );
        $self->{PACKMAN}->{CONFIG}->{DEBUG} > 0
          and print "\nChecking if $pack $reqVer exists...";
        $self->{PACKMAN}->{PACKAGES}->{$pack}
          or print STDERR
"Error: $self->{provide}->{package}::$self->{provide}->{version} requires Package $pack, and it is not install in this site\n"
          and return;
        ($reqVer) and $self->{PACKMAN}->{PACKAGES}->{$pack}->{$reqVer}
          or print STDERR
"Error: $self->{provide}->{package}::$self->{provide}->{version} requires version $reqVer of $pack, and it is not not install in this site\n"
          and return;
        $self->{PACKMAN}->{CONFIG}->{DEBUG} > 0 and print "ok\n";
    }
    $self->{PACKMAN}->{CONFIG}->{DEBUG} > 0 and print "Package added\n\n";
    return 1;
}

sub Initialize {
  print "Initializing the package\n";
  return 1;
}

sub Configure {
    my $self = shift;

    my @packages = @{ $self->{"require"} };

    foreach (@packages) {
        my ( $name, $version ) = split ( "::", $_ );
        $version or $version = "current";
        print "Configuring $name $version\n";
        $self->{PACKMAN}->{PACKAGES}->{$name}->{$version}->Configure();
    }

    return 1;
}

sub Install {
    my $self = shift;

    #Getting the file from the catalog;

    ( $self->{PACKMAN}->{CONFIG}->{DEBUG} > 0 )
      and print "Checking if the path is already installed\n";

    #   print "ESTAMOS MIRANDO SI ESTA INSTALADO\nTENGO $self->{PACKMAN}->{CONFIG}->{CLUSTER_MONITOR_USER} y $self->{PACKMAN}->{CONFIG}->{LOCAL_USER}\n";

    #    print "VAMOS BIEN\n";
    #    exit;
    #    my $path="$ENV{ALIEN_HOME}/packages/".$self->{provide}->{'package'}."/$self->{provide}->{version}";

    if ( -d $self->{path} ) {

        #    print "VAMOS BIEN SIN #\n";

        ( $self->{PACKMAN}->{CONFIG}->{DEBUG} > 0 )
          and print "Package already installed\n";
        return 1;
    }

    #    print "VAMOS BIEN\n";

    print "Installing package $self->{path}\n";

    ( $self->{PACKMAN}->{CONFIG}->{DEBUG} > 0 )
      and print "Creating the directory $self->{path}";

    if ( !( -d $self->{path} ) ) {
        my $dir = "";
        foreach ( split ( "/", $self->{path} ) ) {
            $dir .= "/$_";
            mkdir $dir, 0777;
        }
    }

    chdir( $self->{path} )
      or print STDERR "\nError creating $self->{path} $!\n"
      and return;

    ( $self->{PACKMAN}->{CONFIG}->{DEBUG} > 0 )
      and print "ok\nGetting the file...";
    my $sys1 = `uname -s`;
    chomp $sys1;
    my $sys2 = `uname -m`;
    chomp $sys2;

    my $fileName = "/"
      . $self->{PACKMAN}->{CONFIG}->{ORG_NAME}
      . "/packages/"
      . $self->{provide}->{'package'}
      . "/$sys1-$sys2.$self->{provide}->{version}";

    my $catalog = AliEn::UI::Catalogue::LCM->new(
        {
            "silent" => 1,
            "user", $self->{PACKMAN}->{CONFIG}->{LOCAL_USER},
            "role", $self->{PACKMAN}->{CONFIG}->{CLUSTER_MONITOR_USER},
	    "FORCED_AUTH_METHOD","SSH"
        }
    );
    if (!$catalog)  {
      print STDERR "ERROR Getting the catalogue\n";
      rmdir $self->{path};
      return;
    }

    # we use aioget to install packages now ...

#    my ($file) =
#      $catalog->execute( "get", "$fileName",
#			 "$self->{path}/Pac$$.tar.gz" );

    my ($file) = 
	$catalog->execute( "aioget","-n","$fileName","$self->{path}/Pac$$.tar.gz" ); 
    $catalog->close;

    if ( !$file ) {
        print STDERR "Error getting the file\n$!\n";
        rmdir $self->{path};
        print STDERR "Directory $self->{path} removed\n";
        return;
    }
    ( $self->{PACKMAN}->{CONFIG}->{DEBUG} > 0 )
      and print "ok\nUncompressing the file $file...";

    my $error = system( "tar", "zxf", "$file" );
    if ($error) {
        print STDERR "Error uncompressing $!\n";
        unlink $file;
        rmdir $self->{path};
        print STDERR "Directory $self->{path} removed\n";
        return;
    }

    ( $self->{PACKMAN}->{CONFIG}->{DEBUG} > 0 )
      and print "ok\nRemoving the file...";
    unlink $file;

    ( $self->{PACKMAN}->{CONFIG}->{DEBUG} > 0 )
      and print "ok\nChanging the path of the product\n";

    print "Package installed!!\n";
    return 1;
}
return 1;

__END__


=head1 NAME

Package - A class to include different software packages in AliEn

=head1 SYNOPSIS

=over 4

=item new

=item Require

=item DefaultVersion

=item Provide

=item CheckRequirements

=item Initialize

=item Configure

=item Install

=back

=head1 DESCRIPTION



=head1  SEE ALSO

L<Package::PackMan>
