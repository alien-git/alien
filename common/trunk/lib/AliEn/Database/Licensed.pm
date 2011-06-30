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
package AliEn::Database::Licensed;

use AliEn::Database;
use strict;

use vars qw(@ISA);

@ISA = ("AliEn::Database");

sub preConnect {
  my $self = shift;
  $self->{DB} and $self->{HOST} and $self->{DRIVER} and return 1;

  my $info = $self->{CONFIG}->{CATALOGUE_DATABASE};
  $info =~ s{/[^/]*$}{/licensedSoft};

  $self->info("Using  $info");
  ($self->{HOST}, $self->{DRIVER}, $self->{DB}) = split(m{/}, $info);

  return 1;
}

sub initialize {
  my $self    = shift;
  my %columns = (
    packageId            => "int(11) NOT NULL auto_increment PRIMARY KEY",
    packageName          => "char(255) NOT NULL",
    packageVersion       => "char(255) NOT NULL",
    dependencies         => "char(255) NOT NULL",
    licensed             => "BOOLEAN",
    installation         => "blob",
    configurationFile    => "char(255)",
    configurationCommand => "blob",
    installed            => "int(11)",
    beingInstalled       => "BOOLEAN",
    installDir           => "char(255)",
    installAction        => "char(255)",
  );
  $self->checkTable("PACKAGES", "packageId", \%columns, 'packageId', []) or return;
  $self->checkTable(
    "LICENSES",
    "licenseId",
    { licenseId            => "int(11) NOT NULL auto_increment PRIMARY KEY",
      packageId            => "int(11) NOT NULL",
      totalSeats           => "int(11)",
      configurationCommand => "blob",
      status               => "char(20)",
    }
  ) or return;

  $self->checkTable(
    "CURRENTLICENSES",
    "tokenId",
    { tokenId   => "int(11) NOT NULL auto_increment PRIMARY KEY",
      licenseId => "int(11) NOT NULL",
      startTime => "dateTime",
      endTime   => "dateTime",
      user      => "char(255)",
    }
  );
  return 1;
}

sub getPackage {
  my $self    = shift;
  my $package = shift;
  my $version = shift;
  my $query   = "SELECT * from PACKAGES where packageName='$package'";
  $version and $query .= " and packageVersion='$version'";
  my $entry = $self->query($query) or return;

  return ${$entry}[0];
}

sub getLicenseServers {
  my $self        = shift;
  my $packageInfo = shift;

  my $licenseServers =
    $self->query("SELECT * from LICENSES where packageId=$packageInfo->{packageId} and status='ACTIVE'");

  return $licenseServers;
}

sub getLicenseToken {
  my $self        = shift;
  my $licenseInfo = shift;

  my $time = shift;
  $self->info("Trying to get a token for the license $licenseInfo->{licenseId}");
  $self->lock("CURRENTLICENSES");
  my $current = $self->queryValue(
"SELECT count(*) from CURRENTLICENSES where licenseId=$licenseInfo->{licenseId} and  startTime<now()+time_to_sec($time) and endTime>now()"
  );
  $current or $current = 0;
  $self->info("There are $current tokens in this server");
  my $token = 0;

  if ( ($current < $licenseInfo->{totalSeats})
    || ($licenseInfo->{totalSeats} eq "-1")) {
    $self->info("We could get a token in this server");
    if (
      $self->do(
"INSERT INTO CURRENTLICENSES (licenseId, startTime, endTime) values ( $licenseInfo->{licenseId}, now(),now()+sec_to_time($time))"
      )
      ) {
      $token = $self->getLastId("CURRENTLICENSES");
    } else {
      $self->info("Error inserting the token");
    }
  }
  $self->unlock();
  print "DEVOLVEMOS $token\n";
  return $token;
}

sub releaseLicenseToken {
  my $self  = shift;
  my $token = shift;
  return $self->delete("CURRENTLICENSES", "tokenId=$token");
}

1;
