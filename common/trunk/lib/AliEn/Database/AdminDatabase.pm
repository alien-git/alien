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
package AliEn::Database::AdminDatabase;

use DBI;
use AliEn::Authen::IIIkey;
use AliEn::Config;
use strict;

#
# This class is used as a wrapper of a DBI databasehandle to the ADMIN database on Master server
#
my $dbh;

sub new {
    my $proto  = shift;
    my $class  = ref($proto) || $proto;
    my $self   = {};
    my $passwd = shift;
    if ( !$passwd ) {
        print STDERR "Must be called with admin password as first parameter.\n";
        return;
    }
    if ($dbh) {
        $dbh->disconnect();
    }

    my $ini = AliEn::Config->new();
    ($ini)
      or print STDERR "Error: Initial configuration not found!!\n"
      and return;

    $self->{CENTRALSERVER}   = $ini->getValue('AUTHEN_HOST');
    $self->{CENTRALDATABASE} = $ini->getValue('AUTHEN_DATABASE');
    $self->{CENTRALDRIVER}   = $ini->getValue('AUTHEN_DRIVER');

    my $dsn =
"DBI:$self->{CENTRALDRIVER}:database=$self->{CENTRALDATABASE};host=$self->{CENTRALSERVER};";

    $dbh = DBI->connect( $dsn, 'admin', "$passwd", {} );
    if ( !$dbh ) {
        print STDERR "Error connecting to ADMIN database\n";
        return;
    }
    my $sth = $dbh->prepare("SELECT DBKey from DBKEYS where Name='GlobalKey'");
    $sth->execute();
    my @rows = $sth->fetchrow();
    $sth->finish();
    $self->{GLOBALKEY} = $rows[0];
    $self->{encrypter} = new AliEn::Authen::IIIkey();
    bless( $self, $class );
    return $self;
}

sub close {
    my $self = shift;
    $dbh->disconnect();
}

sub getServerKey {
    my $self = shift;
    return $self->{GLOBALKEY};
}

sub getEncToken {
    my $self     = shift;
    my $username = shift || return;
    my $token    = $self->getToken($username);
    return $self->{encrypter}->crypt( $token, $self->{GLOBALKEY} );
}

sub getSSHKey {
    my $self     = shift;
    my $username = shift;
    my $sth      =
      $dbh->prepare("SELECT SSHKey from TOKENS where Username='$username'");
    $sth->execute();
    my @rows = $sth->fetchrow();
    $sth->finish();
    return $rows[0];
}

sub getPasswd {
    my $self     = shift;
    my $username = shift;
    my $sth      =
      $dbh->prepare("SELECT password from TOKENS where Username='$username'");
    $sth->execute();
    my @rows = $sth->fetchrow();
    $sth->finish();
    return $rows[0];
}

sub getToken {
    my $self     = shift;
    my $username = shift;
    my $sth      =
      $dbh->prepare("SELECT token from TOKENS where Username='$username'");
    $sth->execute();
    my @rows = $sth->fetchrow();
    $sth->finish();
    return $rows[0];
}

sub addTimeToToken {
    my $self  = shift;
    my $user  = shift;
    my $hours = shift;

    $dbh->do(
"update TOKENS set Expires=(DATE_ADD(now() ,INTERVAL $hours HOUR)) where Username='$user'"
    );
    return 1;
}

sub insert {
    my $self    = shift;
    my $command = shift;
    $dbh->do($command);
    return 1;
}

sub query {
    my $self     = shift;
    my $sentence = shift;
    my ( @result, @name, $i );
    $i = 0;

    my $sth = $dbh->prepare($sentence);
    $sth->execute or return;

    while ( @name = $sth->fetchrow() ) {
        $result[ $i++ ] = join "###", @name;
    }
    $sth->finish();

    return @result;
}

sub insertSSHKey() {
    my $self = shift;
    my $user = shift;
    my $key  = shift;
    $self->insert("UPDATE TOKENS set SSHKey='$key' where Username = '$user'");
    return 1;
}

sub getTokenValidPeriod{
    my $self     = shift;
    my $username = shift;

	my @rows      = $dbh->query("SELECT Token,(Expires-Now()) from TOKENS where Username='$username'");

	return $rows[0];
}

return 1;

