#/**************************************************************************
# * Copyright(c) 2001-2003, ALICE Experiment at CERN, All rights reserved. *
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

package AliEn::Database::Admin;

use AliEn::Authen::IIIkey;
use AliEn::Database;

use strict;

use vars qw(@ISA);
@ISA=("AliEn::Database");


sub new {
    my $proto  = shift;
    my $class  = ref($proto) || $proto;
	my $attr   = shift || {};

	$attr->{CONFIG}
		or $attr->{CONFIG} = new AliEn::Config();

	$attr->{DB}
		or $attr->{DB} = $attr->{CONFIG}->getValue('AUTHEN_DATABASE');

	$attr->{DRIVER}
		or $attr->{DRIVER} = $attr->{CONFIG}->getValue('AUTHEN_DRIVER');

	$attr->{HOST}
		or $attr->{HOST} = $attr->{CONFIG}->getValue('AUTHEN_HOST');

	$attr->{ROLE}
		or $attr->{ROLE} = "admin";

	my $self = new AliEn::Database($attr,@_);
    
    $self
      or $self->{LOGGER}->error("Admin","Error creating Database instance")
	and return;
    
    #my $passwd = shift or print STDERR "Must be called with admin password as first parameter.\n" and return;
    
    $self->{ENCRYPTER} = new AliEn::Authen::IIIkey();
    bless ($self, $class);
    
    $self->{GLOBALKEY} = $self->getDBKey();
    
    return $self;
}

sub getServerKey {
    my $self = shift;
    return $self->{GLOBALKEY};
}

sub getDBKey {
	my $self = shift;
	my $username = shift;
	my $string;
	if (!defined($username)) {
		$string = "GlobalKey";
	} else {
		$string = $username . "PrivateKey";
	}
	$self->queryValue("SELECT DBKey FROM DBKEYS WHERE Name = '$string'");
}

sub getEncToken {
    my $self     = shift;
    my $username = shift;
    my $token    = $self->getToken($username) or return;
    return $self->{ENCRYPTER}->crypt( $token, $self->{GLOBALKEY} );
}

sub getToken {
  return shift->getFieldFromTokens(shift,"Token");
}

sub getSSHKey {
  return shift->getFieldFromTokens(shift,"SSHKey");
}

sub getPassword {
  return shift->getFieldFromTokens(shift, "password");
}

sub setToken {
  my $self = shift;
	
  $self->debug(1,"In setToken updating user's token");
  $self->updateToken(shift, {Token=>shift});
}

sub setSSHKey {
  my $self = shift;

  $self->debug(1,"In setSSHKey updating user's SSHKey");
  $self->updateToken(shift, {SSHKey=>shift});
}

sub setPassword {
  my $self = shift;

  $self->debug(1,"In setPassword updating user's password");
  $self->updateToken(shift, {password=>shift});
}

sub getAllFromTokens {
    my $self = shift;
    my $attr = shift || "*";

    $self->query("SELECT $attr from TOKENS");
}

sub insertToken {
	my $self = shift;
	my $set;

	$set->{ID} = shift;
	$set->{Username} = shift;
	$set->{Expires} = "Now()";
	$set->{Token} = shift;
	$set->{password} = shift;
	$set->{SSHKey} = shift;

	return $self->insert("TOKENS",$set);
}

sub updateToken {
  my $self = shift;
  my $user = shift
    or $self->{LOGGER}->error("Admin","In updateToken user name is missing")
      and return;
  my $set = shift;

  $self->debug(1,"In updateToken updating token for user $user");
  return $self->update("TOKENS", $set, "Username = '$user'");
}

sub deleteToken {
  my $self = shift;
  my $user = shift
    or $self->{LOGGER}->error("Admin","In deleteToken user name is missing")
      and return;
  
  $self->debug(1,"In deleteToken deleting token for user $user");
  return $self->delete("TOKENS","Username = '$user'");
}

sub existsToken {
  my $self  = shift;
  my $user = shift
    or $self->{LOGGER}->error("Admin","In existsToken user name is missing")
      and return;
		
  $self->debug(1,"In existsToken checking if user $user have token");
  return $self->queryValue("SELECT COUNT(*) FROM TOKENS WHERE Username='$user'");
}

sub addTime {
  my $self  = shift;
  my $user = shift
    or $self->{LOGGER}->error("Admin","In addTime user name is missing")
      and return;
  my $hours = shift
    or $self->{LOGGER}->error("Admin","In addTime time interval is missing")
      and return;
  
  $self->debug(1,"In addTime increasing expiration period for user's $user token");
  return $self->do("UPDATE TOKENS SET Expires=(DATE_ADD(now() ,INTERVAL $hours HOUR)) WHERE Username='$user'");
}

sub getFieldsFromTokens {
  my $self = shift;
  my $username = shift
    or $self->{LOGGER}->error("Admin","In getFieldsFromTokens user name is missing")
      and return;
  my $attr = shift || "*";
  
  $self->debug(1,"In getFieldsFromTokens fetching attributes $attr for user name $username from table TOKENS");
  $self->queryRow("SELECT $attr from TOKENS where Username='$username'");
}

sub getFieldFromTokens {
  my $self = shift;
  my $username = shift
    or $self->{LOGGER}->error("Admin","In getFieldFromTokens user name is missing")
      and return;
  my $attr = shift || "*";

  $self->debug(1,"In getFieldFromTokens fetching attribute $attr for user name $username from table TOKENS");
  $self->queryValue("SELECT $attr from TOKENS where Username='$username'");
}

###		jobToken

sub insertJobToken {
  my $self = shift;
  my $id = shift;
  my $user = shift;
  my $token = shift;

  $self->debug(1,"In insertJobToken inserting new data into table jobToken");
  return $self->insert("jobToken",{jobId=>$id, userName=>$user, jobToken=>$token});
}

sub getFieldFromJobToken {
  my $self = shift;
  my $id = shift
    or $self->{LOGGER}->error("Admin","In getFieldFromJobToken job id is missing")
      and return;
  my $attr = shift || "*";
  
  $self->debug(1,"In getFieldFromJobToken fetching attribute $attr for job id $id from table jobToken");
  return $self->queryValue("SELECT $attr FROM jobToken WHERE jobId='$id'");
}

sub getFieldsFromJobToken {
  my $self = shift;
  my $id = shift
    or $self->{LOGGER}->error("Admin","In getFieldsFromJobToken job id is missing")
      and return;
  my $attr = shift || "*";
  
  $self->debug(1,"In getFieldsFromJobToken fetching attributes $attr for job id $id from table jobToken");
  return $self->queryRow("SELECT $attr FROM jobToken WHERE jobId='$id'");
}

sub setJobToken {
  my $self = shift;
  my $id = shift
    or $self->{LOGGER}->error("Admin","In setJobToken job id is missing")
      and return;
  my $token = shift;
  
  $self->debug(1,"In setJobToken updating token for job $id");
  return $self->update("jobToken",{jobToken=>$token},"jobId='$id'");
}

sub deleteJobToken {
  my $self = shift;
  my $id = shift
    or $self->{LOGGER}->error("Admin","In deleteJobToken job id is missing")
      and return;

  $self->debug(1,"In deleteJobToken deleting token for job $id");
  return $self->delete("jobToken","jobId='$id'");
}

sub getUsername {
  my $self = shift;
  my $id = shift
    or $self->{LOGGER}->error("Admin","In getUsername job id is missing")
      and return;
  my $token = shift
    or $self->{LOGGER}->error("Admin","In getUsername job token is missing")
      and return;

  $self->debug(1,"In getUsername fetching user name for job $id and token $token");	
  return $self->queryValue("SELECT userName FROM jobToken where jobId='$id' and jobToken='$token'");
}

=head1 NAME

AliEn::Database::Admin

=head1 DESCRIPTION

The AliEn::Database::Admin module extends AliEn::Database module. Module
contains method specific for tables TOKENS and jobToken.

=head1 SYNOPSIS

  use AliEn::Database::Admin;

  my $dbh = AliEn::Database::Admin->new($dbOptions);

  $res = $dbh->getFieldFromTokens($user, $attr);
  $res = $dbh->getFieldFromJobToken($jobId, $attr);
  
  $hashRef = $dbh->getFieldsFromTokens($user, $attr);
  $hashRef = $dbh->getFieldsFromJobToken($jobId, $attr);
  
  $arrRef = $dbh->getAllFromTokens($attr);
    
  $res = $dbh->getToken($user);
  $res = $dbh->getSSHKey($user);
  $res = $dbh->getPassword($user);
  
  $res = $dbh->getServerKey();
  $res = $dbh->getDBKey($user);
  $res = $dbh->getEncToken($user);
  
  $res = $dbh->getUserName($jobId, $jobToken);
  
  $res = $dbh->existsToken($user);
   
  $res = $dbh->insertToken($id, $user, $token, $password,$SSHKey);
  $res = $dbh->insertJobToken($id, $user, $jobToken);

  $res = $dbh->updateToken($user, $set);
  $res = $dbh->addTime($user, $interval);
  
  $res = $dbh->setToken($user, $token);
  $res = $dbh->setSSHKey($user, $key);
  $res = $dbh->setPassword($user, $password);
  $res = $dbh->setJobToken($jobId, $jobToken);
  
  $res = $dbh->deleteToken($user);
  $res = $dbh->deleteJobToken($jobId);
  
=cut

=head1 METHODS

=over

=item C<new>

  $dbh = AliEn::Database::Admin->new( $attr );

  $dbh = AliEn::Database::Admin->new( $attr, $attrDBI );

Creates new AliEn::Database::Admin instance. Arguments are passed to AliEn::Database
method new. For details about arguments see AliEn::Database method C<new>.
Method new sets following attributes if they are not defined: ROLE, HOST, DRIVER,
DB. Attribute ROLE is set to 'admin'. Attributes DRIVER, DB and HOST are set
to values AUTHEN_DRIVER, AUTHEN_DATABASE and AUTHEN_HOST which are fetched using
AliEn::Config module.
Method initialize property GLOBALKEY to value of global key which is fetched from
table DBKEYS. Value of this property can be fetched using C<getServerKey> method.

=item C<getFieldFrom*>

  $res = $dbh->getFieldFromTokens($user, $attr);
  
  $res = $dbh->getFieldFromJobToken($jobId, $attr);  
  
Method fetches value of attribute $attr for tuple with defined unique id: 
in case of Tokens user and in case of jobToken job id.
If unique id is not defined method will return undef and report error.
Method calls AliEn::Database metod queryValue.

=item C<getFieldsFrom*>

  $hashRef = $dbh->getFieldsFromTokens($user, $attr);
  
  $hashRef = $dbh->getFieldsFromJobToken($jobId, $attr);  
  
Method fetches set of attributes $attr for tuple with defined unique id: 
in case of Tokens user and in case of jobToken job id.
Result is reference to hash. Keys in hash are identical to names of attriutes 
in $attr set.
If set of attributes is not defined method returns values of all attributes. If
unique id is not defined method will return undef and report error.
Method calls AliEn::Database metod queryRow.
  
=item C<getAllFromTokens> 

  $arrRef = $dbh->getAllFromTokens($attr);
  
Method retrieves set of attributes $attr for all tuples from table TOKENS. If 
argument $attr is not defined method will return all attributes.

=item C<get*>     
   
  $res = $dbh->getToken($user);
  
  $res = $dbh->getSSHKey($user);
  
  $res = $dbh->getPassword($user);

Method fetches attribute from table TOKENS for user $user. 
If argument $user is not defined method will return undef
and report error.  

=item C<getServerKey>  

  $res = $dbh->getServerKey();
  
Method returns value of property GLOBALYKEY which is set on value
of global key during method C<new> call. 

=item C<getDBKey>  

  $res = $dbh->getDBKey($user);
  
Method fetches value of attribute DBKey from table DBKEYS for name $user. 
If argument $user is not defined method will return DBKey for name GlobalKey.

=item C<getEncToken>  

  $res = $dbh->getEncToken($user);
  
Method fetches encrypted token for user $user from table TOKENS.
For encryption is used module AliEn::Authen::IIIkey.
If argument $user is not defined method will return undef and report error.

=item C<getDBKey>  

  $res = $dbh->getUserName($jobId, $jobToken);
  
Method fetches name of owner of jobtoken $jobToken for job $jobId.
If any of argument is not defined method will return undef and report error.

=item C<existsToken>  

  $res = $dbh->existsToken($user);
  
Method checks if user $user have token in TOKENS table. 
If $user is not defined method will return undef and report error.
    
=item C<insertToken>  

  $res = $dbh->insertToken($id, $user, $token, $password,$SSHKey);
  
Method inserts new token into table TOKENS with defined arguments.
Attribute expires is set to Now(). 
    
=item C<insertJobToken>  

  $res = $dbh->insertJobToken($id, $user, $jobToken);
  
Method inserts new job token into table jobToken with defined arguments.
Attribute expires is set to Now(). 
  
=item C<updateToken>     

  $res = $dbh->updateToken($user, $set);
  
Method updates data for user $user in table TOKENS with update set $set. 
Form of $set argument is defined in AliEn::Database C<update> method.  
If user is not defined method will return undef and report error.
  
=item C<addTime>     

  $res = $dbh->addTime($user, $interval);
  
Method increases value of expires attribute in table TOKENS for user
$user. Time unit of argument $interval is hour.
If any of argument is not defined method will return undef and report error.
 
=item C<set*>     
   
  $res = $dbh->setToken($user, $token);
  
  $res = $dbh->setSSHKey($user, $key);
  
  $res = $dbh->setPassword($user, $password);

Method updates value of attribute in table TOKENS for user $user. 
If argument $user is not defined method will return undef
and report error.  
 
=item C<set*>     
   
  $res = $dbh->setToken($user, $token);
  
  $res = $dbh->setSSHKey($user, $key);
  
  $res = $dbh->setPassword($user, $password);

Method updates value of attribute in table TOKENS for user $user. 
If argument $user is not defined method will return undef
and report error.  
 
=item C<setJobToken>     
   
  $res = $dbh->setToken($jobIf, $jobToken);
  
Method updates value of attribute jobToken in table jobToken
for job $jobId. 
If argument $jobId is not defined method will return undef
and report error.  

=item C<delete*>     

  $res = $dbh->deleteToken($id);
  
  $res = $dbh->deleteJobToken($id);
  
Method deletes entry for given unique id $id. In case of Token unique
id is user name, and in case of Job Token unique id is job id.
If unique id is not defined method will return undef and report error.

=back

=head1 SEE ALSO

AliEn::Database, AliEn::Authen::IIIkey, AliEn::Config

=cut

1;



