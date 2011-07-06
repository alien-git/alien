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
package AliEn::Database::Catalogue;

use AliEn::Database;
use AliEn::Database::Catalogue::LFN;
use AliEn::Database::Catalogue::GUID;

use strict;
use AliEn::SOAP;
use AliEn::GUID;

=head1 NAME

AliEn::Database::Catalogue - database wrapper for AliEn catalogue

=head1 DESCRIPTION

This module interacts with a database of the AliEn Catalogue. The AliEn Catalogue can be distributed among several databases, each one with a different layout. In this basic layout, there can be several tables containing the entries of the catalogue. 

=cut

use vars qw(@ISA $DEBUG);

push @ISA, qw(AliEn::Database);
$DEBUG = 0;

=head1 SYNOPSIS

  use AliEn::Database::Catalogue;

  my $catalogue=AliEn::Database::Catalogue->new() or exit;


=head1 METHODS

=over

=cut

sub preConnect {
  my $self = shift;
  
  $self->{DB} and $self->{HOST} and $self->{DRIVER} and return 1;

  #! ($self->{DB} and $self->{HOST} and $self->{DRIVER} ) or (!$self->{CONFIG}->{CATALOGUE_DATABASE}) and  return;
  $self->debug(2, "Using the default $self->{CONFIG}->{CATALOGUE_DATABASE}");
  ($self->{HOST}, $self->{DRIVER}, $self->{DB}) = split(m{/}, $self->{CONFIG}->{CATALOGUE_DATABASE});

  return 1;
}

sub initialize {
  my $self = shift;
  my $opt1 = {};
  my $opt2 = {};
  foreach (keys %{$self}) {
    $opt2->{$_} = $opt1->{$_} = $self->{$_};
  }
  foreach ('HOST', 'DRIVER', 'DB') {
    if ($self->{$_} eq "1") {
      delete $opt1->{$_};
      delete $opt2->{$_};
    }
  }

  $self->{LFN_DB} = AliEn::Database::Catalogue::LFN->new($opt1, @_) or return;
  $self->{GUID_DB} = AliEn::Database::Catalogue::GUID->new($opt2, @_) or return;

  $self->{LFN_DB}->setConnections($self);
  $self->{GUID_DB}->setConnections($self);

  return 1;
}

=item C<createCatalogueTables>

This methods creates the database schema in an empty database. The tables that this implemetation have are:
HOSTS, 

=cut

#
# Checking the consistency of the database structure
sub createCatalogueTables {
  my $self = shift;

  my $options = shift || {};
  $self->{LFN_DB}->createCatalogueTables() or return;
  my @args;
  $options->{reconnected} and push @args, $self->{LFN_DB};
  $self->{GUID_DB}->createCatalogueTables(@args) or return;

  return 1;
}

sub getAllInfoFromLFN {
  my $self = shift;
  return $self->{LFN_DB}->getAllInfoFromLFN(@_);
}

sub getAllInfoFromGUID {
  my $self = shift;
  return $self->{GUID_DB}->getAllInfoFromGUID(@_);
}

sub getAllExtendedInfoFromLFN {
  my $self = shift;

  my $info = $self->{LFN_DB}->getAllInfoFromLFN({method => "queryRow"}, @_)
    or return;

  $info or $self->info("The entry doesn't exist") and return;

  my $info2 = $self->{GUID_DB}->getAllInfoFromGUID({pfn => 1}, $info->{guid})
    or return;
  $info->{guidInfo} = $info2;
  return $info;
}

=item c<existsEntry($lfn)>

This function receives an lfn, and checks if it exists in the catalogue. It checks for lfns like '$lfn' and '$lfn/', and, in case the entry exists, it returns the name (the name has a '/' at the end if the entry is a directory)

=cut

sub existsEntry {
  return existsLFN(@_);
}

sub existsLFN {
  my $self = shift;
  return $self->{LFN_DB}->existsLFN(@_);
}

=item C<getHostsForEntry($lfn)>

This function returns a list of all the possible hosts and tables that might contain entries of a directory

=cut

sub getHostsForLFN {
  my $self = shift;
  return $self->{LFN_DB}->getHostsForEntry(@_);
}

=item C<getSEListFromFile($lfn)>

Retrieves the list of SE that have a copy of the lfn 

=cut

sub renumberLFNtable {
  my $self = shift;
  return $self->{LFN_DB}->renumberLFNtable(@_);
}

sub renumberGUIDtable {
  my $self = shift;

  return $self->{GUID_DB}->renumberGUIDtable(@_);
}

sub getSEListFromFile {
  return getSEListFromLFN(@_);
}

sub getSEListFromLFN {
  my $self = shift;
  my $lfn  = shift;
  my $guid = $self->{LFN_DB}->getGUIDFromLFN($lfn) or return;
  return $self->getSEListFromGUID($guid, @_);
}

sub getSEListFromGUID {
  my $self = shift;
  return $self->{GUID_DB}->getSEList(@_);
}

=item C<deleteMirrorFromFile($lfn, $seName)>

Deletes a mirror from a file

=cut

sub deleteMirrorFromGUID {
  my $self = shift;
  return $self->{GUID_DB}->deleteMirrorFromGUID(@_);
}

sub deleteMirrorFromLFN {
  my $self = shift;
  my $lfn  = shift;
  my $guid = $self->{LFN_DB}->getGUIDFromLFN($lfn)
    or $self->info("Error getting the guid of $lfn")
    and return;
  return $self->{GUID_DB}->deleteMirrorFromGUID($guid, $lfn, @_);
}

=item C<insertMirrorFromFile($lfn, $seName)>

Inserts mirror of a file

=cut

sub insertMirrorToGUID {
  my $self = shift;
  return $self->{GUID_DB}->insertMirrorToGUID(@_);
}

sub insertMirrorFromFile {
  return insertMirrorToLFN(@_);
}

sub insertMirrorToLFN {
  my $self = shift;
  my $lfn  = shift;
  my $guid = $self->{LFN_DB}->getGUIDFromLFN($lfn);
  return $self->{GUID_DB}->insertMirrorToGUID($guid, @_);
}

sub do {
  my $self = shift;
  return $self->{LFN_DB}->do(@_);
}

sub query {
  my $self = shift;
  return $self->{LFN_DB}->do(@_);
}

sub existsTable {
  my $self = shift;
  return $self->{LFN_DB}->existsTable(@_);
}

sub createCollection {
  my $self = shift;
  $self->{GUID_DB}->insertGUID("", @_) or return;
  return $self->{LFN_DB}->createCollection(@_);
}

sub addFileToCollection {
  my $self = shift;
  return $self->{LFN_DB}->addFileToCollection(@_);
}

sub getInfoFromCollection {
  my $self = shift;
  return $self->{LFN_DB}->getInfoFromCollection(@_);
}

sub removeFileFromCollection {
  my $self = shift;
  return $self->{LFN_DB}->removeFileFromCollection(@_);
}

=item C<createFile($hash)>

Adds a new file to the database. It receives a hash with the following information:



=cut

sub createFile {
  my $self = shift;
  my $options = shift || "";
  $self->debug(2, "In catalogue, createFile");
  if ($options =~ /m/) {
    $self->debug(1, "The guid might be there");
    if (!$self->{GUID_DB}->increaseReferences($options, @_)) {
      $self->{GUID_DB}->insertGUID($options, @_) or return;
    }
  } elsif ($options =~ /k/) {
    $self->debug(4, "The GUID is supposed to be registered");
    $self->{GUID_DB}->increaseReferences($options, @_) or return;
  } else {
    $self->{GUID_DB}->insertGUID($options, @_) or return;
  }
  my $done = $self->{LFN_DB}->createFile($options, @_) or return;
  $self->info("File(s) inserted");
  return $done;
}

sub getParentDir {
  my $self = shift;
  return $self->{LFN_DB}->getParentDir(@_);
}

sub updateFile {
  return updateLFN(@_);
}

sub updateLFN {
  my $self   = shift;
  my $lfn    = shift;
  my $update = shift;
  if ($update->{size} or $update->{md5} or $update->{se}) {
    my $guid = $self->{LFN_DB}->getGUIDFromLFN($lfn) or return;

    #First, let's update the information of the guid
    $self->{GUID_DB}->updateOrInsertGUID($guid, $update, @_)
      or $self->info("Error updating the guid")
      and return;
  }
  if (!$self->{LFN_DB}->updateLFN($lfn, $update,)) {
    $self->info("We should undo the change");
    return;
  }
  return 1;
}

sub deleteFile {
  my $self = shift;
  return $self->{LFN_DB}->deleteFile(@_);
}

sub getLFNlike {
  my $self = shift;
  return $self->{LFN_DB}->getLFNlike(@_);
}
##############################################################################
##############################################################################
#
# Lists a directory: WARNING: it doesn't return '..'
#

=item C<listDirectory($entry, $options)>

Returns all the entries of a directory. '$entry' can be either an lfn (in which case listDirectory will retrieve the rest of the info from the database), or a hash containing the info of that directory. 

Possible options:

=over

=item a

list also the current directory

=item f

Do not sort the output

=item F

put a '/' at the end of directories


=back


=cut

sub listDirectory {
  my $self = shift;
  return $self->{LFN_DB}->listDirectory(@_);
}

#
# createDirectory ($lfn, [$gowner, [$perm, [$replicated, [$table]]]])
#
sub createDirectory {
  my $self = shift;
  return $self->{LFN_DB}->createDirectory(@_);
}

sub createRemoteDirectory {
  my $self = shift;
  return $self->{LFN_DB}->createRemoteDirectory(@_);
}

sub removeDirectory {
  my $self = shift;
  return $self->{LFN_DB}->removeDirectory(@_);
}

sub tabCompletion {
  my $self = shift;
  $self->{LFN_DB}->tabCompletion(@_);
}

=item C<copyDirectory($source, $target)>

This subroutine copies a whole directory. It checks if part of the directory is in a different database

=cut

sub copyDirectory {
  my $self = shift;
  return $self->{LFN_DB}->copyDirectory(@_);
}

=item C<moveEntries($lfn, $toTable)>

This function moves all the entries under a directory to a new table
A new table is always created.

Before calling this function, you have to be already in the right database!!!
You can make sure that you are in the right database with a call to checkPermission

=cut

sub moveEntries {
  moveLFNs(@_);
}

sub moveLFNs {
  my $self = shift;
  return $self->{LFN_DB}->moveLFNs(@_);
}

sub moveGUIDs {
  my $self = shift;
  return $self->{GUID_DB}->moveGUIDs(@_);
}
##############################################################################
##############################################################################
sub addUser {
  my $self    = shift;
  my $user    = shift;
  my $group   = shift;
  my $db_user = $user;

  if ($self->{LFN_DB}->{DRIVER} =~ /Oracle/) {
    $db_user = $self->{LFN_DB}->{ORACLE_USER};
  }

  $self->{LFN_DB} or $self->info("Not connected to the database") and return;

  $self->{LFN_DB}->insertIntoGroups($user, $group, 1);

  return 1;
}

sub getNewDirIndex {
  my $self = shift;

  $self->lock("CONSTANTS");

  my ($dir) = $self->queryValue("SELECT value from CONSTANTS where name='MaxDir'");
  $dir++;

  $self->update("CONSTANTS", {value => $dir}, "name='MaxDir'");
  $self->unlock();

  $self->info("New table number: $dir");

  $self->{LFN_DB}->checkLFNTable($dir)
    or $self->info("Error checking the tables $dir")
    and return;

  return $dir;
}

#
#Returns the name of the file of a path
#
sub _basename {
  my $self  = shift;
  my ($arg) = @_;
  my $pos   = rindex($arg, "/");

  ($pos < 0) and return ($arg);

  return (substr($arg, $pos + 1));
}

sub deleteLink {
  my $self     = shift;
  my $parent   = shift;
  my $basename = shift;
  my $newpath  = shift;

  $self->deleteDirEntry($parent, $basename);
  $self->deleteFromD0Like($newpath);
}

### Groups functions

sub getUserid {
  my $self = shift;
  return $self->{LFN_DB}->getUserid(@_);
}

sub getUserGroups {
  my $self = shift;
  return $self->{LFN_DB}->getUserGroups(@_);
}

sub checkUserGroup {
  my $self = shift;
  return $self->{LFN_DB}->checkUserGroup(@_);
}

sub getAllFromGroups {
  my $self = shift;
  return $self->{LFN_DB}->getAllFromGroups(@_);
}

sub insertIntoGroups {
  my $self = shift;
  return $self->{LFN_DB}->insertIntoGroups(@_);
}

sub deleteUser {
  my $self = shift;
  my $user = shift
    or $self->{LOGGER}->error("Catalogue", "In deleteUser user is missing")
    and return;

  $DEBUG and $self->debug(2, "In deleteUser deleting entries with user $user from GROUPS table");
  $self->delete("GROUPS", "Username='$user'");
}

###	Environment functions

sub insertEnv {
  my $self = shift;
  my $user = shift
    or $self->{LOGGER}->error("Catalogue", "In insertEnv user is missing")
    and return;
  my $curpath = shift
    or $self->{LOGGER}->error("Catalogue", "In insertEnv current path is missing")
    and return;

  $DEBUG and $self->debug(2, "In insertEnv deleting old environment");
  $self->delete("ENVIRONMENT", "userName='$user'")
    or $self->{LOGGER}->error("Catalogue", "Cannot delete old environment")
    and return;

  $DEBUG and $self->debug(2, "In insertEnv inserting new environment");
  $self->insert("ENVIRONMENT", {userName => $user, env => "pwd $curpath"})
    or $self->{LOGGER}->error("Catalogue", "Cannot insert new environment")
    and return;

  1;
}

sub getEnv {
  my $self = shift;
  my $user = shift
    or $self->{LOGGER}->error("Catalogue", "In getEnv user is missing")
    and return;

  $DEBUG and $self->debug(2, "In insertEnv fetching environment for user $user");
  $self->queryValue("SELECT env FROM ENVIRONMENT WHERE userName='$user'");
}

#	TAG functions

# quite complicated manoeuvers in Catalogue/Tag.pm - f_addTagValue
# difficult to merge with the others
#sub insertDirtagVarsFileValuesNew {
sub insertTagValue {
  my $self = shift;
  return $self->{LFN_DB}->insertTagValue(@_);
}

sub getTags {
  my $self = shift;
  return $self->{LFN_DB}->getTags(@_);
}

sub cleanupTagValue {
  my $self = shift;
  return $self->{LFN_DB}->cleanupTagValue(@_);
}

sub getFieldsFromTagEx {
  my $self = shift;
  return $self->{LFN_DB}->getFieldsFromTagEx(@_);
}

sub getTagNamesByPath {
  my $self = shift;
  return $self->{LFN_DB}->getTagNamesByPath(@_);
}

sub getAllTagNamesByPath {
  my $self = shift;
  return $self->{LFN_DB}->getAllTagNamesByPath(@_);
}

sub getFieldsByTagName {
  my $self = shift;
  return $self->{LFN_DB}->getFieldsByTagName(@_);
}

sub getTagTableName {
  my $self = shift;
  return $self->{LFN_DB}->getTagTableName(@_);
}

sub deleteTagTable {
  my $self = shift;
  return $self->{LFN_DB}->deleteTagTable(@_);
}

sub insertIntoTag0 {
  my $self = shift;
  return $self->{LFN_DB}->insertIntoTag0(@_);
}

=item getDiskUsage($lfn)

Gets the disk usage of an entry (either file or directory)

=cut

sub getDiskUsage {
  my $self = shift;
  $self->{LFN_DB}->getDiskUsage(@_);
}

sub selectTable {
  return selectLFNDatabase(@_);
}

sub selectLFNDatabase {
  my $self = shift;

  my $db = $self->{LFN_DB}->selectTable(@_) or return;
  $self->{LFN_DB} = $db;
  return $db;
}

sub getLFNfromGUID {
  my $self = shift;
  return $self->{GUID_DB}->getLFNfromGUID(@_);
}

sub getPathPrefix {
  my $self = shift;
  $self->{LFN_DB}->getPatchPrefix(@_);
}

sub findLFN() {
  my $self = shift;
  return $self->{LFN_DB}->findLFN(@_);
}

sub setExpire {
  my $self = shift;
  return $self->{LFN_DB}->setExpire(@_);
}

sub close {
  my $self = shift;
  $self->{LFN_DB}->close();
  $self->{GUID_DB}->close();

}

sub destroy {
  my $self = shift or return;

  $self->{LFN_DB}  and $self->{LFN_DB}->destroy();
  $self->{GUID_DB} and $self->{GUID_DB}->destroy();

  #  $self->SUPER::destroy();
}

sub getAllReplicatedData {
  my $self = shift;
  my $info = $self->{LFN_DB}->getAllReplicatedData()
    or return;
  my $info2 = $self->{GUID_DB}->getAllReplicatedData()
    or return;
  foreach (keys %$info2) {
    $info->{$_} = $info2->{$_};
  }

  return $info;
}

sub setAllReplicatedData {
  my $self = shift;
  $self->{LFN_DB}->setAllReplicatedData(@_)  or return;
  $self->{GUID_DB}->setAllReplicatedData(@_) or return;
  return 1;
}

sub reconnect {
  my $self = shift;
  $self->{LFN_DB}->reconnect(@_);
}

sub setSEio {
  my $self          = shift;
  my $options       = shift;
  my $site          = shift;
  my $name          = shift;
  my $seioDaemons   = shift;
  my $seStoragePath = shift;
  my $SEName        = "$self->{CONFIG}->{ORG_NAME}::${site}::$name";
  my $SEnumber      = $self->{LFN_DB}->queryValue("SELECT seNumber from SE where upper(seName)=upper('$SEName')");

  #Check that the SE exists;
  if (!$SEnumber) {
    $self->info("The se $SEName does not exist!", 1);
    return;
  }

  if (
    !$self->{LFN_DB}->executeInAllDB(
      "update", "SE",
      {seName => $SEName, seStoragePath => $seStoragePath, seioDaemons => $seioDaemons},
      "upper(seName)=upper('$SEName')"
    )
    ) {
    $self->info("Error updating $SEName with seStoragePath $seStoragePath & seioDaemons $seioDaemons");
    return;
  }
  return 1;
}

sub getSENumber {
  my $self = shift;
  my $se   = shift;
  return $self->{LFN_DB}
    ->queryValue("SELECT seNumber from SE where upper(seName)=upper(?)", undef, {bind_values => [$se]});
}

sub getSEio {
  my $self    = shift;
  my $options = shift;
  my $site    = shift;
  my $name    = shift;
  my $SEName  = "$self->{CONFIG}->{ORG_NAME}::${site}::$name";
  my $SEio    = $self->{LFN_DB}->queryRow("SELECT * from SE where upper(seName)=upper('$SEName')");
  return $SEio;
}

sub getSENameFromNumber {
  my $self   = shift;
  my $number = shift;
  return $self->{LFN_DB}->queryValue("SELECT seName from SE where seNumber=?", undef, {bind_values => [$number]});
}

sub addSE {
  my $self    = shift;
  my $options = shift;
  my $site    = shift;
  my $name    = shift;

  my $addToTables = 1;
  my $SEName      = "$self->{CONFIG}->{ORG_NAME}::${site}::$name";
  my $SEnumber    = $self->{LFN_DB}->queryValue("SELECT seNumber from SE where seName='$SEName'");

  #Check that the SE doesn't exist;
  if ($SEnumber) {
    if ($options =~ /p/) {
      $addToTables = 0;
    } else {
      $self->info("The se $SEName already exists!!", 1);
      return;
    }
  }

  if ($addToTables) {

    #First, let's create the database
    $SEnumber = 1;
    my $max = $self->{LFN_DB}->queryValue("SELECT max(seNumber)+1 FROM SE");
    ($max) and $SEnumber = $max;

    $self->info("Adding the new SE $SEName with $SEnumber");

    if (!$self->{LFN_DB}->executeInAllDB("insert", "SE", {seName => $SEName, seNumber => $SEnumber})) {
      $self->info("Error adding the entry");
      $self->{LFN_DB}->executeInAllDB("delete", "SE", "upper(seName)=upper('$SEName') and seNumber=$SEnumber");
      return;
    }
  }

  $self->info("Entry Added!!!");

  return $SEnumber;
}

sub removeSE {
  my $self   = shift;
  my $sename = shift;
  $self->info("Removing the se $sename from the database");

  $self->{LFN_DB}->executeInAllDB("delete", "SE", "UPPER(seName)=UPPER('$sename')");
  return 1;
}

sub describeTable {
  my $self = shift;
  $self->{LFN_DB}->describeTable(@_);
}

sub setUserGroup {
  my $self = shift;
  $self->debug(1, "Let's change the userid ");
  $self->{LFN_DB}->setUserGroup(@_);
  $self->{GUID_DB}->setUserGroup(@_);
  return 1;
}

sub addHost {
  my $self      = shift;
  my $host      = shift;
  my $driver    = shift;
  my $db        = shift;
  my $org       = (shift or "");
  my $hostIndex = $self->getHostIndex($host, $db, $driver);

  if ($hostIndex) {
    print STDERR "Error: $db in $host already exists!!\n";
    return;
  }

  $hostIndex = $self->{LFN_DB}->getMaxHostIndex + 1;

  $self->info("Trying to connect to $db in $host...");
  my ($oldHost, $oldDB, $oldDriver) = ($self->{HOST}, $self->{DB}, $self->{DRIVER});

  my $replicatedInfo = $self->getAllReplicatedData()
    or $self->info("Error getting the info from the database")
    and return;

  $self->debug(1, "Connecting to new database ($host $db $driver)");
  my $oldConfig = $self->{CONFIG};
  my $newConfig;
  if ($org) {
    $newConfig = $self->{CONFIG}->Reload({"organisation", $org});
    $newConfig or $self->info("Error gettting the new configuration") and return;

    $self->{CONFIG} = $newConfig;
  }

  if (!$self->reconnect($host, $db, $driver)) {
    $self->info("Error: not possible to connect to $driver $db in $host");
    $self->reconnect($oldHost, $oldDB, $oldDriver);
    $newConfig and $self->{CONFIG} = $oldConfig;
    return;
  }
  $self->{SCHEMA} = $db;
  $self->{SCHEMA} =~ s/(.+):(.+)/$2/i;
  if (!$org) {
    $self->createCatalogueTables({reconnected => 1});

    #Now, we have to fill in the tables
    $self->setAllReplicatedData($replicatedInfo) or return;

    $self->{LFN_DB}->insertHost($hostIndex, $host, $db, $driver);

  }

  #in the old nodes, add the new link
  foreach my $rtempHost (@{$replicatedInfo->{hosts}}) {
    $self->debug(1, "Connecting to database ($rtempHost->{address} $rtempHost->{db} $rtempHost->{driver})");
    $self->reconnect($rtempHost->{address}, $rtempHost->{db}, $rtempHost->{driver});
    $self->{LFN_DB}->insertHost($hostIndex, $host, $db, $driver, $org);
  }

  $self->debug(1, "Connecting to old database ($oldHost $oldDB $oldDriver)");
  $self->reconnect($oldHost, $oldDB, $oldDriver);
  $self->info("Host added!!");
  return 1;
}

sub getNumberOfEntries {
  my $self  = shift;
  my $entry = shift;
  if (defined $entry->{guidTime}) {
    $self->debug(1, "Getting the number of guids");
    return $self->{GUID_DB}->getNumberOfEntries($entry, @_);
  }
  return $self->{LFN_DB}->getNumberOfEntries($entry, @_);
}

sub getIndexHostFromGUID {
  my $self = shift;
  return $self->{GUID_DB}->getIndexHostFromGUID(@_);
}

sub checkLFN {
  my $self   = shift;
  my $dbname = shift;
  my $ctable = shift;

  $dbname
    and $dbname !~ /^$self->{DB}$/
    and return;
  $self->info("Checking the tables in $self->{DB}");

  my $db = $self->{LFN_DB};
  $db or $self->info("Error connecting to $db") and next;

  my $tables = $db->queryColumn('select tablename from INDEXTABLE order by 1', undef, undef);
  foreach my $t (@$tables) {
          $ctable
      and $ctable !~ /^L${t}L$/
      and $self->info("Skipping table L${t}L")
      and next;
    if (
      $db->queryValue(
"select 1 from (select max(ctime) ctime, count(*) counter from L${t}L) a left join  LL_ACTIONS on tablenumber=? and action='STATS' where extra is null or extra<>counter or time is null or time<ctime",
        undef,
        {bind_values => [$t]}
      )
      ) {
      $self->info("We have to update the table $t");
      $db->updateStats($t);
    }
  }
  return 1;
}

sub checkOrphanGUID {
  my $self = shift;
  $self->debug(1, "Checking orphanguids in the database");

  my $db = $self->{GUID_DB}
    or return;
  my $tables = $db->query("select * from GL_ACTIONS where action='TODELETE'");
  foreach my $table (@$tables) {
    $self->info("Doing the table $table->{tableNumber}");
    $db->checkOrphanGUID($table->{tableNumber}, @_);
  }
  $db->do(
"delete from TODELETE  using TODELETE join SE s on TODELETE.senumber=s.senumber where sename='no_se' and pfn like 'guid://%'"
  );

  return 1;
}

sub optimizeGUIDtables {
  my $self = shift;

  $self->info("Let's optimize the guid tables");

  my $db = $self->{GUID_DB} or return;

  my $tables = $db->query("SELECT tableName, guidTime from GUIDINDEX", undef, undef);
  foreach my $info (@$tables) {
    my $table = "G$info->{tableName}L";
    $self->info("  Checking the table $table");
    my $number = $db->queryValue("select count(*) from $table");
    $self->info("There are $number entries");
    my $done = 0;
    while ($number > 3000000) {
      $self->info("There are more than 3M ($number) ! Splitting the table");
      my $guid =
        $db->queryRow("select guidid, binary2string(guid) guid from $table order by 1 desc limit 1 offset 2000000");
      $guid->{guid} or next;
      $self->info("We have to split according to $guid->{guid}");
      $db->moveGUIDs($guid->{guid}, "f") or last;
      $self->info("Let's count again");
      $number = $db->queryValue("select count(*) from $table");
      $done   = 1;
    }
    $done and $db->checkGUIDTable($table);
    if ($number < 1000000) {
      $self->info("There are less than 1M. Let's merge with the previous (before $info->{guidTime})");
      $self->optimizeGUIDtables_removeTable($info, $db, $table);
    }
  }
  return 1;
}

sub optimizeGUIDtables_removeTable {
  my $self  = shift;
  my $info  = shift;
  my $db    = shift;
  my $table = shift;

  defined $info->{guidTime} or return 1;

  my $previousGUID = $info->{guidTime};
  $previousGUID =~ s/.........$//;
  $previousGUID = sprintf("%s%09X", $previousGUID, hex(substr($info->{guidTime}, -9)) - 1);

  ($previousGUID eq "FFFFFFFF")
    and $self->info("This is the first table")
    and return 1;
  my $t = $db->queryRow("select * from GUIDINDEX where guidTime<? order by guidTime desc limit 1",
    undef, {bind_values => [$previousGUID]});

  ($table eq "G$t->{tableName}L")
    and $self->info("Same table?? :(")
    and return;

  my $info2   = $db->query("describe $table");
  my $columns = "";
  foreach my $c (@$info2) {
    $columns .= "$c->{Field},";
  }
  $columns =~ s/guidid,//i;
  $columns =~ s/,$//;
  my $entries = $db->queryValue("select count(*) from  G$t->{tableName}L");
  if ($entries > 2000000) {
    $self->info("The previous table has too many entries");
    return;
  }

  $self->info("This is in the same database. Tables $table and G$t->{tableName}L");
  $db->renumberGUIDtable("", $table);
  $db->renumberGUIDtable("", "G$t->{tableName}L");
  $db->lock(
"$table write, G$t->{tableName}L write, ${table}_PFN write, ${table}_REF write, G$t->{tableName}L_PFN write, G$t->{tableName}L_REF"
  );
  my $add = $db->queryValue("select max(guidid) from G$t->{tableName}L") || 0;
  $db->do(
    "insert into G$t->{tableName}L_PFN  ( pfn,seNumber,guidId ) select  pfn,seNumber, guidId+$add from ${table}_PFN");
  $db->do("insert into G$t->{tableName}L_REF  (lfnRef,guidId ) select  lfnRef, guidId+$add from ${table}_REF");

  $db->do("insert into G$t->{tableName}L  ($columns, guidId ) select  $columns, guidId+$add from ${table}");
  $self->info("And now, the index  $info->{guidTime}");
  $db->unlock();
  $db->deleteFromIndex("guid", $info->{guidTime});

  return 1;
}

sub getDF {
  my $self   = shift;
  my $sename = shift;
  my $opt    = shift;

  my $query =
"select *, if(size>0,if(usedSpace/size<0, 0, floor(100* usedSpace/size)),0) used  from SE a join SE_VOLUMES b  on a.seName=b.seName";
  my $bind = [];
  if ($sename) {
    $query .= " where a.seName=?";
    push @$bind, $sename;
  }
  my $info = $self->{LFN_DB}->{FIRST_DB}->query($query, undef, {bind_values => $bind});
  return $info;

}

sub masterSE_list {
  my $self   = shift;
  my $sename = shift;

  #If this option is given, give back all the guids
  $self->info("The options are @_");
  my $guids = grep (/^-guid$/, @_);
  my $senumber = $self->getSENumber($sename)
    or $self->info("Error getting the se number of $sename")
    and return;

  my $info = {
    referenced => 0,
    replicated => 0,
    broken     => 0
  };

  $guids and $info->{guids} = {referenced => [], replicated => [], broken => []};

  my $method = "queryValue";
  my $select = "count(*)";
  if ($guids) {
    $method = "queryColumn";
    $select = "binary2string(guid)";
  }
  my $db = $self->{GUID_DB} or return;

  my $tables = $db->queryColumn("SELECT tableName from GUIDINDEX", undef, undef);
  foreach my $table (@$tables) {
    $table = "G${table}L" or return;
    my $referenced = $db->$method(
      "select $select from $table join 
      ${table}_PFN p  using (guidid) join ${table}_REF r using (guidid)
      where  p.senumber=? ", undef, {bind_values => [$senumber]}
    );

    my $broken = $db->$method(
      "select $select from $table join 
      ${table}_PFN p  using (guidid) left join ${table}_REF r using (guidid)
      where  p.senumber=? and r.guidid is null",
      undef, {bind_values => [$senumber]}
    );
    my $replicated = $db->$method(
      "select $select from (select guid from $table join
      ${table}_PFN p using (guidid) join ${table}_PFN p2 using (guidid)
      where p.senumber=? and p2.senumber!= p.senumber group by guidid) a", undef, {bind_values => [$senumber]}
    );
    if ($guids) {
      $info->{broken}     += $#$broken + 1;
      $info->{replicated} += $#$replicated + 1;
      $info->{referenced} += $#$referenced + 1;
      $info->{guids}->{broken} = [ @$broken, @{$info->{guids}->{broken}} ];
    } else {
      $info->{broken}     += $broken;
      $info->{replicated} += $replicated;
      $info->{referenced} += $referenced;
    }
    $self->info("After $table, $info->{referenced},  $info->{broken} and $info->{replicated}");
  }
  return $info;
}

sub masterSE_getFiles {
  my $self           = shift;
  my $sename         = shift;
  my $previous_table = shift || "";
  my $limit          = shift;
  my $options        = shift || {};

  my $previous_host;
  $previous_table =~ s/^(\d+)_// and $previous_host = $1;
  my $senumber = $self->getSENumber($sename)
    or $self->info("Error getting the se number of $sename")
    and return;

  my $return = [];

  my $query = "select binary2string(g.guid)guid,p.pfn  ";
  $options->{md5} and $query .= ", g.md5 ";

  #Let's skip all the hosts that we have already seen
  my $db = $self->{LFN_DB};
  my $tables = $db->queryColumn("SELECT tableName from GUIDINDEX order by 1", undef, undef);
  foreach my $table (@$tables) {
    $table = "G${table}L";
    $previous_table and $previous_table !~ /^$table$/ and next;

    #      $table =~ /G46L/ or next;
    if ($previous_table) {
      $previous_table = "";
    }
    my $endquery = "";
    if ($options->{unique}) {
      $self->info("Checking that the file is not replicated");
      $endquery =
"and not exists (select 1 from ${table}_PFN p2 where p2.senumber!=p.senumber and p2.guidid=p.guidid) group by guid";
    }
    if ($options->{replicated}) {
      $endquery =
        "and exists (select 1 from ${table}_PFN p2 where p2.senumber!=p.senumber and p2.guidid=p.guidid) group by guid";
    }
    my $entries = [];
    if ($options->{lfn}) {
      $self->debug(1, "Getting the lfn of the files");
      my $ref = $db->query(
"select lfnRef, db, a.lfn  from (select  distinct lfnRef  from  ${table}_REF join  ${table}_PFN p using (guidid) where p.senumber=?) a join  HOSTS h join INDEXTABLE a using (hostindex)   where lfnRef like concat(h.hostindex, '_%') and lfnRef=concat(a.hostIndex,'_', a.tableName) ",
        undef,
        {bind_values => [$senumber]}
      );
      foreach my $entry (@$ref) {
        my ($host, $lfnTable) = split(/_/, $entry->{lfnRef});
        my $dd = $db->query(
"$query, concat(?,lfn) lfn  from $table g join  ${table}_PFN p  using (guidid) join $entry->{db}.L${lfnTable}L l using (guid) where p.senumber=? $endquery",
          undef,
          {bind_values => [ $entry->{lfn}, $senumber ]}
        );
        print "    doing $table and $entry->{lfnRef} $#$dd\n";

        #	  my $dd=[];
        $entries = [ @$entries, @$dd ];
      }
    } else {
      $entries = $db->query("$query from  $table g join  ${table}_PFN p  using (guidid) where p.senumber=? $endquery",
        undef, {bind_values => [$senumber]});
    }
    $return = [ @$return, @$entries ];
    if ($#$return > $limit) {
      $self->info("Let's return now before putting more entries");
      return ($return, "_$table");
    }
  }
  $self->info("We have seen all the entries");
  return ($return, "");
}

sub calculateBrokenLFN {
  my $self    = shift;
  my $table   = shift;
  my $db      = shift;
  my $options = shift;

  my $extratable = "";
  $self->info("Calculating all the broken links in $table->{lfn}");

  my $GUIDList = $db->getPossibleGuidTables($table->{tableName});
  my $t        = "L$table->{tableName}L";
  $db->checkLFNTable($table->{tableName});
  $db->do("truncate table ${t}_broken");
  $db->do("insert into ${t}_broken  select entryId from  $t where type='f'");
  foreach my $entry (@$GUIDList) {
    $options->{nopfn} and $extratable = "join  $entry->{db}.G$entry->{tableName}L_PFN using (guidid)";
    $db->do(
"delete from  ${t}_broken using  ${t}_broken join $t using (entryId) join $entry->{db}.G$entry->{tableName}L using (guid) $extratable"
    );

  }
  return 1;
}

sub getBrokenLFN {
  my $self    = shift;
  my $options = shift;
  $self->info("Getting all the broken lfn @_");

  my $dir = shift || "";

  my $all = [];
  my $allEntries;
  if ($dir) {
    $self->info("Doing only $dir");
    $allEntries = $self->{LFN_DB}->getHostsForEntry($dir);
  }
  my $db = $self->{LFN_DB} or return;

  my $tables;
  if ($allEntries) {
    foreach my $c (@$allEntries) {
      push @$tables, $c;
    }
  } else {
    $tables = $db->query("SELECT tableName,lfn from INDEXTABLE", undef, undef);
  }
  for my $t (@$tables) {
    $db->checkLFNTable($t->{tableName});
    $self->info("Checking the table $t->{tableName}");
    $options->{calculate} and $self->calculateBrokenLFN($t, $db, $options);
    my $like = "";
    my $bind = [ $t->{lfn} ];
    if ($dir) {
      $like = "where concat('$t->{lfn}',lfn) like concat(?,'%')";
      push @$bind, $dir;
    }
    my $entries = $db->queryColumn(
      "SELECT concat(?,lfn) from L$t->{tableName}L join  L$t->{tableName}L_broken using (entryId) $like ",
      undef, {bind_values => $bind});
    foreach my $e (@$entries) {
      $self->info($e, 0, 0);
    }
    push @$all, @$entries;
  }
  return $all;
}

=head1 SEE ALSO

AliEn::Database

=cut

1;

