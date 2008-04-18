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
$DEBUG=0;

=head1 SYNOPSIS

  use AliEn::Database::Catalogue;

  my $catalogue=AliEn::Database::Catalogue->new() or exit;


=head1 METHODS

=over

=cut


sub preConnect {
  my $self=shift;
  foreach ('HOST', 'DRIVER', 'DB'){
    $self->{$_} or $self->{$_}=1;
  }

  $self->debug(1,"We don't really need the preconnect...");
  return 1;
}

sub _connect{
  my $self=shift;
  $self->debug(1, "The catalogue itself doesn't have to connect....");
  return 1;
}

sub initialize {
  my $self=shift;
  my $opt1={};
  my $opt2={};
  foreach (keys  %{$self}){
    $opt2->{$_}=$opt1->{$_}=$self->{$_};
  }
  foreach ('HOST', 'DRIVER', 'DB'){
    if ($self->{$_} eq "1"){
      delete $opt1->{$_};
      delete $opt2->{$_};
    }
  }

  $self->{LFN_DB}=AliEn::Database::Catalogue::LFN->new($opt1, @_) or return;
  $self->{GUID_DB}=AliEn::Database::Catalogue::GUID->new($opt2,@_) or return;

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

  my $options=shift || {};
  $self->{LFN_DB}->createCatalogueTables() or return;
  my @args;
  $options->{reconnected} and push @args, $self->{LFN_DB};
  $self->{GUID_DB}->createCatalogueTables(@args) or return;
  
  return 1;
}

sub getAllInfoFromLFN{
  my $self=shift;
  return $self->{LFN_DB}->getAllInfoFromLFN(@_);
}

sub getAllInfoFromGUID{
  my $self=shift;
  return $self->{GUID_DB}->getAllInfoFromGUID(@_);
}
sub getAllExtendedInfoFromLFN{
  my $self=shift;

  my $info=$self->{LFN_DB}->getAllInfoFromLFN({method=>"queryRow"}, @_)
    or return;

  $info or $self->info("The entry doesn't exist") and return;

  my $info2=$self->{GUID_DB}->getAllInfoFromGUID({pfn=>1},$info->{guid})
    or return;
  $info->{guidInfo}=$info2;
  return $info;
}



=item c<existsEntry($lfn)>

This function receives an lfn, and checks if it exists in the catalogue. It checks for lfns like '$lfn' and '$lfn/', and, in case the entry exists, it returns the name (the name has a '/' at the end if the entry is a directory)

=cut


sub existsEntry{
  return existsLFN(@_);
}
sub existsLFN{
  my $self=shift;
  return $self->{LFN_DB}->existsLFN(@_);
}


=item C<getHostsForEntry($lfn)>

This function returns a list of all the possible hosts and tables that might contain entries of a directory

=cut

sub getHostsForLFN{
  my $self=shift;
  return $self->{LFN_DB}->getHostsForEntry(@_);
}

=item C<getSEListFromFile($lfn)>

Retrieves the list of SE that have a copy of the lfn 

=cut
sub renumberLFNtable{
  my $self=shift;
  return $self->{LFN_DB}->renumberLFNtable(@_)
}
sub getSEListFromFile{
  return getSEListFromLFN(@_);
}

sub getSEListFromLFN {
  my $self=shift;
  my $lfn=shift;
  my $guid=$self->{LFN_DB}->getGUIDFromLFN($lfn) or return;
  return $self->getSEListFromGUID($guid, @_);
}

sub getSEListFromGUID{
  my $self=shift;
  return $self->{GUID_DB}->getSEList(@_);
}


=item C<deleteMirrorFromFile($lfn, $seName)>

Deletes a mirror from a file

=cut

sub deleteMirrorFromGUID {
  my $self=shift;
  return $self->{GUID_DB}->deleteMirrorFromGUID(@_);
}

sub deleteMirrorFromLFN {
  my $self=shift;
  my $lfn=shift;
  my $guid=$self->{LFN_DB}->getGUIDFromLFN($lfn)
    or $self->info("Error getting the guid of $lfn") and return;
  return $self->{GUID_DB}->deleteMirrorFromGUID($guid, @_);
}

=item C<insertMirrorFromFile($lfn, $seName)>

Inserts mirror of a file

=cut

sub insertMirrorToGUID{
  my $self=shift;
  return $self->{GUID_DB}->insertMirrorToGUID(@_);
}

sub insertMirrorFromFile{
  return insertMirrorToLFN(@_);
}
sub insertMirrorToLFN {
  my $self=shift;
  my $lfn=shift;
  my $guid=$self->{LFN_DB}->getGUIDFromLFN($lfn);
  return $self->{GUID_DB}->insertMirrorToGUID($guid, @_);
}



sub do{
  my $self=shift;
  return $self->{LFN_DB}->do(@_);
}

sub query{
  my $self=shift;
  return $self->{LFN_DB}->do(@_);
}

sub existsTable{
  my $self=shift;
  return $self->{LFN_DB}->existsTable(@_);
}

sub createCollection{
  my $self=shift;
  $self->{GUID_DB}->insertGUID("", @_) or return;
  return $self->{LFN_DB}->createCollection(@_);
}

sub addFileToCollection{
  my $self=shift;
  return $self->{LFN_DB}->addFileToCollection(@_);
}

sub getInfoFromCollection{
  my $self=shift;
  return $self->{LFN_DB}->getInfoFromCollection(@_);
}

sub removeFileFromCollection{
  my $self=shift;
  return $self->{LFN_DB}->removeFileFromCollection(@_);
}


=item C<createFile($hash)>

Adds a new file to the database. It receives a hash with the following information:



=cut

sub createFile {
  my $self=shift;
  my $options=shift || "";
  $self->debug(2, "In catalogue, createFile");
  if ($options =~ /k/){
    $self->debug(4, "The GUID is supposed to be registered");
    $self->{GUID_DB}->increaseReferences($options, @_) or return;
  } else{
    $self->{GUID_DB}->insertGUID($options, @_) or return;
  }
  my $done=$self->{LFN_DB}->createFile($options, @_) or return;
  $self->info("File(s) inserted");
  return $done;
}

sub getParentDir {
  my $self=shift;
  return $self->{LFN_DB}->getParentDir(@_);
}

sub updateFile {
  return updateLFN(@_);
}

sub updateLFN{
  my $self=shift;
  my $lfn=shift;
  my $update=shift;
  if ($update->{size} or $update->{md5} or $update->{se} ){
    my $guid=$self->{LFN_DB}->getGUIDFromLFN($lfn) or return;
    
    #First, let's update the information of the guid
    $self->{GUID_DB}->updateOrInsertGUID($guid,$update, @_) or 
      $self->info("Error updating the guid") and return;
  }
  if (!$self->{LFN_DB}->updateLFN($lfn,$update, )){
    $self->info("We should undo the change");
    return;
  }
  return 1;
}

sub deleteFile {
  my $self=shift;
  return $self->{LFN_DB}->deleteFile(@_);
}
sub getLFNlike {
  my $self=shift;
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
  my $self=shift;
  return $self->{LFN_DB}->listDirectory(@_);
}

#
# createDirectory ($lfn, [$gowner, [$perm, [$replicated, [$table]]]])
#
sub createDirectory {
  my $self=shift;
  return $self->{LFN_DB}->createDirectory(@_);
}
sub createRemoteDirectory {
  my $self=shift;
  return $self->{LFN_DB}->createRemoteDirectory(@_);
}

sub removeDirectory {
  my $self=shift;
  return $self->{LFN_DB}->removeDirectory(@_);
}

sub tabCompletion {
  my $self=shift;
  $self->{LFN_DB}->tabCompletion(@_);
}

=item C<copyDirectory($source, $target)>

This subroutine copies a whole directory. It checks if part of the directory is in a different database

=cut

sub copyDirectory{
  my $self=shift;
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
  my $self=shift;
  return $self->{LFN_DB}->moveLFNs(@_);
}  

sub moveGUIDs {
  my $self=shift;
  return $self->{GUID_DB}->moveGUIDs(@_);
}
##############################################################################
##############################################################################
sub addUser {
  my $self=shift;
  my $user=shift;
  my $group=shift;
  my $passwd=shift;

  my $rhosts = $self->{LFN_DB}->getAllHosts();
  my $guidtables=$self->{GUID_DB}->query("SELECT distinct hostIndex, tableName from GUIDINDEX");
  my $guidOrdered={};
  foreach (@$guidtables){
    my @list=$_->{tableName};
    $guidOrdered->{$_->{hostIndex}} and push @list, @{$guidOrdered->{$_->{hostIndex}}};
    $guidOrdered->{$_->{hostIndex}}=\@list;
  }

  my $error=0;
  foreach my $rtempHost (@$rhosts) {
    print "Granting privileges for $user in $rtempHost->{db} (so far $error)\n";
    my ($db, $extra)=$self->{LFN_DB}->reconnectToIndex( $rtempHost->{hostIndex}, "", $rtempHost );
    $db or $self->info("Error reconnecting to $rtempHost->{hostIndex}") and $error=1 and next; 
    $db->grantExtendedPrivilegesToUser($db->{DB}, $user, $passwd);

    $db->insertIntoGroups($user, $group, 1);

    foreach my $table (@{$guidOrdered->{$rtempHost->{hostIndex}}}){
      $self->info("WE HAVE TO GIVE ACCESS TO $table");
      $db->grantPrivilegesToUser(["INSERT,DELETE,UPDATE on G${table}L",
				  "INSERT,DELETE,UPDATE on G${table}L_PFN"], $user, $passwd) or $error=1;
    }
  }
  $error and return;

  return 1;
}

sub grantPrivilegesToUser{
  my $self=shift;
  return $self->{LFN_DB}->grantPrivilegesToUser(@_);
}

sub grantBasicPrivilegesToUser {
  my $self = shift;
  my $db = shift
    or $self->{LOGGER}->error("Catalogue","In grantBasicPrivilegesToUser database name is missing")
      and return;
  my $user = shift
    or $self->{LOGGER}->error("Catalogue","In grantBasicPrivilegesToUser user is missing")
      and return;
  my $passwd = shift;

  $self->grantPrivilegesToUser(["EXECUTE ON *"], $user, $passwd)
    or return;

  my $rprivileges = ["SELECT ON $db.*",
		     "INSERT, DELETE ON $db.TAG0"];


  $DEBUG and $self->debug(2,"In grantBasicPrivilegesToUser granting privileges to user $user"); 
  $self->grantPrivilegesToUser($rprivileges, $user);
}

sub grantExtendedPrivilegesToUser {
  my $self = shift;
  my $db = shift
    or $self->{LOGGER}->error("Catalogue","In grantExtendedPrivilegesToUser database name is missing")
	and return;
  my $user = shift
    or $self->{LOGGER}->error("Catalogue","In grantExtendedPrivilegesToUser user is missing")
	and return;
  my $passwd = shift;

  $self->grantPrivilegesToUser(["SELECT ON $db.*"], $user, $passwd)
  	or return;

  my $rprivileges = [
		     "INSERT, DELETE  ON $db.TAG0",
#		     "INSERT, DELETE ON $db.FILES",
		     "INSERT, DELETE ON $db.ENVIRONMENT", 
#		     "INSERT ON $db.SE"
		     "EXECUTE ON *",
		     "INSERT, DELETE ON $db.G0L",
];

  $DEBUG and $self->debug(2,"In grantExtendedPrivilegesToUser granting privileges to user $user"); 
  $self->grantPrivilegesToUser($rprivileges, $user);
}


sub getNewDirIndex {
  my $self=shift;

  $self->lock("CONSTANTS");

  my ($dir) = $self->queryValue("SELECT value from CONSTANTS where name='MaxDir'");
  $dir++;
  
  $self->update("CONSTANTS", {value => $dir}, "name='MaxDir'");
  $self->unlock();

  $self->info( "New table number: $dir");

  $self->checkDLTable($dir) or 
    $self->info( "Error checking the tables $dir") and return;

  return $dir;
}


#
#Returns the name of the file of a path
#
sub _basename {
  my $self = shift;
  my ($arg) = @_;
  my $pos = rindex( $arg, "/" );

  ( $pos < 0 ) and    return ($arg);

  return ( substr( $arg, $pos + 1 ) );
}

sub deleteLink {
    my $self = shift;
    my $parent = shift;
    my $basename = shift;
    my $newpath = shift;

    $self->deleteDirEntry($parent, $basename);
    $self->deleteFromD0Like($newpath);
}

### Hosts functions

sub getFieldsFromHosts{
	my $self = shift;
	my $host = shift
		or $self->{LOGGER}->error("Catalogue","In getFieldsFromHosts host index is missing")
		and return;
	my $attr = shift || "*";

	$DEBUG and $self->debug(2,"In getFieldFromHosts fetching value of attributes $attr for host index $host");
	$self->queryRow("SELECT $attr FROM HOSTS WHERE hostIndex = '$host'");
}

sub getFieldFromHosts{
  my $self = shift;
  my $host = shift
    or $self->{LOGGER}->error("Catalogue","In getFieldFromHosts host index is missing")
      and return;
  my $attr = shift || "*";
  
  $DEBUG and $self->debug(2,"In getFieldFromHosts fetching value of attribute $attr for host index $host");
  $self->queryValue("SELECT $attr FROM HOSTS WHERE hostIndex = ?", undef, 
		    {bind_values=>[$host]});
}

sub getFieldsFromHostsEx {
  my $self = shift;
  my $attr = shift || "*";
  my $where = shift || "";

  $self->query("SELECT $attr FROM HOSTS $where");
}

sub getFieldFromHostsEx {
  my $self = shift;
  my $attr = shift || "*";
  my $where = shift || "";
  
  $self->queryColumn("SELECT $attr FROM HOSTS $where");
}

sub getHostIndex {
    my $self = shift;
    return $self->{LFN_DB}->getHostIndex(@_);
}
sub getIndexTable{
  my $self=shift;
  return $self->{LFN_DB}->getIndexTable(@_);
}
sub getIndexHost {
  my $self=shift;
  $self->{LFN_DB}->getIndexHost(@_);
}
sub getAllHosts {
  my $self = shift;
  return $self->{LFN_DB}->getAllHosts(@_);
}


sub updateHost {
  my $self = shift;
  return $self->{LFN_DB}->updateHost(@_);
}

sub deleteHost {
  my $self = shift;
  return $self->{LFN_DB}->deleteHost(@_);
}

### Groups functions

sub getUserid{
  my $self=shift;
  return $self->{LFN_DB}->getUserid(@_);
}

sub getUserGroups {
  my $self=shift;
  return $self->{LFN_DB}->getUserGroups(@_);
}

sub checkUserGroup{
  my $self = shift;
  return $self->{LFN_DB}->checkUserGroup(@_);
}

sub getAllFromGroups {
  my $self=shift;
  return $self->{LFN_DB}->getAllFromGroups(@_);
}

sub insertIntoGroups {
  my $self = shift;
  return $self->{LFN_DB}->insertIntoGroups(@_);
}

sub deleteUser {
  my $self = shift;
  my $user = shift
    or $self->{LOGGER}->error("Catalogue","In deleteUser user is missing")
      and return;
  
  $DEBUG and $self->debug(2,"In deleteUser deleting entries with user $user from GROUPS table");
  $self->delete("GROUPS","Username='$user'");
}



###	Environment functions

sub insertEnv {
  my $self = shift;
  my $user = shift
    or $self->{LOGGER}->error("Catalogue","In insertEnv user is missing")
      and return;
  my $curpath = shift
    or $self->{LOGGER}->error("Catalogue","In insertEnv current path is missing")
      and return;
  
  $DEBUG and $self->debug(2,"In insertEnv deleting old environment");
  $self->delete("ENVIRONMENT","userName='$user'") 
    or $self->{LOGGER}->error("Catalogue", "Cannot delete old environment")
      and return;
  
  $DEBUG and $self->debug(2,"In insertEnv inserting new environment");
  $self->insert("ENVIRONMENT",{userName=>$user,env=>"pwd $curpath"})
    or $self->{LOGGER}->error("Catalogue", "Cannot insert new environment")
      and return;

  1;
}

sub getEnv {
  my $self = shift;
  my $user = shift
    or $self->{LOGGER}->error("Catalogue","In getEnv user is missing")
      and return;

  $DEBUG and $self->debug(2,"In insertEnv fetching environment for user $user");
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

sub cleanupTagValue{
  my $self=shift;
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
  my $self=shift;
  return $self->{LFN_DB}->getTagTableName(@_);
}

sub deleteTagTable {
  my $self=shift;
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
  my $self=shift;
  $self->{LFN_DB}->getDiskUsage(@_);
}

sub selectDatabase{
  return selectLFNDatabase(@_);
}

sub selectLFNDatabase {
  my $self=shift;

  my $db=$self->{LFN_DB}->selectDatabase(@_) or return;
  $self->{LFN_DB}=$db;
  return $db;
}

sub getLFNfromGUID {
  my $self=shift;
  return $self->{LFN_DB}->getLFNfromGUID(@_);
}

sub getPathPrefix{
  my $self=shift;
  $self->{LFN_DB}->getPatchPrefix(@_);
}

sub findLFN() {
  my $self=shift;
  return $self->{LFN_DB}->findLFN(@_);
}

sub setExpire{
  my $self=shift;
  return $self->{LFN_DB}->setExpire(@_);
}

sub close{
  my $self=shift;
  $self->{LFN_DB}->close();
  $self->{GUID_DB}->close();

}

sub destroy {
  my $self=shift or return;

  $self->{LFN_DB} and $self->{LFN_DB}->destroy();
  $self->{GUID_DB} and $self->{GUID_DB}->destroy();
#  $self->SUPER::destroy();
}

sub getAllReplicatedData{
  my $self=shift;
  my $info=$self->{LFN_DB}->getAllReplicatedData() 
    or return;
  my $info2=$self->{GUID_DB}->getAllReplicatedData()
    or return;
  foreach (keys %$info2){
    $info->{$_}=$info2->{$_};
  }

  return $info;
}

sub setAllReplicatedData{
  my $self=shift;
  $self->{LFN_DB}->setAllReplicatedData(@_) or return;
  $self->{GUID_DB}->setAllReplicatedData(@_) or return;
  return 1;
}

sub reconnect{
  my $self=shift;
  $self->{LFN_DB}->reconnect(@_);
}


sub setSEio {
    my $self=shift;
    my $options=shift;
    my $site=shift;
    my $name=shift;
    my $seioDaemons=shift;
    my $seStoragePath=shift;
    my $SEName="$self->{CONFIG}->{ORG_NAME}::${site}::$name";
    my $SEnumber=$self->{LFN_DB}->queryValue("SELECT seNumber from SE where seName='$SEName'");

    #Check that the SE exists;
    if (!$SEnumber){
	$self->info("The se $SEName does not exist!", 1);
	return;
    }

    if (!$self->{LFN_DB}->executeInAllDB("update", "SE", {seName=>$SEName, seStoragePath=>$seStoragePath,seioDaemons=>$seioDaemons},"seName='$SEName'")) {
	$self->info("Error updating $SEName with seStoragePath $seStoragePath & seioDaemons $seioDaemons");	
	return;
    }
    return 1;
}

sub getSEio {
    my $self=shift;
    my $options=shift;
    my $site=shift;
    my $name=shift;
    my $SEName="$self->{CONFIG}->{ORG_NAME}::${site}::$name";
    my $SEio=$self->{LFN_DB}->queryRow("SELECT * from SE where seName='$SEName'");
    return $SEio;
}
sub getSENameFromNumber{
  my $self=shift;
  my $number=shift;
  return $self->{LFN_DB}->queryValue("SELECT seName from SE where seNumber=?", undef , {bind_values=>[$number]});
}

sub addSE{
  my $self=shift;
  my $options=shift;
  my $site=shift;
  my $name=shift;

  my $addToTables=1;
  my $SEName="$self->{CONFIG}->{ORG_NAME}::${site}::$name";
  my $SEnumber=$self->{LFN_DB}->queryValue("SELECT seNumber from SE where seName='$SEName'");

  #Check that the SE doesn't exist;
  if ($SEnumber){
    if ($options =~ /p/) {
      $addToTables=0;
    } else {
      $self->info("The se $SEName already exists!!", 1);
      return;
    }
  }
  my $dbName="se_".lc($SEName);
  $dbName =~ s{::}{_}g;

  if ($addToTables) {
    #First, let's create the database
    $SEnumber=1;
    my $max=$self->{LFN_DB}->queryValue("SELECT max(seNumber)+1 FROM SE");
    ($max) and $SEnumber=$max;
    
    $self->info("Adding the new SE $SEName with $SEnumber");
    
    if (!$self->{LFN_DB}->executeInAllDB("insert", "SE", {seName=>$SEName, seNumber=>$SEnumber})) {
      $self->info("Error adding the entry");
      $self->{LFN_DB}->executeInAllDB("delete", "SE", "seName='$SEName' and seNumber=$SEnumber");
      return;
    }
  }
  
  $self->debug(2, "Let's create the tables");
  
  if ($options=~ /d/){
    $self->info("Copying the data");
    $self->{LFN_DB}->executeInAllDB("do", "insert into $dbName.FILES (pfn, size, guid)  select pfn, size, guid from FILES2 where se='$SEName'")
  }
  $self->info("Entry Added!!!");
  
  return ($dbName, $SEnumber);
}


sub createTable {
  my $self=shift;
  my $host       = shift;
  my $db         = shift;
  my $driver     = shift;
  my $user       = shift;
  my $table      = shift;
  my $definition = shift;

  $self->info("Creating the table $table" );
  my $errorMessage="Error creating the new table \n";

  my $index=$self->getHostIndex($host, $db, $driver);
  $index or $self->info("Error getting the index of '$host', '$db', and '$driver'", 1) and return;

  my ($db2, $extra)=$self->{LFN_DB}->reconnectToIndex($index)
    or $self->info("Error reconnecting to the index $index", 1) and return;

  $db2->createTable($table, $definition)
    or $self->info("$errorMessage ($! $@)\n",1)
      and return;

  $db2->grantAllPrivilegesToUser($user, $db, $table)
    or $self->info("Error granting privileges on table $table for $user\n($! $@ $DBI::errstr\n",1 )
      and return;
  $self->info("Table created!!!!");
  return 1;
}

sub describeTable {
  my $self=shift;
  $self->{LFN_DB}->describeTable(@_);
}

sub setUserGroup{
  my $self=shift;
  $self->debug(1,"Let's change the userid ");
  $self->{LFN_DB}->setUserGroup(@_);
  $self->{GUID_DB}->setUserGroup(@_);
  return 1;
}


sub addHost {
  my $self=shift;
  my $host   = shift;
  my $driver = shift;
  my $db     = shift;
  my $org    =(shift or "");
  my $hostIndex = $self->getHostIndex ($host, $db, $driver);
  
  if ($hostIndex) {
    print STDERR "Error: $db in $host already exists!!\n";
    return;
  }

  $hostIndex = $self->{LFN_DB}->getMaxHostIndex + 1;

  $self->info( "Trying to connect to $db in $host...");
  my ( $oldHost, $oldDB, $oldDriver ) = (
					 $self->{HOST},
					 $self->{DB},
					 $self->{DRIVER}
					);
  
  my $replicatedInfo=$self->getAllReplicatedData()
    or $self->info("Error getting the info from the database") and return;


  $self->debug(1, "Connecting to new database ($host $db $driver)");
  my $oldConfig=$self->{CONFIG};
  my $newConfig;
  if ($org) {
    $newConfig=$self->{CONFIG}->Reload({"organisation", $org});
    $newConfig or $self->info( "Error gettting the new configuration") and return;

    $self->{CONFIG}=$newConfig;
  }

  if ( !$self->reconnect( $host, $db, $driver ) ) {
    $self->info("Error: not possible to connect to $driver $db in $host");
    $self->reconnect( $oldHost, $oldDB, $oldDriver );
    $newConfig and $self->{CONFIG}=$oldConfig;
    return;
  }
  if (!$org) {
    $self->createCatalogueTables({reconnected=>1});
    my  $addbh = new AliEn::Database::Admin();
    ($addbh)
      or $self->info("Error getting the Admin" ) and return;

    my $rusertokens = $addbh->getAllFromTokens("Username, password");
    $addbh->destroy();

    #also, grant the privileges for all the users
    foreach my $rtempUser (@$rusertokens) {
      $self->grantBasicPrivilegesToUser($db, $rtempUser->{Username}, $rtempUser->{password});
    }
    #Now, we have to fill in the tables
    $self->setAllReplicatedData($replicatedInfo) or return;

    $self->{LFN_DB}->insertHost($hostIndex, $host, $db, $driver);
    
  }
  
  #in the old nodes, add the new link
  foreach my $rtempHost (@{$replicatedInfo->{hosts}}) {
    $self->debug(1, "Connecting to database ($rtempHost->{address} $rtempHost->{db} $rtempHost->{driver})");
    $self->reconnect( $rtempHost->{address}, $rtempHost->{db}, $rtempHost->{driver} );
    $self->{LFN_DB}->insertHost($hostIndex, $host, $db, $driver, $org);
  }

  $self->debug(1, "Connecting to old database ($oldHost $oldDB $oldDriver)");
  $self->reconnect( $oldHost, $oldDB, $oldDriver );
  $self->info( "Host added!!");
  return 1;
}
=head1 SEE ALSO

AliEn::Database

=cut

1;


