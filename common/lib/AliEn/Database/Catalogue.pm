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

use AliEn::GUID;

=head1 NAME

AliEn::Database::Catalogue - database wrapper for AliEn catalogue

=head1 DESCRIPTION

This module interacts with a database of the AliEn Catalogue. The AliEn Catalogue can be distributed among several databases, each one with a different layout. In this basic layout, there can be several tables containing the entries of the catalogue. 

=cut

use vars qw(@ISA $DEBUG);

push @ISA, qw(AliEn::Database AliEn::Database::Catalogue::LFN AliEn::Database::Catalogue::GUID );
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

=item C<createCatalogueTables>

This methods creates the database schema in an empty database. The tables that this implemetation have are:
HOSTS, 

=cut

#
# Checking the consistency of the database structure
sub createCatalogueTables {
  my $self = shift;

  my $options = shift || {};
  $self->LFN_createCatalogueTables() or return;
  $self->GUID_createCatalogueTables() or return;

  return 1;
}


sub getAllExtendedInfoFromLFN {
  my $self = shift;
  
  my $info = $self->getAllInfoFromLFN({method => "queryRow"}, @_);
  

  $info or $self->info("The entry doesn't exist") and return;
  
  my $info2 = $self->getAllInfoFromGUID({pfn => 1}, $info->{guid})
    or return;
    
  $info->{guidInfo} = $info2;
  return $info;
}

=item c<existsEntry($lfn)>

This function receives an lfn, and checks if it exists in the catalogue. It checks for lfns like '$lfn' and '$lfn/', and, in case the entry exists, it returns the name (the name has a '/' at the end if the entry is a directory)

=cut

sub existsEntry {
  my $self=shift;
  return $self->existsLFN(@_);
}

=item C<getTablesForEntry($lfn)>

This function returns a list of all the possible hosts and tables that might contain entries of a directory

=cut

sub getHostsForLFN {
  my $self = shift;
  return $self->getTablesForEntry(@_);
}

=item C<getSEListFromFile($lfn)>

Retrieves the list of SE that have a copy of the lfn 

=cut

sub getSEListFromFile {
  return getSEListFromLFN(@_);
}

sub getSEListFromLFN {
  my $self = shift;
  my $lfn  = shift;
  my $guid = $self->getGUIDFromLFN($lfn) or return;
  return $self->getSEListFromGUID($guid, @_);
}

sub getSEListFromGUID {
  my $self = shift;
  return $self->getSEList(@_);
}


=item C<deleteMirrorFromFile($lfn, $seName)>

Deletes a mirror from a file

=cut

sub deleteMirrorFromLFN {
  my $self = shift;
  my $lfn  = shift;
  my $guid = $self->getGUIDFromLFN($lfn)
    or $self->info("Error getting the guid of $lfn")
    and return;
  return $self->deleteMirrorFromGUID($guid, $lfn, @_);
}

=item C<insertMirrorFromFile($lfn, $seName)>

Inserts mirror of a file

=cut

sub insertMirrorFromFile {
  return insertMirrorToLFN(@_);
}

sub insertMirrorToLFN {
  my $self = shift;
  my $lfn  = shift;
  my $guid = $self->getGUIDFromLFN($lfn);
  return $self->insertMirrorToGUID($guid, @_);
}


sub createCollection {
  my $self = shift;
  $self->insertGUID("", @_) or return;
  return $self->LFN_createCollection(@_);
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
    if (!$self->GUID_increaseReferences($options, @_)) {
      $self->insertGUID($options, @_) or return;
    }
  } elsif ($options =~ /k/) {
    $self->debug(4, "The GUID is supposed to be registered");
    $self->GUID_increaseReferences($options, @_) or return;
  } else {
  	$self->info("Let's insert the guid");
    $self->insertGUID($options, @_) or return;
  }
  my $done = $self->LFN_createFile($options, @_) or return;
  $self->info("File(s) inserted");
  return $done;
}

sub updateFile {
  return updateLFN(@_);
}

sub updateLFN {
  my $self   = shift;
  my $lfn    = shift;
  my $update = shift;
  if ($update->{size} or $update->{md5} or $update->{se}) {
    my $guid = $self->getGUIDFromLFN($lfn) or return;

    #First, let's update the information of the guid
    $self->updateOrInsertGUID($guid, $update, @_)
      or $self->info("Error updating the guid")
      and return;
  }
  if (!$self->LFN_updateEntry($lfn, $update,)) {
    $self->info("We should undo the change");
    return;
  }
  return 1;
}

##############################################################################
##############################################################################
#
# Lists a directory: WARNING: it doesn't return '..'
#


#

=item C<moveEntries($lfn, $toTable)>

This function moves all the entries under a directory to a new table
A new table is always created.

Before calling this function, you have to be already in the right database!!!
You can make sure that you are in the right database with a call to checkPermission

=cut

sub moveEntries {
  my $self=shift;
  $self->moveLFNs(@_);
}

##############################################################################
##############################################################################
sub addUser {
  my $self    = shift;
  my $user    = shift;
  my $group   = shift;


  return $self->insertIntoGroups($user, $group, 1);
}

sub getNewDirIndex {
  my $self = shift;

  $self->lockglobal(); # Getting global lock to avoid coincidences in dir value

  my ($dir) = $self->queryValue("select max(substr(table_name,2,length(table_name)-2)) from information_schema.TABLES where table_name like 'L%L'");
  $dir or $dir=0;
  $dir++;

  $self->info("New table number: $dir");

  $self->checkLFNTable($dir)
    or $self->info("Error checking the tables $dir")
    and $self->unlockglobal() and return;

  $self->unlockglobal();

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

sub deleteUser {
  my $self = shift;
  my $user = shift
    or $self->info("In deleteUser user is missing", 1)
    and return;
  my $userId = $self->getOwnerId($user);
  $DEBUG and $self->debug(2, "In deleteUser deleting entries with user $user from UGMAP table");
  $self->delete("UGMAP", "Userid='$userId'");
  $DEBUG and $self->debug(2, "In deleteUser deleting entries with user $user from USERS table");
  $self->delete("USERS", "Username='$user'");
  $DEBUG and $self->debug(2, "In deleteUser deleting entries with user $user from GRPS table");
  $self->delete("GRPS", "Groupname='$user'");
}


=item getDiskUsage($lfn)

Gets the disk usage of an entry (either file or directory)

=cut


sub setSEio {
  my $self          = shift;
  my $options       = shift;
  my $site          = shift;
  my $name          = shift;
  my $seioDaemons   = shift;
  my $seStoragePath = shift;
  my $SEName        = "$self->{CONFIG}->{ORG_NAME}::${site}::$name";
  my $SEnumber      = $self->queryValue("SELECT seNumber from SE where upper(seName)=upper('$SEName')");

  #Check that the SE exists;
  if (!$SEnumber) {
    $self->info("The se $SEName does not exist!", 1);
    return;
  }

  if (
    !$self->update("SE",
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
  my $options =shift || {};
  
  defined $se or return 0;
  $options->{force} and AliEn::Util::deleteCache($self);
  my $cache = AliEn::Util::returnCacheValue($self, "seNumber-$se");
  $cache and return $cache;
  my $senumber =
    $self->queryValue("SELECT seNumber FROM SE where upper(seName)=upper(?)", undef, {bind_values => [$se]});
  if (defined $senumber) {
    AliEn::Util::setCacheValue($self, "seNumber-$se", $senumber);
   
  }
  return $senumber;
}

sub getSEio {
  my $self    = shift;
  my $options = shift;
  my $site    = shift;
  my $name    = shift;
  my $SEName  = "$self->{CONFIG}->{ORG_NAME}::${site}::$name";
  my $SEio    = $self->queryRow("SELECT * from SE where upper(seName)=upper('$SEName')");
  return $SEio;
}

sub getSENameFromNumber {
  my $self   = shift;
  my $number = shift;
  return $self->queryValue("SELECT seName from SE where seNumber=?", undef, {bind_values => [$number]});
}

sub addSE {
  my $self    = shift;
  my $options = shift;
  my $site    = shift;
  my $name    = shift;

  my $addToTables = 1;
  my $SEName      = "$self->{CONFIG}->{ORG_NAME}::${site}::$name";
  my $SEnumber    = $self->queryValue("SELECT seNumber from SE where seName='$SEName'");

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
    my $max = $self->queryValue("SELECT max(seNumber)+1 FROM SE");
    ($max) and $SEnumber = $max;

    $self->info("Adding the new SE $SEName with $SEnumber");

    if (!$self->insert("SE", {seName => $SEName, seNumber => $SEnumber})) {
      $self->info("Error adding the entry");

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

  $self->delete("SE", "UPPER(seName)=UPPER('$sename')");
  return 1;
}

sub setUserGroup {
  my $self = shift;
  my $user       = shift;
  my $group      = shift;
  my $changeUser = shift;

  my $field = "ROLE";
  $changeUser or $field = "VIRTUAL_ROLE";

  $self->debug(1, "Setting the userid to $user ($group)");
  $self->{$field} = $user;
  $self->{MAINGROUP} = $group;
 
  return 1;
}


sub getNumberOfEntries {
  my $self  = shift;
  my $entry = shift;
  if (defined $entry->{guidTime}) {
    $self->debug(1, "Getting the number of guids");
    return $self->GUID_getNumberOfEntries($entry, @_);
  }
  return $self->LFN_getNumberOfEntries($entry, @_);
}


sub checkLFN {
  my $self   = shift;
  #my $ctable = shift;

  $self->info("YUHUU Checking the tables in $self->{DB}");
  

  
  my $tables= $self->queryColumn('select tableName from INDEXTABLE');
  
        
  foreach my $t (@$tables) {
      my $done=$self->queryValue("select 1 from LL_ACTIONS a right  join 
      (select max(ctime) real_time, count(1) real_count from L${t}L )d  on
       (extra=real_count and time=real_time and action='STATS' ) where a.action is null");
       
      $done or $self->info("table L${t}L is up to date")and next;
   
      $self->info("We have to update the table $t");
      $self->updateLFNStats($t);
  }
  return 1;
}

sub checkOrphanGUID {
  my $self = shift;
  $self->debug(1, "Checking orphanguids in the database");

  my $tables = $self->query("select * from GL_ACTIONS where action='TODELETE'");
  foreach my $table (@$tables) {
    $self->info("Doing the table $table->{tableName}");
    $self->GUID_checkOrphanGUID($table->{tableName}, @_);
  }
  $self->do(
"delete from TODELETE  using TODELETE join SE s on TODELETE.senumber=s.senumber where sename='no_se' and pfn like 'guid://%'"
  );

  return 1;
}

sub optimizeGUIDtables {
  my $self = shift;
  my $max_lim = shift;
  my $min_lim = shift;

  $self->info("Let's optimize the guid tables");

  my $tables = $self->query("SELECT tableName, guidTime from GUIDINDEX", undef, {timeout=>[60000]});
  foreach my $info (@$tables) {
    my $table = "G$info->{tableName}L";
    $self->info("  Checking the table $table");
    my $number = $self->queryValue("select count(*) from $table");
    $self->info("There are $number entries");
    my $done = 0;
    while ($number > $max_lim) {
      $self->info("There are more than $max_lim ($number) ! Splitting the table");
      my $guid =  $self->queryRow("select binary2date(guid) guidD, binary2string(guid) guid from $table order by 1 desc limit 1 offset $max_lim",
        undef, {timeout=>[60000]});
      #my $guid =  $self->queryRow("select guidid, binary2string(guid) guid from $table order by 2 desc limit 1 offset $max_lim");
      $guid->{guid} or next;
      $self->info("We have to split according to $guid->{guid}");
      $self->moveGUIDs($guid->{guid},$table, "f") or last;
      $self->info("Let's count again");
      $number = $self->queryValue("select count(*) from $table");
      $done   = 1;
    }
    $done and $self->checkGUIDTable($table);
    if ($number < $min_lim) {
      $self->info("There are less than $min_lim. Let's merge with the previous (before $info->{guidTime})");
      $self->optimizeGUIDtables_removeTable($info, $table,$max_lim,$min_lim,$number);
    }
  }
  
  $self->info("Finally updating the GUIDINDEX");
  $self->updateGUIDINDEX() or $self->info("Error: Updating the GUIDINDEX guidTime attribute");# and return;
  return 1;
}

sub optimizeGUIDtables_removeTable {
  my $self  = shift;
  my $info  = shift;
  my $table = shift;
  my $max_lim = shift;
  my $min_lim = shift;
  my $number = shift;

  defined $info->{guidTime} or return 1;

  my $previousGUID = $info->{guidTime};
  my $prevGUID = $info->{guidTime};
  #$previousGUID =~ s/.........$//;
  #$previousGUID = sprintf("%s%09X", $previousGUID, hex(substr($info->{guidTime}, -9)) - 1);

  #($previousGUID eq "FFFFFFFF")
  ($prevGUID eq "")
    and $self->info("This is the first table")
    and return 1;
  #my $t = $self->queryRow("select * from GUIDINDEX where guidTime<? order by guidTime desc limit 1",
  #  undef, {bind_values => [$previousGUID]});
  #my $t = $self->queryRow("select * from GUIDINDEX where guidTime<string2date(?) order by guidTime desc limit 1",
  #  undef, {bind_values => [$previousGUID]});
  my $t = $self->queryRow("select * from GUIDINDEX where guidTime<? order by guidTime desc limit 1",
    undef, {bind_values => [$previousGUID]});

  ($table eq "G$t->{tableName}L")
    and $self->info("Same table?? :(")
    and return;

  my $info2   = $self->query("describe $table");
  my $columns = "";
  foreach my $c (@$info2) {
    $columns .= "$c->{Field},";
  }
  $columns =~ s/guidid,//i;
  $columns =~ s/,$//;
  my $entries = $self->queryValue("select count(*) from  G$t->{tableName}L");
  if ($entries + $number > $max_lim) {
    $self->info("The previous table will have too many entries");
    return;
  }

  $self->info("This is in the same database. Tables $table and G$t->{tableName}L");
  $self->renumberGUIDtable("", $table);
  $self->renumberGUIDtable("", "G$t->{tableName}L");
  $self->lock(
"$table write, G$t->{tableName}L write, ${table}_PFN write, ${table}_REF write, G$t->{tableName}L_PFN write, G$t->{tableName}L_REF"
  );
  my $add = $self->queryValue("select max(guidid) from G$t->{tableName}L") || 0;
  $self->do(
    "insert into G$t->{tableName}L_PFN  ( pfn,seNumber,guidId ) select  pfn,seNumber, guidId+$add from ${table}_PFN");
  $self->do("insert into G$t->{tableName}L_REF  (lfnRef,guidId ) select  lfnRef, guidId+$add from ${table}_REF");

  $self->do("insert into G$t->{tableName}L  ($columns, guidId ) select  $columns, guidId+$add from ${table}");
  $self->do("DROP TABLE $table");
  $self->do("DROP TABLE $table"."_PFN");
  $self->do("DROP TABLE $table"."_QUOTA");
  $self->do("DROP TABLE $table"."_REF");
  $self->info("And now, the index  $info->{guidTime}");
  $self->unlock();
  $self->deleteFromIndex("guid", $info->{guidTime});

  return 1;
}

sub getDF {
  my $self   = shift;
  my $sename = shift;
  my $opt    = shift;

  my $query =
"select *, if(size>0,if(usedSpace/size<0, 0, floor(100* usedSpace/size)),0) used  from SE a join SE_VOLUMES b  on a.seNumber=b.seNumber";
  my $bind = [];
  if ($sename) {
    $query .= " where a.seName=?";
    push @$bind, $sename;
  }
  my $info = $self->query($query, undef, {bind_values => $bind});
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


  my $tables = $self->queryColumn("SELECT tableName from GUIDINDEX", undef, undef);
  foreach my $table (@$tables) {
    $table = "G${table}L" or return;
    my $referenced = $self->$method(
      "select $select from $table join 
      ${table}_PFN p  using (guidid) join ${table}_REF r using (guidid)
      where  p.senumber=? ", undef, {bind_values => [$senumber]}
    );

    my $broken = $self->$method(
      "select $select from $table join 
      ${table}_PFN p  using (guidid) left join ${table}_REF r using (guidid)
      where  p.senumber=? and r.guidid is null",
      undef, {bind_values => [$senumber]}
    );
    my $replicated = $self->$method(
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
  #my $options        = shift || {};
  my $md5             = shift || 0;
  my $unique          = shift ||0;
  my $replicated      = shift ||0;
  my $lfn             = shift ||0;

  my $previous_host;
  $self->info("IN MASTERSE_GETFILES, we have $sename, $previous_table, $limit, $md5, $unique, $replicated and $lfn");
  $previous_table =~ s/^(\d+)_// and $previous_host = $1;
  my $senumber = $self->getSENumber($sename)
    or $self->info("Error getting the se number of $sename")
    and return;

  my $return = [];

  my $query = "select binary2string(g.guid)guid,p.pfn  ";
  $md5 and $query .= ", g.md5 ";

  #Let's skip all the hosts that we have already seen
  my $startLooking=1;
  $previous_table and $startLooking=0;
  
  my $tables = $self->queryColumn("SELECT tableName from GUIDINDEX order by 1", undef, undef);
  foreach my $table (@$tables) {
    $table = "G${table}L";
    if($previous_table and $previous_table =~ /^_$table$/){
    	$startLooking = 1;
    	next;
    }

    #      $table =~ /G46L/ or next;
    if ($previous_table) {
      $previous_table = "";
    }
    my $endquery = "";
    if ($unique) {
      $self->info("Checking that the file is not replicated");
      $endquery =
"and not exists (select 1 from ${table}_PFN p2 where p2.senumber!=p.senumber and p2.guidid=p.guidid) group by guid";
    }
    if ($replicated) {
      $endquery =
        "and exists (select 1 from ${table}_PFN p2 where p2.senumber!=p.senumber and p2.guidid=p.guidid) group by guid";
    }
    my $entries = [];
    if ($lfn) {
      $self->info("WE HAVE TO MAKE SURE THAT THE REFERENCES ARE UP TO DATE");
      $self->debug(1, "Getting the lfn of the files");
      my $ref = $self->query(
"select  distinct lfnRef, lfn  from  ${table}_REF join  ${table}_PFN p using (guidid) 
join INDEXTABLE on (lfnRef=tableName) where p.senumber=?",
        undef,
        {bind_values => [$senumber]}
      );
      $ref or return;
      
      foreach my $entry (@$ref) {
        my $lfnTable="$entry->{lfnRef}";
        
        my $dd = $self->query(
"$query, concat(?,lfn) lfn  from $table g join  ${table}_PFN p  using (guidid) 
  join $entry->{db}.L${lfnTable}L l using (guid) where p.senumber=? $endquery",
          undef,
          {bind_values => [ $entry->{lfn}, $senumber ]}
        );
        $dd or return;
        $self->info("    doing $table and $entry->{lfnRef} $#$dd");

        #	  my $dd=[];
        $entries = [ @$entries, @$dd ];
      }
    } else {
      $entries = $self->query("$query from  $table g join  ${table}_PFN p  using (guidid) where p.senumber=? $endquery",
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
  my $options = shift;

  my $extratable = "";
  $self->info("Calculating all the broken links in $table->{lfn}");

  my $GUIDList = $self->getPossibleGuidTables($table->{tableName});
  my $t        = "L$table->{tableName}L";
  $self->checkLFNTable($table->{tableName});
  $self->do("truncate table ${t}_broken");
  $self->do("insert into ${t}_broken  select entryId from  $t where type='f'");
  foreach my $entry (@$GUIDList) {
    $options->{nopfn} and $extratable = "join  $entry->{db}.G$entry->{tableName}L_PFN using (guidid)";
    $self->do(
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
    $allEntries = $self->getTablesForEntry($dir);
  }
  

  my $tables;
  if ($allEntries) {
    foreach my $c (@$allEntries) {
      push @$tables, $c;
    }
  } else {
    $tables = $self->query("SELECT tableName,lfn from INDEXTABLE", undef, undef);
  }
  for my $t (@$tables) {
    $self->checkLFNTable($t->{tableName});
    $self->info("Checking the table $t->{tableName}");
    $options->{calculate} and $self->calculateBrokenLFN($t, $options);
    my $like = "";
    my $bind = [ $t->{lfn} ];
    if ($dir) {
      $like = "where concat('$t->{lfn}',lfn) like concat(?,'%')";
      push @$bind, $dir;
    }
    my $entries = $self->queryColumn(
      "SELECT concat(?,lfn) from L$t->{tableName}L join  L$t->{tableName}L_broken using (entryId) $like ",
      undef, {bind_values => $bind});
    foreach my $e (@$entries) {
      $self->info($e, 0, 0);
    }
    push @$all, @$entries;
  }
  return $all;
}


sub checkSETable {
  my $self = shift;

  my %columns = (
    seName           => "varchar(60) character set latin1 collate latin1_general_ci NOT NULL",
    seNumber         => "int(11) NOT NULL auto_increment primary key",
    seQoS            => "varchar(200) character set latin1 collate latin1_general_ci",
    seioDaemons      => "varchar(255)",
    seStoragePath    => "varchar(255)",
    seNumFiles       => "bigint",
    seUsedSpace      => "bigint",
    seType           => "varchar(60)",
    seMinSize        => "int default 0",
    seExclusiveWrite => "varchar(300) character set latin1 collate latin1_general_ci",
    seExclusiveRead  => "varchar(300) character set latin1 collate latin1_general_ci",
    seVersion        => "varchar(300)",
    seDemoteWrite    => "float not null default 0",
    seDemoteRead     => "float not null default 0"
  );

  return $self->checkTable("SE", "seNumber", \%columns, 'seNumber', ['UNIQUE INDEX (seName)'], {engine => "innodb"})
    ;    #or return;
         #This table we want it case insensitive

  #  return $self->do("alter table SE  convert to CHARacter SET latin1");
}
sub getUserid {
  my $self  = shift;
  my $user  = shift;
  my $group = shift;
  my $where = "primarygroup=1";
  $group and $where = "Groupname='$group'";
  return $self->queryValue("SELECT Userid from UGMAP JOIN USERS ON Userid=uId JOIN GRPS ON Groupid=gId where Username='$user' and $where"); 
}


sub deleteFromIndex {
  my $self    = shift;
  my @entries = @_;

  map { $_ = "lfn like '$_'" } @entries;
  my $indexTable = "INDEXTABLE";
  $self->info("Ready to delete the index for @_");
  if ($_[0] =~ /^guid$/) {
    $self->info("Deleting from the guidindex");
    $indexTable = "GUIDINDEX";
    shift;
    @entries = @_;
    @entries = map { $_ = "guidTime = '$_'" } @entries;
  }

  my $action = "DELETE FROM $indexTable WHERE " . join(" or ", @entries);
  return $self->do($action);

}

sub insertInIndex {
  my $self      = shift;
  my $table     = shift;
  my $lfn       = shift;
  my $options   = shift;

  $table =~ s/^L(\d+)L$/$1/;
  my $indexTable = "INDEXTABLE";
  my $column     = "lfn";
  my $value      = "'$lfn'";
  if ($options->{guid}) {
    $table =~ s/^G(\d+)L$/$1/;
    $column     = "guidTime";
    $indexTable = "GUIDINDEX";
    $value      = "string2date('$lfn')";
  }
  $indexTable =~ /GUIDINDEX/ and $column = 'guidTime';
  my $action = "INSERT INTO $indexTable ( tableName, $column) values( '$table', $value)";
  return $self->do($action);
}

sub getIndexTable {
  my $self = shift;
  return $self->{INDEX_TABLENAME};
}

sub checkUserGroup {
  my $self = shift;
  my $user = shift
    or $self->debug(2, "In checkUserGroup user is missing")
    and return;
  my $group = shift
    or $self->debug(2, "In checkUserGroup group is missing")
    and return;

  $DEBUG and $self->debug(2, "In checkUserGroup checking if user $user is member of group $group");
  my $v = AliEn::Util::returnCacheValue($self, "usergroup-$user-$group");
  defined $v and return $v;
  $v = $self->queryValue("SELECT count(*) from UGMAP JOIN USERS ON Userid=uId JOIN GRPS ON Groupid=gId where Username='$user' and Groupname = '$group'");
  AliEn::Util::setCacheValue($self, "usergroup-$user-$group", $v);

  return $v;
}
sub returnFromCacheOrQuery{
  my $self=shift;
  my $cacheKey=shift;
  my $query=shift;
  my $bind =shift;

  my $v = AliEn::Util::returnCacheValue($self, $cacheKey);
  defined $v and return $v;
  $v = $self->queryValue( $query, undef,{bind_values=>$bind});
  AliEn::Util::setCacheValue($self, $cacheKey, $v);
  return $v;
}
sub getOwnerId {
  my $self  = shift;
  my $user  = shift;
  $user or return;
  return $self->returnFromCacheOrQuery("userId-$user", "SELECT uId from USERS where Username=?", [$user]);
}

sub getGownerId {
  my $self  = shift;
  my $user  = shift;
  $user or return;
  return $self->returnFromCacheOrQuery("gourpId-$user", "SELECT gId from GRPS where Groupname=?", [$user]);
}

sub getOwner {
  my $self  = shift;
  my $uId  = shift;
  $uId or return;
  return $self->returnFromCacheOrQuery("user-$uId", "SELECT Username from USERS where uId=?", [$uId]);
}

sub getGowner {
  my $self  = shift;
  my $gId  = shift;
  $gId or return;
  return $self->returnFromCacheOrQuery("group-$gId", "SELECT Groupname from GRPS where gId=?", [$gId]);
}



1;
