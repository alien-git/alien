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
package AliEn::Database::Catalogue::LFN;

use AliEn::Database::Catalogue::Shared;
use strict;

use Data::Dumper;

=head1 NAME

AliEn::Database::Catalogue - database wrapper for AliEn catalogue

=head1 DESCRIPTION

This module interacts with a database of the AliEn Catalogue. The AliEn Catalogue can be distributed among several databases, each one with a different layout. In this basic layout, there can be several tables containing the entries of the catalogue. 

=cut

use vars qw( $DEBUG);

#This array is going to contain all the connections of a given catalogue

$DEBUG = 0;

=head1 SYNOPSIS

  use AliEn::Database::Catalogue;

  my $catalogue=AliEn::Database::Catalogue->new() or exit;


=head1 METHODS

=over

=cut

=item C<createCatalogueTables>

This methods creates the database schema in an empty database. The tables that this implemetation have are:
 

=cut

#my $binary2string="insert(insert(insert(insert(hex(guid),9,0,'-'),14,0,'-'),19,0,'-'),24,0,'-')";

#

# Checking the consistency of the database structure
sub LFN_createCatalogueTables {
  my $self = shift;

  $DEBUG and $self->debug(2, "In createCatalogueTables creating all tables...");

  foreach ("Constants", "SE") {
    my $method = "check" . $_ . "Table";
    $self->$method()
      or $self->{LOGGER}->error("Catalogue", "Error checking the $_ table")
      and return;
  }
  my %tables = (
    TRIGGERS => [
      "lfn",
      { lfn         => "varchar(255)",
        triggerName => "varchar(255)",
        entryId     => "int auto_increment primary key"
      }
    ],
    TRIGGERS_FAILED => [
      "lfn",
      { lfn         => "varchar(255)",
        triggerName => "varchar(255)",
        entryId     => "int auto_increment primary key"
      }
    ],
    LFN_UPDATES => [
      "guid",
      { guid    => "binary(16)",
        action  => "char(10)",
        entryId => "int auto_increment primary key"
      },
      'entryId',
      ['INDEX (guid)']
    ],
    TAG0 => [
      "entryId",
      { entryId   => "int(11) NOT NULL auto_increment primary key",
        path      => "varchar (255)",
        tagName   => "varchar (50)",
        tableName => "varchar(50)",
        user      => 'varchar(20)'
      },
      'entryId'
    ],
    UGMAP => [
      "Userid",
      { Userid       => "int not null",
        Groupid      => "int not null",
        PrimaryGroup => "int(1)",
      }
    ],
    USERS => [
      "uId",
      { uId          => "mediumint unsigned not null auto_increment primary key",
        Username     => "char(20) UNIQUE NOT NULL",
      },
      'uId'
    ],
    GRPS => [
      "gId",
      { gId          => "mediumint unsigned not null auto_increment primary key",
        Groupname     => "char(20) UNIQUE NOT NULL",
      },
      'gId'
    ],
    INDEXTABLE => [
      "tableName",
      { 
        lfn       => "varchar(255)",
        tableName => "int(11) NOT NULL primary key",
      },
      'tableName',
      ['UNIQUE INDEX (lfn)']
    ],
    ENVIRONMENT => [
      'userName',
      { userName => "char(20) NOT NULL PRIMARY KEY",
        env      => "char(255)"
      }
    ],
    ACTIONS => [
      'action',
      { action => "char(40) not null primary key",
        todo   => "int(1) default 0 not null "
      },
      'action'
    ],
    PACKAGES => [
      'fullPackageName',
      { 'fullPackageName' => 'varchar(255)',
        packageName       => 'varchar(255)',
        username          => 'varchar(20)',
        packageVersion    => 'varchar(255)',
        platform          => 'varchar(255)',
        lfn               => 'varchar(255)',
        size              => 'bigint'
      },
    ],
    COLLECTIONS => [
      'collectionId',
      { 'collectionId' => "int not null auto_increment primary key",
        'collGUID'     => 'binary(16)'
      }
    ],
    COLLECTIONS_ELEM => [
      'collectionId',
      { 'collectionId' => 'int not null',
        origLFN        => 'varchar(255)',
        guid           => 'binary(16)',
        data           => "varchar(255)",
        localName      => "varchar(255)"
      },

      "",
      ['INDEX (collectionId)']
    ],

    "SE_VOLUMES" => [
      "volume",
      { volumeId   => "int(11) NOT NULL auto_increment PRIMARY KEY",
        seName     => "char(255) collate latin1_general_ci NOT NULL ",
        volume     => "char(255) NOT NULL",
        mountpoint => "char(255)",
        usedspace  => "bigint",
        freespace  => "bigint",
        size       => "bigint",
        method     => "char(255)",
      },
      "volumeId",
      [ 'UNIQUE INDEX (volume)', 'INDEX(seName)' ],
    ],
    "LL_STATS" => [
      "tableNumber",
      { tableNumber => "int(11) NOT NULL",
        min_time    => "char(20) NOT NULL",
        max_time    => "char(20) NOT NULL",
      },
      undef,
      ['UNIQUE INDEX(tableNumber)']
    ],
    LL_ACTIONS => [
      "tableNumber",
      { tableNumber => "int(11) NOT NULL",
        action      => "char(40) not null",
        time        => "timestamp default current_timestamp",
        extra       => "varchar(255)"
      },
      undef,
      ['UNIQUE INDEX(tableNumber,action)']
    ],
    SERanks => [
      "sitename",
      { sitename => "varchar(100) collate latin1_general_ci  not null",
        seNumber => "integer not null",
        rank     => "smallint(7) not null",
        updated  => "smallint(1)"
      },
      undef,
      ['UNIQUE INDEX(sitename,seNumber), PRIMARY KEY(sitename,seNumber), INDEX(sitename), INDEX(seNumber)']
    ],

    FQUOTAS => [
      "user",
      { user                  => "varchar(64) NOT NULL",
        totalSize             => "bigint(20) collate latin1_general_ci DEFAULT '0' NOT NULL ",
        maxNbFiles            => "int(11) DEFAULT '0' NOT NULL ",
        nbFiles               => "int(11) DEFAULT '0' NOT NULL ",
        tmpIncreasedTotalSize => "bigint(20) DEFAULT '0' NOT NULL ",
        maxTotalSize          => "bigint(20) DEFAULT '0' NOT NULL ",
        tmpIncreasedNbFiles   => "int(11) DEFAULT '0' NOT NULL "
      },
      undef,
      ['PRIMARY KEY(user)']
    ],

    LFN_BOOKED => [
      "lfn",
      { lfn             => "varchar(255)",
        expiretime      => "int",
        guid            => "binary(16) ",
        size            => "bigint",
        md5sum          => "varchar(32)",
        owner           => "varchar(20)  collate latin1_general_ci ",
        gowner          => "varchar(20)",
        pfn             => "varchar(255)",
        se              => "varchar(100)  collate latin1_general_ci ",
        quotaCalculated => "smallint",
        user            => "varchar(20)  collate latin1_general_ci ",
        existing        => "smallint(1)",
        jobid           => "int(11)",
      },
      undef,
      [ 'PRIMARY KEY(lfn,pfn,guid)', 'INDEX(pfn)', 'INDEX(guid)', 'INDEX(jobid)' ]
    ],
    PFN_TODELETE => [ "pfn", {pfn => "varchar(255)", retry => "integer not null"}, undef, ['UNIQUE INDEX(pfn)'] ]

  );
  foreach my $table (keys %tables) {
    $self->info("Checking table $table");
    $self->checkTable($table, @{$tables{$table}}) or return;
  }

  $self->checkLFNTable("0") or return;
  $self->do(
    "INSERT INTO ACTIONS ( action) SELECT 'PACKAGES' FROM DUAL WHERE NOT  EXISTS 
(SELECT  * FROM ACTIONS WHERE ACTION = 'PACKAGES') "
  );
  $self->info("Let's create the functions");
  $self->createLFNfunctions;
  $DEBUG and $self->debug(2, "In createCatalogueTables creation of tables finished.");
  $self->do("alter table TAG0 drop index path");

  #  $self->do("alter table TAG0 add index path (path)");
  $self->createIndex("TAG0", "index path (path)");
  1;
}

#
#
# internal functions
sub checkConstantsTable {
  my $self    = shift;
  my %columns = (
    name  => "varchar(100) NOT NULL",
    value => "int",
  );
  $self->checkTable("CONSTANTS", "name", \%columns, 'name') or return;
  my $exists = $self->queryValue("SELECT count(*) from CONSTANTS where name='MaxDir'");
  $exists and return 1;
  return $self->do("INSERT INTO CONSTANTS values ('MaxDir', 0)");
}

sub checkLFNTable {
  my $self  = shift;
  my $table = shift;
  defined $table or $self->info("Error: we didn't get the table number to check") and return;

  $table =~ /^\d+$/ and $table = "L${table}L";

  my $number;
  $table =~ /^L(\d+)L$/ and $number = $1;

  my %columns = (
    entryId => "int unsigned NOT NULL auto_increment primary key",
    lfn => "varchar(255)",    #in Oracle the empty string is null, so we have to allow this column to be null
    type       => "char(1)  default 'f' NOT NULL",
    ctime      => "timestamp  DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP",
    expiretime => "datetime",
    size       => "bigint  default 0 not null ",
    perm       => "char(3) not null",
    guid       => "binary(16)",
    replicated => "smallint(1) default 0 not null",
    dir        => "int unsigned not null",
    ownerId    => "mediumint unsigned",
    gownerId   => "mediumint unsigned",
    md5        => "char(32)",
    guidtime   => "char(8)",
    broken     => 'smallint(1) default 0 not null ',
    jobid      => "int(11)",
  );

  $self->checkTable(${table}, "entryId", \%columns, 'entryId',
    [ 'UNIQUE INDEX (lfn)', "INDEX(dir)", "INDEX(guid)", "INDEX(type)", "INDEX(ctime)", "INDEX(guidtime)" ])
    or return;
  $self->checkTable("${table}_broken", "entryId", {entryId => "bigint(11) NOT NULL  primary key"}) or return;
  $self->checkTable("${table}_QUOTA", "user",
    {user => "varchar(64) NOT NULL", nbFiles => "int(11) NOT NULL", totalSize => "bigint(20) NOT NULL"},
    undef, ['INDEX user_ind (user)'],)
    or return;

  $self->optimizeTable(${table});

  #  $self->do("optimize table ${table}_QUOTA");

  return 1;
}

##############################################################################
##############################################################################
sub setIndexTable {
  my $self  = shift;
  my $table = shift;
  my $lfn   = shift;
  defined $table or return;
  $table =~ /^\d*$/ and $table = "L${table}L";

  $DEBUG and $self->debug(2, "Setting the indextable to $table ($lfn)");
  $self->{INDEX_TABLENAME} = {name => $table, lfn => $lfn};
  return 1;
}

sub getAllInfoFromLFN {
  my $self    = shift;
  my $options = shift;

  my $tableName = $self->{INDEX_TABLENAME}->{name};
  $options->{table}
    and $options->{table}->{tableName}
    and $tableName = $options->{table}->{tableName};
  my $tablePath = $self->{INDEX_TABLENAME}->{lfn};
  $options->{table}
    and $options->{table}->{lfn}
    and $tablePath = $options->{table}->{lfn};
  defined $tableName or $self->info("Error: missing the tableName in getAllInfoFromLFN") and return;

  #  @_ or $self->info( "Warning! missing arguments in internal function getAllInfoFromLFN") and return;
  $tableName =~ /^\d+$/ and $tableName = "L${tableName}L";
  my @entries = grep (s{^$tablePath}{}, @_);
  my @list = @entries;

  $DEBUG and $self->debug(2, "Checking for @entries in $tableName");
  my $op = '=';
  ($options->{like}) and $op = "$options->{like}";

  map { $_ = "lfn $op ?" } @entries;

  my $where = "WHERE " . join(" or ", @entries);
  my $opt = ($options->{options} or "");
  ($opt =~ /h/) and $where .= " and lfn not like '%/.%'";
  ($opt =~ /d/) and $where .= " and type='d'";
  ($opt =~ /f/) and $where .= " and type='f'";
  ($opt =~ /l/) and $where .= " and type='l'";

  my $order = $options->{order};
  $options->{where} and $where .= " $options->{where}";
  $order and $where .= " order by $order";

  if ($options->{retrieve}) {
    $options->{retrieve} =~ s{lfn}{concat('$tablePath',lfn) as lfn};

    # $options->{retrieve} =~ s{guid}{$binary2string as guid};
    my $b = $self->binary2string;
    $options->{retrieve} =~ s{guid}{$b as guid};
  }
  my $retrieve = (
    $options->{retrieve}
      or "entryId,ownerId ,replicated,guidtime,  broken, expiretime, "
      . $self->reservedWord("size")
      . ",dir,  gownerId ,  "
      . $self->reservedWord("type")
      . " ,md5,perm,concat('$tablePath',lfn) as lfn, "
      . $self->binary2string
      . " as guid,"
      . $self->dateFormat("ctime")
  );

  my $method = ($options->{method} or "query");

  if ($options->{exists}) {
    $method   = "queryValue";
    $retrieve = "count(*)";
  }

  $options->{bind_values} and push @list, @{$options->{bind_values}};

  my $DBoptions = {bind_values => \@list};
  return $self->$method("SELECT $retrieve FROM $tableName JOIN USERS ON ownerId=uId JOIN GRPS ON gownerId=gId $where", undef, $DBoptions);
}

=item c<existsLFN($lfn)>

This function receives an lfn, and checks if it exists in the catalogue. It checks for lfns like '$lfn' and '$lfn/', and, in case the entry exists, it returns the name (the name has a '/' at the end if the entry is a directory)

=cut

sub existsLFN {
  my $self  = shift;
  my $entry = shift;

  $entry =~ s{/?$}{};
  my $options = {bind_values => ["$entry/"]};
  my $query = "SELECT tableName,lfn from INDEXTABLE where lfn= substr(?,1, length(lfn))  order by length(lfn) desc";
  $query = $self->paginate($query, 1, 0);
  my $tableRef = $self->queryRow($query, undef, $options);

  defined $tableRef or return;
  my $dataFromLFN =
    $self->getAllInfoFromLFN({method => "queryValue", retrieve => "lfn", table => $tableRef}, $entry, "$entry/");
  $dataFromLFN and return $dataFromLFN;
  return;
}

=item C<getTablesForEntry($lfn)>

This function returns a list of all the possible tables that might contain entries of a directory

=cut

sub getTablesForEntry {
  my $self = shift;
  my $lfn  = shift;

  $lfn =~ s{/?$}{};

  #First, let's take the entry that has the directory
  my $query =
    "SELECT tableName,lfn from INDEXTABLE where lfn=substr('$lfn/',1,length(lfn))  order by length(lfn) desc ";
  $query = $self->paginate($query, 1, 0);
  my $entry = $self->query($query);
  $entry or return;

  #Now, let's get all the possibles expansions (but the expansions at least as
  #long as the first index
  my $length = length(${$entry}[0]->{lfn});
  my $expansions =
    $self->query("SELECT distinct tableName,lfn from INDEXTABLE where lfn like '$lfn/%' and length(lfn)>$length");
  my @all = (@$entry, @$expansions);
  return \@all;
}

=item C<createFile($hash)>

Adds a new file to the database. It receives a hash with the following information:



=cut

sub LFN_createFile {
  my $self      = shift;
  my $options   = shift || "";
  my $tableName = $self->{INDEX_TABLENAME}->{name};
  my $tableLFN  = $self->{INDEX_TABLENAME}->{lfn};
  my $entryDir;
  my $seNumbers = {};
  my @inserts   = @_;
  foreach my $insert (@inserts) {
    $insert->{type} or $insert->{type} = 'f';
    $entryDir or $entryDir = $self->getParentDir($insert->{lfn});
    $insert->{dir} = $entryDir;
    $insert->{lfn} =~ s{^$tableLFN}{};
    foreach my $key (keys %$insert) {
      $key =~ /^guid$/ and next;
      if ($key =~ /^(se)|(pfn)|(pfns)$/) {
        delete $insert->{$key};
        next;
      }
      $insert->{$key} = "'$insert->{$key}'";
    }
    if ($insert->{guid}) {
      $insert->{guidtime} = "string2date('$insert->{guid}')";
      $insert->{guid}     = "string2binary('$insert->{guid}')";
    }
  }
  $self->info("Inserting the lfn");
  my $done = $self->multiinsert($tableName, \@inserts, {noquotes => 1, silent => 1});
  if (!$done and $DBI::errstr =~ /Duplicate entry '(\S+)'/) {
    $self->info("The entry '$tableLFN$1' already exists");
  }

  return $done;
}

#
# This subroutine returns the guid of an LFN. Before calling it,
# you have to make sure that you are in the right database
# (with checkPersmissions for instance)
sub getGUIDFromLFN {
  my $self = shift;

  return $self->getAllInfoFromLFN({retrieve => 'guid', method => 'queryValue'}, @_);
}

sub getParentDir {
  my $self = shift;
  my $lfn  = shift;
  $lfn =~ s{/[^/]*/?$}{/};

  my $tableName = $self->{INDEX_TABLENAME}->{name} or return;
  $lfn =~ s{$self->{INDEX_TABLENAME}->{lfn}}{};
  return $self->queryValue("select entryId from $tableName where lfn=?", undef, {bind_values => [$lfn]});
}

sub LFN_updateEntry {
  my $self = shift;
  $self->debug(2, "In updateFile with @_");
  my $file   = shift;
  my $update = shift;

  my $lfnUpdate = {};

  my $options = {noquotes => 1};

  $update->{size}   and $lfnUpdate->{size}   = $update->{size};
  $update->{guid}   and $lfnUpdate->{guid}   = "string2binary(\"$update->{guid}\")";
  $update->{ownerId}  and $lfnUpdate->{ownerId}  = "\"$update->{ownerId}\"";
  $update->{gownerId} and $lfnUpdate->{gownerId} = "\"$update->{gownerId}\"";

  #maybe all the information to update was only on the guid side
  (keys %$lfnUpdate) or return 1;
  my $tableName = $self->{INDEX_TABLENAME}->{name};
  my $lfn       = $self->{INDEX_TABLENAME}->{lfn};

  $self->debug(1, "There is something to update!!");
  $file =~ s{^$lfn}{};
  $self->update($tableName, $lfnUpdate, "lfn='$file'", {noquotes => 1})
    or return;
  $file and return 1;

  $self->info("This is in fact an index!!!!");
  my $parentpath = $lfn;

  #If it is /, we don't have to do anything
  ($parentpath =~ s{[^/]*/?$}{}) or return 1;

  $self->info("HERE WE SHOULD UPDATE ALSO THE FATHER");

  my $db = $self->selectTable($parentpath);
  $lfn =~ s{^$db->{INDEX_TABLENAME}->{lfn}}{};

  # my $new_lfn = $lfn;$new_lfn and $new_lfn="='$lfn'" or $new_lfn=" is null";
  # return $db->update($db->{INDEX_TABLENAME}->{name}, $lfnUpdate, "lfn $new_lfn ", {noquotes=>1});

  return $db->update($db->{INDEX_TABLENAME}->{name}, $lfnUpdate, "lfn='$lfn'", {noquotes => 1});
}

sub deleteFile {
  my $self = shift;
  my $file = shift;

  my $tableName = $self->{INDEX_TABLENAME}->{name};

  #  my $index=",$self->{CURHOSTID}_$tableName,";
  $file =~ s{^$self->{INDEX_TABLENAME}->{lfn}}{};

  return $self->delete($tableName, "lfn='$file'");
}

sub getLFNlike {
  my $self = shift;
  my $lfn  = shift;

  my @result;
  $lfn =~ s/\*/%/g;
  $lfn =~ s/\?/_/g;

  $self->debug(1, "Trying to find paths like $lfn");
  my @todo = $lfn;

  while (@todo) {
    my $parent = shift @todo;

    if ($parent =~ s{([^/]*[\%][^/]*)/?(.*)$}{}) {
      $self->debug(1, "Looking in $parent for $1 (still to do $2)");
      my ($pattern, $todo) = ($1, $2);

      my $db = $self->selectTable($parent)
        or $self->info("Error selecting the database of $parent")
        and next;
      my $parentdir = $db->getAllInfoFromLFN({retrieve => 'entryId', method => 'queryValue'}, $parent);

      my $tableName = $db->{INDEX_TABLENAME}->{name};
      my $tablelfn  = $db->{INDEX_TABLENAME}->{lfn};

      my $ppattern = "$parent$pattern";

      my $entries =
        $db->queryColumn("SELECT concat('$tablelfn',lfn) from $tableName where dir=? and (lfn like ? or lfn like ?)",
        undef, {bind_values => [ $parentdir, $ppattern, "$ppattern/" ]})
        or $self->info("error doing the query")
        and next;
      foreach my $entry (@$entries) {
        if ($todo) {
          $entry =~ m{/$}
            and push @todo, "$entry$todo";
        } else {
          push @result, $entry;
        }
      }
    } else {
      my $db = $self->selectTable($parent)
        or $self->info("Error selecting the database of $parent")
        and next;
      my $parentdir = $db->getAllInfoFromLFN({retrieve => 'entryId', method => 'queryValue'}, $parent, "$parent/")
        or next;
      push @result, "$db->{INDEX_TABLENAME}->{lfn}$parent";
    }
  }

  return \@result;
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
  my $self    = shift;
  my $info    = shift;
  my $options = shift || "";
  my $selimit = shift;
  my $entry;

  if (UNIVERSAL::isa($info, "HASH")) {
    $entry = $info;
  } else {
    $entry = $self->getAllInfoFromLFN({method => "queryRow"}, $info);
    $entry or $self->info("Error getting the info of $info") and return;
  }
  my $sort = "order by lfn";
  $options =~ /f/ and $sort = "";
  my @all;

  if (!$selimit) {
    my $content = $self->getAllInfoFromLFN(
      { table       => $self->{INDEX_TABLENAME},
        where       => "dir=? $sort",
        bind_values => [ $entry->{entryId} ]
      }
    );

    $content and push @all, @$content;
  } else {
    $self->debug(1, "We only have to display from $selimit (table $self->{INDEX_TABLENAME}");
    my $GUIDList = $self->getPossibleGuidTables($self->{INDEX_TABLENAME}->{name});

    my $content = $self->getAllInfoFromLFN(
      { table       => $self->{INDEX_TABLENAME},
        where       => "dir=? and substr(lfn,length(lfn))='/' $sort",
        bind_values => [ $entry->{entryId} ]
      }
    );

    $content and push @all, @$content;
    foreach my $elem (@$GUIDList) {
      $self->debug(1, "Checking table $elem and $elem->{tableName}");
      ($elem->{address} eq $self->{HOST})
        or $self->info("We can't check the se (the info is in another host")
        and return;

      my $content = $self->getAllInfoFromLFN(
        { table => {
            tableName =>
"$self->{INDEX_TABLENAME}->{name} l, $elem->{db}.G$elem->{tableName}L g, $elem->{db}.G$elem->{tableName}L_PFN p",
            lfn => $self->{INDEX_TABLENAME}->{lfn}
          },
          where       => "dir=? and l.guid=g.guid and p.guidid=g.guidid and p.seNumber=? $sort",
          bind_values => [ $entry->{entryId}, $selimit ],
          retrieve    => "distinct l.entryId,l.ownerId,l.replicated,l.guidtime,l.lfn,  l.broken, l.expiretime, l."
            . $self->reservedWord("size")
            . ",l.dir,  l.gownerId,  l."
            . $self->reservedWord("type")
            . " ,l.md5,l.perm, l.guid,l."
            . $self->binary2string("l.Guid")
            . " as Guid,"
            . $self->dateFormat("ctime")
        }
      );

      $content and push @all, @$content;
    }
    @all = sort { $a->{lfn} cmp $b->{lfn} } @all;
  }
  if ($options =~ /a/) {
    $entry->{lfn} = ".";
    @all = ($entry, @all);
  }
  if ($options !~ /F/) {
    foreach my $entry2 (@all) {
      $entry2->{lfn} =~ s{/$}{};
    }
  }
  return @all;
}

#
# createDirectory ($lfn, [$perm, [$replicated, [$table]]])
#
sub createDirectory {
  my $self   = shift;
  my $ownerId = $self->getOwnerId($self->{VIRTUAL_ROLE});
  my $gownerId = $self->getGownerId($self->{VIRTUAL_ROLE});
  my $insert = {
    lfn        => shift,
    perm       => (shift or "755"),
    replicated => (shift or 0),
    ownerId    => $ownerId,
    gownerId   => $gownerId,
    type       => 'd'
  };
  return $self->_createEntry($insert, @_);
}

sub createRemoteDirectory {
  my $self = shift;
  my ($lfn) = @_;
  my $oldtable = $self->{INDEX_TABLENAME};

  #Now, in the new database
  my $newTable = $self->getNewDirIndex() or $self->info("Error getting the new dirindex") and return;
  $newTable = "L${newTable}L";

  #ok, let's insert the entry in $table
  $self->info("Before callting createDirectory");
  my $done = $self->createDirectory("$lfn/", $self->{UMASK}, 0, {name => $newTable, lfn => "$lfn/"});

  $done or $self->info("Error creating the directory $lfn") and return;
  $self->info("Directory $lfn/ created");
  $self->info("Now, let's try to do insert the entry in the INDEXTABLE");
  if (!$self->insertInIndex( $newTable, "$lfn/")) {
    $self->delete($newTable, "lfn='$lfn/'");
    return;
  }
  $self->info("Almost everything worked!!");
  $self->createDirectory("$lfn/", $self->{UMASK}, 1, $oldtable) or return;
  $self->info("Everything worked!!");

  return $done;

}

#Delete file from Catalogue
sub removeFile {
  my $self     = shift;
  my $lfn      = shift;
  my $filehash = shift;
  my $user     = shift || $self->{ROLE};

  #Insert into LFN_BOOKED
  my $parent = "$lfn";
  $parent =~ s{([^/]*[\%][^/]*)/?(.*)$}{};
  my $db = $self->selectTable($parent)
    or $self->info( "Error selecting the database of $parent")
    and return;
  my $tableName  = "$db->{INDEX_TABLENAME}->{name}";
  my $tablelfn   = "$db->{INDEX_TABLENAME}->{lfn}";
  my $lfnOnTable = "$lfn";
  $lfnOnTable =~ s/$tablelfn//;
  my $guid = $db->queryValue("SELECT binary2string(l.guid) as guid FROM $tableName l WHERE l.lfn=?",
    undef, {bind_values => [$lfnOnTable]})
    || 0;

  #Insert into LFN_BOOKED only when the GUID has to be deleted
  $db->do(
        "INSERT INTO LFN_BOOKED(lfn, owner, expiretime, "
      . $db->reservedWord("size")
      . ", guid, gowner, "
      . $db->reservedWord("user") . ", pfn)
    SELECT ?, USERS.Username, -1, l."
      . $db->reservedWord("size") 
      . ", l.guid, GRPS.Groupname, ?,'*' FROM $tableName l JOIN USERS ON l.ownerId=uId JOIN GRPS ON l.gownerId=gId WHERE l.lfn=? AND l.type<>'l'",
    {bind_values => [ $lfn, $user, $lfnOnTable ]}
    )
    or $self->info( "Could not insert LFN(s) in the booking pool")
    and return;

  #Delete from table
  $db->do("DELETE FROM $tableName WHERE lfn=?", {bind_values => [$lfnOnTable]});

  #Update Quotas
  if ($filehash->{type} eq "f") {
    $self->fquota_update(-1 * $filehash->{size}, -1, $user)
      or $self->info( "ERROR: Could not update quotas")
      and return;
  }

  $self->info("$lfn was moved to booking table");

  return 1;
}

#Delete folder from Catalogue
sub removeDirectory {
  my $self      = shift;
  my $path      = shift;
  my $parentdir = shift;
  my $user      = shift || $self->{ROLE};

  #Insert into LFN_BOOKED and delete lfns
  my $entries = $self->getTablesForEntry($path)
    or $self->info("ERROR: Could not get tables for $path", 1)
    and return;
  my @index = ();
  my $size  = 0;
  my $count = 0;
  foreach my $entry (@$entries) {
    $self->info("Deleting all the entries from  table $entry->{tableName} and lfn=$entry->{lfn}");
#    my ($db2, $path2) = $self->reconnectToIndex($db->{hostIndex}, $path);
#    $db2
#      or $self->info( "ERROR: Could not reconnect to host")
#      and return;
    my $tmpPath = "$path/";
    $tmpPath =~ s{^$entry->{lfn}}{};
    $count += (
      $self->queryValue("SELECT count(*) FROM L$entry->{tableName}L l WHERE l.type='f' AND l.lfn LIKE concat(?,'%')",
        undef, {bind_values => [$tmpPath]})
        || 0
    );
    $size += (
      $self->queryValue(
        "SELECT SUM(l."
          . $self->reservedWord("size")
          . ") FROM L$entry->{tableName}L l WHERE l.lfn LIKE concat(?,'%') AND l.type='f'",
        undef,
        {bind_values => [$tmpPath]}
        )
        || 0
    );
    $self->insertLFNBookedRemoveDirectory($entry->{lfn}, 'L' . $entry->{tableName} . 'L', $user, $tmpPath)
      or $self->info( "ERROR: Could not add entries $tmpPath to LFN_BOOKED")
      and return;
    $self->delete("L$entry->{tableName}L", "lfn like '$tmpPath%'");
    $entry->{lfn} =~ /^$path/ and push @index, "$entry->{lfn}\%";
  }

  #Clean up index
  if ($#index > -1) {
    $self->deleteFromIndex(@index);
    if (grep(m{^$path/?\%$}, @index)) {
      my $entries = $self->getTablesForEntry($parentdir)
        or $self->info( "Error getting the tables for '$path'")
        and return;
      my $db = ${$entries}[0];
      my ($newdb, $path2) = $self->reconnectToIndex($db->{hostIndex}, $parentdir);
      $newdb
        or $self->info( "Error reconecting to index")
        and return;
      my $tmpPath = "$path/";
      $tmpPath =~ s{^$db->{lfn}}{};
      $newdb->delete("L$db->{tableName}L", "lfn='$tmpPath'");
    }
  }

  $self->fquota_update(-$size, -$count, $user)
    or $self->info( "ERROR: Could not update quotas")
    and return;
  return 1;
}

=item C<moveFolder($source, $target)>

This subroutine moves a whole directory. It checks if part of the directory is in a different database

=cut

#Move folder
sub moveFolder {
  my $self   = shift;
  my $source = shift;
  my $target = shift;
  $target =~ s{/?$}{/};
  $source =~ s{/?$}{/};
  my $user = $self->{CONFIG}->{ROLE};

  my $parent = "$source";
  $parent =~ s{([^/]*[\%][^/]*)/?(.*)$}{};
  my $dbSource = $self->selectTable($parent)
    or $self->info( "Error selecting the database of $parent")
    and return;
  my $tableName_source = "$dbSource->{INDEX_TABLENAME}->{name}";
  my $tablelfn_source  = "$dbSource->{INDEX_TABLENAME}->{lfn}";
  $parent = "$target";
  my $dbTarget = $self->selectTable($parent)
    or $self->info( "Error selecting the database of $parent")
    and return;
  my $tableName_target = "$dbTarget->{INDEX_TABLENAME}->{name}";
  my $tablelfn_target  = "$dbTarget->{INDEX_TABLENAME}->{lfn}";

  my $lfnOnTable_source = "$source";
  $lfnOnTable_source =~ s/$tablelfn_source//;
  my $lfnOnTable_target = "$target";
  $lfnOnTable_target =~ s/$tablelfn_target//;

  if ($tablelfn_source eq $tablelfn_target) {

    #If source and target are in same L#L table then just edit the names
    $dbSource->do(
      "UPDATE $tableName_source SET lfn=REPLACE(lfn,?,?) 
                   WHERE lfn REGEXP ?",
      {bind_values => [ $lfnOnTable_source, $lfnOnTable_target, "^" . $lfnOnTable_source ]}
      )
      or $self->info( "Could not update Catalogue")
      and return;
  } else {

    #If the source and target are in different L#L tables then add in new table and delete from old table
    $dbTarget->do(
          "INSERT INTO $tableName_target(ownerId, replicated, ctime, guidtime, lfn, broken, expiretime, "
        . $dbTarget->reservedWord("size")
        . ", dir, gownerId, type, guid, md5, perm) 
                  SELECT ownerId, replicated, ctime, guidtime, REPLACE(lfn,?,?) as lfn, broken, expiretime, "
        . $dbTarget->reservedWord("size") . ", -1 as dir, gownerId, type, guid, md5, perm 
                  FROM $tableName_source
                  WHERE lfn REGEXP ?",
      {bind_values => [ $lfnOnTable_source, $lfnOnTable_target, "^" . $lfnOnTable_source ]}
      )
      or $self->info( "Error updating database")
      and return;
  }

  #Set the dir entries correctly
  $target =~ s{^$tablelfn_target}{};
  my $targetParent = $target;
  $targetParent =~ s{/[^/]+/?$}{/} or $targetParent = "";
  my $entries = $dbTarget->query(
    "select * from
    (SELECT lfn, entryId, dir from $tableName_target where dir=-1 or lfn='$target' or lfn='$targetParent') dd 
    where lfn like '$target\%/' or lfn='$target' or lfn='$targetParent' order by length(lfn) asc", undef,{bind_values => []});
  foreach my $entry (@$entries) {
    my $update = "update $tableName_target set dir=$entry->{entryId} where " . $self->regexp("lfn", "^$entry->{lfn}\[^/]+/?\$");
    $dbTarget->do($update);
  }
  $dbSource->do("DELETE FROM $tableName_source WHERE lfn REGEXP ?", {bind_values => [ "^" . $lfnOnTable_source ]})
    or $self->info( "Could not update database")
    and return;
  my $result = $dbSource->query("SELECT * FROM $tableName_target WHERE lfn REGEXP ?",
    undef, {bind_values => [ "^" . $lfnOnTable_target ]});
  return 1;
}

#Move file
sub moveFile {
  my $self   = shift;
  my $source = shift;
  my $target = shift;
  my $user   = $self->{CONFIG}->{ROLE};
  my $parent = "$source";
  $parent =~ s{([^/]*[\%][^/]*)/?(.*)$}{};
  my $dbSource = $self->selectTable($parent)
    or $self->info( "Error selecting the database of $parent")
    and return;
  my $tableName_source = "$dbSource->{INDEX_TABLENAME}->{name}";
  my $tablelfn_source  = "$dbSource->{INDEX_TABLENAME}->{lfn}";
  $parent = "$target";
  my $dbTarget = $self->selectTable($parent)
    or $self->info( "Error selecting the database of $parent")
    and return;
  my $tableName_target = "$dbTarget->{INDEX_TABLENAME}->{name}";
  my $tablelfn_target  = "$dbTarget->{INDEX_TABLENAME}->{lfn}";

  my $lfnOnTable_source = "$source";
  $lfnOnTable_source =~ s/$tablelfn_source//;
  my $lfnOnTable_target = "$target";
  $lfnOnTable_target =~ s/$tablelfn_target//;

  if ($tablelfn_source eq $tablelfn_target) {

    #If source and target are in same L#L table then just edit the names
    $dbSource->do("UPDATE $tableName_source SET lfn=? WHERE lfn=?",
      {bind_values => [ $lfnOnTable_target, $lfnOnTable_source ]})
      or $self->info( "Error updating database")
      and return;
  } else {

    #If the source and target are in different L#L tables then add in new table and delete from old table
    $dbTarget->do(
          "INSERT INTO $tableName_target(ownerId, replicated, ctime, guidtime, lfn, broken, expiretime, "
        . $dbTarget->reservedWord("size")
        . ", dir, gownerId, type, guid, md5, perm) 
      SELECT ownerId, replicated, ctime, guidtime, ?, broken, expiretime, "
        . $dbTarget->reservedWord("size")
        . ", dir, gownerId, type, guid, md5, perm FROM $tableName_source WHERE lfn=?",
      {bind_values => [ $lfnOnTable_target, $lfnOnTable_source ]}
      )
      or $self->info( "Error updating database")
      and return;
  }
  my $parentdir = "$lfnOnTable_target";
  $parentdir =~ s{[^/]*$}{};
  $parentdir =~ s/^$tablelfn_target//;
  my $entryId =
    $dbTarget->queryValue("SELECT entryId FROM $tableName_target WHERE lfn=?", undef, {bind_values => [$parentdir]});
  $dbTarget->do("UPDATE $tableName_target SET dir=? WHERE lfn=?", {bind_values => [ $entryId, $lfnOnTable_target ]})
    or $self->info( "Error updating database")
    and return;
  $dbSource->do("DELETE FROM $tableName_source WHERE lfn=?", {bind_values => [$lfnOnTable_source]})
    or $self->info( "Error updating database")
    and return;
  return 1;
}

#Create softlink between two LFNs
sub softLink {
  my $self   = shift;
  my $source = shift;
  my $target = shift;
  my $parent = "$source";
  $parent =~ s{([^/]*[\%][^/]*)/?(.*)$}{};
  my $dbSource = $self->selectTable($parent)
    or $self->info( "Error selecting the database of $parent")
    and return;
  my $tableName_source = "$dbSource->{INDEX_TABLENAME}->{name}";
  my $tablelfn_source  = "$dbSource->{INDEX_TABLENAME}->{lfn}";
  $parent = "$target";
  my $dbTarget = $self->selectTable($parent)
    or $self->info( "Error selecting the database of $parent")
    and return;
  my $tableName_target  = "$dbTarget->{INDEX_TABLENAME}->{name}";
  my $tablelfn_target   = "$dbTarget->{INDEX_TABLENAME}->{lfn}";
  my $lfnOnTable_source = "$source";
  $lfnOnTable_source =~ s/$tablelfn_source//;
  my $lfnOnTable_target = "$target";
  $lfnOnTable_target =~ s/$tablelfn_target//;

  if ($tablelfn_source eq $tablelfn_target) {

    #If source and target are in same L#L table then just edit the names
    $dbTarget->do(
          "INSERT INTO $tableName_target(ownerId, replicated, ctime, guidtime, lfn, broken, expiretime, "
        . $dbTarget->reservedWord("size")
        . ", dir, gownerId, type, guid, md5, perm) 
      SELECT ownerId, replicated, ctime, guidtime, ?, broken, expiretime, "
        . $dbTarget->reservedWord("size") 
        . ", dir, gownerId, 'l', guid, md5, perm FROM $tableName_source WHERE lfn=?",
      {bind_values => [ $lfnOnTable_target, $lfnOnTable_source ]}
      )
      or $self->info( "Error updating database", "[updateDatabse]")
      and return;
  } else {

    #If the source and target are in different L#L tables then add in new table and delete from old table
    $dbTarget->do(
          "INSERT INTO $tableName_target(ownerId, replicated, ctime, guidtime, lfn, broken, expiretime, "
        . $dbTarget->reservedWord("size")
        . ", dir, gownerId, type, guid, md5, perm) 
      SELECT ownerId, replicated, ctime, guidtime, ?, broken, expiretime, "
        . $dbTarget->reservedWord("size")
        . ", dir, gownerId, 'l', guid, md5, perm FROM $tableName_source WHERE lfn=?",
      {bind_values => [ $lfnOnTable_target, $lfnOnTable_source ]}
      )
      or $self->info( "Error updating database")
      and return;
  }
  my $parentdir = "$lfnOnTable_target";
  $parentdir =~ s{[^/]*$}{};
  my $entryId =
    $dbTarget->queryValue("SELECT entryId FROM $tableName_target WHERE lfn=?", undef, {bind_values => [$parentdir]});
  $self->info("$parentdir : $entryId");
  $dbTarget->do("UPDATE $tableName_target SET dir=? WHERE lfn=?", {bind_values => [ $entryId, $lfnOnTable_target ]})
    or $self->info( "Error updating database")
    and return;
  return 1;
}

sub tabCompletion {
  my $self      = shift;
  my $entryName = shift;
  my $tableName = $self->{INDEX_TABLENAME}->{name};
  my $lfn       = $self->{INDEX_TABLENAME}->{lfn};
  my $dirName   = $entryName;
  $dirName   =~ s{[^/]*$}{};
  $dirName   =~ s{^$lfn}{};
  $entryName =~ s{^$lfn}{};
  my $dir = $self->queryValue("SELECT entryId from $tableName where lfn=?", undef, {bind_values => [$dirName]});
  $dir or return;
  my $rfiles = $self->queryColumn(
    "SELECT concat('$lfn',lfn) from $tableName where dir=$dir and " . $self->regexp("lfn", "^$entryName\[^/]*\/?\$"));
  return @$rfiles;

}
##############################################################################
##############################################################################
sub actionInIndex {
  my $self   = shift;
  my $action = shift;

  my $tempHost = {
    'organisation' => undef,
    'db'           => $self->{DB},
    'address'      => $self->{HOST},
    'driver'       => $self->{DRIVER}
  };
  $self->info("Updating the INDEX table of  $tempHost->{db}");
  $self->do($action) or $self->debug(2, "Warning: Error doing $action");
  $DEBUG and $self->debug(2, "Everything is done!!");

  return 1;
}


sub getAllIndexes {
  my $self = shift;
  return $self->query("SELECT * FROM INDEXTABLE");

}

sub getNumEntryIndexes {
  #edited by dushyant
  my $self = shift;
  my $option = shift;
  ($option) or $option=0;

  if($option==1){
    $self->do("DROP TABLE if exists temp_LL");
    $self->do("CREATE TABLE temp_LL (tn int unique, num int, lfn varchar(255))");
  }
  
  my $q = $self->query("SELECT tableName,lfn FROM INDEXTABLE order by tableName");
  #my $lol = scalar @$q;
  #$self->info("Checking scalar value :: $lol");
  #use Data::Dumper;
  #$self->info(Dumper(@$q));
  #$self->info(Dumper($self->query("SELECT tableName,lfn FROM INDEXTABLE")));
  my @tnames = ();
  my @result = ();
  foreach my $row(@$q)
  {
        my $tn = $row->{tableName};
        my $lfn = "$row->{lfn}";
        push(@tnames,$tn);
        my $newT = 'L'.$tn.'L';
        my $q1 = "SELECT COUNT(*) from ".$newT." ";
        my $num = $self->queryValue($q1);
        if($option ==1 ){
          $self->do("INSERT INTO temp_LL VALUES ($tn,$num,'$lfn')");
        }
        push(@result,$num);
  }
  if($option == 1){
      my $qu=$self->query("SELECT * from temp_LL ORDER BY num ASC ");
      my @tnames1 = ();
      my @result1 = ();
      foreach my $row(@$qu){
        push(@tnames1,$row->{tn});
        push(@result1,$row->{num});
      }
      #$self->do("DROP TABLE if exists temp_LL");
      return (@tnames1,@result1) ;
  }
  return (@tnames,@result) ;
  #return %res ;
}

sub getNumEntryGUIDINDEX {
  #edited by dushyant
  my $self = shift;
  my $q = $self->query("SELECT tableName FROM GUIDINDEX order by tableName");
  my @tnames = ();
  my @result = ();
  foreach my $row(@$q)
  {
        my $tn = $row->{tableName};
        push(@tnames,$tn);
        my $newT = 'G'.$tn.'L';
        my $q1 = "SELECT COUNT(*) from ".$newT." ";
        my $temp = $self->queryValue($q1);
        push(@result,$temp);
  }
  return (@tnames,@result) ;
  #return %res ;
}

sub getNumEntryGUIDINDEX_PFN {
  my $self = shift;
  my $q = $self->query("SELECT tableName FROM GUIDINDEX order by tableName");
  my @tnames = ();
  my @result = ();
  foreach my $row(@$q)
  {
        my $tn = $row->{tableName};
        push(@tnames,$tn);
        my $newT = 'G'.$tn.'L_PFN';
        my $q1 = "SELECT COUNT(*) from ".$newT." ";
        my $temp = $self->queryValue($q1);
        push(@result,$temp);
  }
  return (@tnames,@result) ;
  #return %res ;
}


=item C<copyDirectory($source, $target)>

This subroutine copies a whole directory. It checks if part of the directory is in a different database

=cut

sub copyDirectory {
  my $self    = shift;
  my $options = shift;
  my $source  = shift;
  my $target  = shift;
  $source and $target
    or $self->info("Not enough arguments in copyDirectory", 1111)
    and return;
  $source =~ s{/?$}{/};
  $target =~ s{/?$}{/};
  $DEBUG and $self->debug(1, "Copying a directory ($source to $target)");

  my $sourceHosts = $self->getTablesForEntry($source);

  my $sourceInfo = $self->getIndexHost($source);

  my $targetHost  = $self->getIndexHost($target);
  my $targetIndex = $targetHost->{hostIndex};
  my $targetTable = "L$targetHost->{tableName}L";
  my $targetLFN   = $targetHost->{lfn};

  my $user = $options->{user} || $self->{VIRTUAL_ROLE};

  #Before doing this, we have to make sure that we are in the right database
  my ($targetDB, $Path2) = $self->reconnectToIndex($targetIndex) or return;

  my $sourceLength = length($source) + 1;

  my $targetName = $targetDB->existsLFN($target);
  if ($targetName) {
    if ($targetName !~ m{/$}) {
      $self->info("cp: cannot overwrite non-directory `$target' with directory `$source'", "222");
      return;
    }
    my $sourceParent = $source;
    $sourceParent =~ s {/([^/]+/?)$}{/};
    $self->info("Copying into an existing directory (parent is $sourceParent)");
    $sourceLength = length($sourceParent) + 1;
    $options->{k} and $sourceLength = length($source) + 1;
  }
  my $beginning = $target;
  $beginning =~ s/^$targetLFN//;

  my $select =
      "insert into $targetTable(lfn,ownerId,gownerId,"
    . $self->reservedWord("size")
    . ",type,guid,guidtime,perm,dir) select distinct concat ('$beginning',substr(concat('";
  my $select2 =
      "', t1.lfn), $sourceLength)) as lfn, '$user', '$user',t1."
    . $self->reservedWord("size")
    . ",t1.type,t1.guid,t1.guidtime,t1.perm,-1 ";
  my @values = ();

  my $binary2string = $self->binary2string;
  $binary2string =~ s/guid/t1.guid/;
  foreach my $entry (@$sourceHosts) {
    $DEBUG and $self->debug(1, "Copying from $entry to $targetIndex and $targetTable");
    my ($db, $Path2) = $self->reconnectToIndex($entry->{hostIndex});

    my $tsource = $source;
    $tsource =~ s{^$entry->{lfn}}{};
    my $like = "t1.replicated=0";

    my $table = "L$entry->{tableName}L";
    my $join =
"$table t1,$table t2 where t2.type='d' and (t1.dir=t2.entryId or t1.entryId=t2.entryId)  and t2.lfn like '$tsource%'";
    if ($targetIndex eq $entry->{hostIndex}) {
      $options->{k} and $like .= " and t1.lfn!='$tsource'";
      $DEBUG and $self->debug(1, "This is easy: from the same database");

      # we want to copy the lf, which in fact would be something like
      # substring(concat('$entry->{lfn}', lfn), length('$sourceIndex'))
      $self->do("$select$entry->{lfn}$select2 from $join and $like");

    } else {
      $DEBUG and $self->debug(1, "This is complicated: from another database");
      my $query =
          "SELECT distinct concat('$beginning', substr(concat('$entry->{lfn}',t1.lfn), $sourceLength )) as lfn, t1."
        . $self->reservedWord("size")
        . ",t1.type, $binary2string  as guid ,t1.perm FROM $join and $like";
      $options->{k}
        and $query =
        "select * from ($query) d where lfn!=concat('$beginning', substr('$entry->{lfn}$tsource', $sourceLength ))";
      my $entries = $db->query($query);
      foreach my $files (@$entries) {
        my $guid = "NULL";
        (defined $files->{guid}) and $guid = "$files->{guid}";

        $files->{lfn} =~ s{^}{};
        $files->{lfn} =~ s{^$targetLFN}{};
        push @values,
" ( '$files->{lfn}',  '$user', '$user', '$files->{size}', '$files->{type}', string2binary('$guid'), string2date('$guid'),'$files->{perm}', -1)";
      }
    }
  }

  if ($#values > -1) {
    my $insert =
        "INSERT into $targetTable(lfn,ownerId,gownerId,"
      . $targetDB->reservedWord("size")
      . ",type,guid,guidtime,perm,dir) values ";

    #$insert .= join (",", @values);
    #$targetDB->do($insert);
    foreach (@values) {
      $targetDB->do($insert . " " . $_);
    }
  }

  $target =~ s{^$targetLFN}{};
  my $targetParent = $target;
  $targetParent =~ s{/[^/]+/?$}{/} or $targetParent = "";
  $DEBUG and $self->debug(1, "We have inserted the entries. Now we have to update the column dir");

  #and now, we should update the entryId of all the new entries
  #This query is divided in a subquery to profit from the index with the column dir
  my $entries = $targetDB->query(
"select * from (SELECT lfn, entryId from $targetTable where dir=-1 or lfn='$target' or lfn='$targetParent') dd where lfn like '$target\%/' or lfn='$target' or lfn='$targetParent'"
  );
  foreach my $entry (@$entries) {
    $DEBUG and $self->debug(1, "Updating tbe entry $entry->{lfn}");
    my $update = "update $targetTable set dir=$entry->{entryId} where dir=-1 and "
      . $self->regexp("lfn", "^$entry->{lfn}\[^/]+/?\$");
    $targetDB->do($update);

  }

  $DEBUG and $self->debug(1, "Directory copied!!");
  return 1;
}

=item C<moveLFNs($lfn, $toTable)>

This function moves all the entries under a directory to a new table
A new table is always created.

Before calling this function, you have to be already in the right database!!!
You can make sure that you are in the right database with a call to checkPermission

=cut

sub moveLFNs {
  my $self    = shift;
  my $lfn     = shift;
  my $options = shift || {};

  $DEBUG and $self->debug(1, "Starting  moveLFNs, with $lfn ");

  my $toTable;

  my $isIndex = $self->queryValue("SELECT 1 from INDEXTABLE where lfn=?", undef, {bind_values => [$lfn]});

  my $entry     = $self->getIndexHost($lfn) or $self->info("Error getting the info of $lfn") and return;
  my $fromTable = $entry->{tableName};
  my $fromLFN   = $entry->{lfn};
  my $toLFN     = $lfn;

  if ($options->{b}) {
    $isIndex
      or $self->info("We are supposed to move back, but the entry is not in a different table...")
      and return;
    $self->info("We have to move back!!");

    my $parent = $lfn;
    $parent =~ s{/[^/]*/?$}{/};
    my $entryP = $self->getIndexHost($parent);
    $toTable = $entryP->{tableName};
    ($entryP->{hostIndex} eq $entry->{hostIndex})
      or $self->info("We can only move back if the entries are in the same database...")
      and return;

    $toLFN = $entryP->{lfn};

  } else {
    $isIndex and $self->info("This is already in a different table...") and return;
    $toTable = $self->getNewDirIndex();
  }
  defined $toTable or $self->info("Error getting the name of the new table") and return;

  $toTable   =~ /^(\d+)*$/ and $toTable   = "L${toTable}L";
  $fromTable =~ /^(\d+)*$/ and $fromTable = "L${fromTable}L";

  $self->lock(
"$toTable WRITE, $toTable as ${toTable}d READ,  $toTable as ${toTable}r READ, $fromTable as ${fromTable}d READ, $fromTable as ${fromTable}r READ, $fromTable"
  );
  $self->renumberLFNtable($toTable, {'locked', 1});
  my $min = $self->queryValue("select max(entryId)+1 from $toTable");
  $min or $min = 1;

  $self->renumberLFNtable($fromTable, {'locked', 1, 'min', $min});

  #ok, this is the easy case, we just copy into the new table
  my $columns =
      "entryId,md5,ownerId,gownerId,replicated,expiretime,"
    . $self->reservedWord("size") . ",dir,"
    . $self->reservedWord("type")
    . ",guid,perm";
  my $tempLfn = $lfn;
  $tempLfn =~ s{$fromLFN}{};

  #First, let's insert the entries in the new table
  if (
    !$self->do(
"INSERT into $toTable($columns,lfn) select $columns,substr(concat('$fromLFN', lfn), length('$toLFN')+1) from $fromTable where lfn like '${tempLfn}%' and lfn not like '' "
    )
    ) {
    $self->unlock();
    return;
  }
  $self->unlock();

  ($isIndex) and $self->deleteFromIndex($lfn);
  if ($options->{b}) {
    my $newLfn = $lfn;
    $newLfn =~ s/^$toLFN//;

    my $oldDir = $self->queryValue("select entryId from $fromTable where lfn=''");
    my $newDir = $self->queryValue("select entryId from $toTable where lfn=?", undef, {bind_values => [$newLfn]});
    $self->do("update $toTable set replicated=0 where replicated=1 and lfn=?", {bind_values => [$newLfn]});
    $self->do("update $toTable set dir=? where dir=?", {bind_values => [ $newDir, $oldDir ]});
    $self->do("drop table $fromTable");
    $self->do("drop table ${fromTable}_QUOTA");
    $self->do("drop table ${fromTable}_broken");
  } else {
    if (!$self->insertInIndex($toTable, $lfn)) {
      $self->delete($toTable, "lfn like '${tempLfn}%'");
      return;
    }
    if (!$isIndex) {

      #Finally, let's delete the old table;
      $self->delete($fromTable, "lfn like '${tempLfn}_%'");
      $self->update($fromTable, {replicated => 1}, "lfn='$tempLfn'");
    } else {
      $self->delete($fromTable, "lfn like '${tempLfn}%'");
    }
    my $user = $self->queryValue("select Username from $toTable JOIN USERS ON ownerId=uId where lfn=''");
    $self->info("And now, let's give access to $user to '$toTable");
  }
  return 1;
}


### Host functions

sub getIndexHost {
  my $self = shift;
  my $lfn  = shift;
  $lfn =~ s{/?$}{/};

  #my $options={bind_values=>[$lfn]};
  my $query =
    "SELECT tableName,lfn FROM INDEXTABLE where lfn=substr('$lfn',1, length(lfn))  order by length(lfn) desc ";
  $query = $self->paginate($query, 1, 0);

  # return $self->queryRow($query, undef, $options);
  return $self->queryRow($query, undef, undef);
}

### Groups functions

sub getUserGroups {
  my $self = shift;
  my $user = shift
    or $self->{LOGGER}->error("Catalogue", "In getUserGroups user is missing")
    and return;
  my $prim = shift;
  defined $prim or $prim = 1;

  my $cache = AliEn::Util::returnCacheValue($self, "groups-$user-$prim");
  if (defined $cache) {
    $DEBUG and $self->debug(2, "$$ Returning the value from the cache ($cache)");
    return $cache;
  }
  $DEBUG and $self->debug(2, "In getUserGroups fetching groups for user $user");
  my $data = $self->queryColumn("SELECT Groupname from UGMAP JOIN USERS ON uId=Userid JOIN GRPS ON gId=Groupid where Username='$user' and PrimaryGroup = $prim ");
  #my $data = $self->queryColumn("SELECT Groupname ,Userid from UGMAP JOIN USERS ON uId=Userid JOIN GRPS ON gId=Groupid where Username='$user' and PrimaryGroup = $prim ");
  AliEn::Util::setCacheValue($self, "groups-$user-$prim", $data);
  return $data;
}

sub getAllFromGroups {
  my $self = shift;
  my $attr = shift || "*";

  $DEBUG and $self->debug(2, "In getAllFromGroups fetching attributes $attr for all tuples from UGMAP table");
  $self->query("SELECT $attr FROM UGMAP");
}

sub insertIntoGroups {
  my $self  = shift;
  my $user  = shift;
  my $group = shift;
  my $var   = shift;

  $self->_do("INSERT INTO USERS (Username) SELECT '$user' FROM DUAL WHERE NOT EXISTS (SELECT * FROM USERS WHERE Username='$user')");
  $self->_do("INSERT INTO GRPS (Groupname) SELECT '$group' FROM DUAL WHERE NOT EXISTS (SELECT * FROM GRPS WHERE Groupname='$group')");
  my $userid = $self->getOwnerId($user);
  my $groupid = $self->getGownerId($group);
  $DEBUG and $self->debug(2, "In insertIntoGroups inserting new data");
  $self->_do(
    "INSERT INTO UGMAP ( Userid, Groupid, PrimaryGroup) SELECT '$userid','$groupid','$var' FROM DUAL WHERE NOT  EXISTS 
    (SELECT  * FROM UGMAP WHERE Userid = '$userid' AND Groupid = '$groupid' AND PrimaryGroup = '$var')" );
 
  #$self->_do("INSERT IGNORE INTO USERS (Username)  VALUES ('$user') "); 
  #$self->_do("INSERT IGNORE INTO GRPS (Groupname)  VALUES ('$group') "); 
  #$self->_do("INSERT IGNORE INTO GROUPS (Username, Groupname, PrimaryGroup) values ('$user','$group','$var')");
}

###	Environment functions
#	TAG functions

# quite complicated manoeuvers in Catalogue/Tag.pm - f_addTagValue
# difficult to merge with the others
#sub insertDirtagVarsFileValuesNew {
sub insertTagValue {
  my $self   = shift;
  my $action = shift;
  my $path   = shift;
  my $tag    = shift;
  my $file   = shift;
  my $rdata  = shift;

  my $tableName = $self->getTagTableName($path, $tag, {parents => 1});
  $tableName
    or $self->info("Error: we can't find the name of the table", 1)
    and return;
  my $fileName = "$path$file";

  #  $tableName =~ /T[0-9]+V$tag$/ or $fileName="$path$fileName";

  my $finished = 0;
  my $result;
  $self->debug(1, "Ready to insert in the table $tableName and $fileName");
  if ($action eq "update") {
    $self->info("We want to update the latest entry (if it exists)");
    my $maxEntryId =
      $self->queryValue("SELECT MAX(entryId) FROM $tableName where " . $self->reservedWord("file") . "=?",
      undef, {bind_values => [$fileName]});
    $DEBUG and $self->debug(2, "Got $maxEntryId");

    if ($maxEntryId) {
      $DEBUG and $self->debug(2, "WE ARE SUPPOSED TO ALTER THE ENTRY $maxEntryId");
      $result = $self->update($tableName, $rdata, $self->reservedWord("file") . "='$fileName' and entryId=$maxEntryId");
      $finished = 1;
    }
  }

  if (!$finished) {
    $DEBUG and $self->debug(2, "Ok, we have to add the entry");

    $rdata->{file} = $fileName;
    my @keys = keys %{$rdata};
    $self->info("We have $rdata and @keys");
    $result = $self->insert($tableName, $rdata);
    if ($result) {
      $self->info("Let's make sure that we only have one entry");

      #$self->delete($tableName, "file='$fileName' and entryId<".$self->getLastId());
      $self->delete($tableName,
        $self->reservedWord("file") . "='$fileName' and entryId<" . $self->getLastId($tableName));
    }
  }

  return $result;
}

sub getTags {
  my $self      = shift;
  my $directory = shift;
  my $tag       = shift;

  my $tableName = $self->getTagTableName($directory, $tag, {parents => 1})
    or $self->info("In getFieldsFromTagEx table name is missing")
    and return;

  my $columns = shift || "*";
  my $where   = shift || "";
  my $options = shift || {};
  my $query =
      "SELECT $columns from $tableName t where t.entryId=(select max(entryId) from $tableName t2 where t."
    . $self->reservedWord("file") . "=t2."
    . $self->reservedWord("file")
    . ") and $where";

  if ($options->{filename}) {

    #    $query.=" and entryId=(select max(entryId) from $tableName where file=?)";
    $query .= " and " . $self->reservedWord("file") . "=? ";
    my @list = ();
    $options->{bind_values} and @list = @{$options->{bind_values}};
    push @list, $options->{filename};
    $options->{bind_values} = \@list;

  }
  if ($options->{limit}) {
    $query = $self->paginate($query, $options->{limit}, 0);
  }
  return $self->query($query, undef, $options);
}

sub getTagNamesByPath {
  my $self = shift;
  my $path = shift;

  $self->queryColumn("SELECT tagName from TAG0 where path=?", undef, {bind_values => [$path]});
}

sub getAllTagNamesByPath {
  my $self    = shift;
  my $path    = shift;
  my $options = shift || {};

  return $self->dbGetAllTagNamesByPath($path, $options);
}

sub getFieldsByTagName {
  my $self      = shift;
  my $tagName   = shift;
  my $fields    = shift || "*";
  my $distinct  = shift;
  my $directory = shift;
  my @bind      = ($tagName);
  my $sql       = "SELECT ";
  $distinct and $sql .= "DISTINCT ";

  $sql .= "  $fields FROM TAG0 WHERE tagName=?";
  if ($directory) {
    $sql .= " and (path like concat(?, '%') or ? like concat(path,'%')) ";
    push @bind, $directory, $directory;
  }

  $self->query($sql, undef, {bind_values => \@bind});
}

sub getTagTableName {
  my $self        = shift;
  my $path        = shift;
  my $tag         = shift;
  my $options     = shift || {};
  my $query       = "path=?";
  my $whole_query = "";
  if ($options->{parents}) {
    $query = " ? like concat(path, '%') order by path desc";

    $whole_query = $self->paginate("SELECT tableName from TAG0 where tagName=? and $query", 1, 0);    #limit and offset

  } else {
    $whole_query = "SELECT tableName from TAG0 where tagName=? and $query";
  }
  my $res = $self->queryValue($whole_query, undef, {bind_values => [ $tag, $path ]});
  return $res;

  #my $query="path=?";
  #$options->{parents} and $query="? like concat(path, '%') order by path desc limit 1";
  #my $tableName = $self->queryValue("SELECT tableName from TAG0 where tagName=? and $query",undef,
  #  {bind_values=>[$tag, $path]});
  #return $tableName;
}

sub deleteTagTable {
  my $self = shift;
  my $tag  = shift;
  my $path = shift;
  $DEBUG and $self->debug(2, "In deleteTagTable");

  my $done;
  my $tagTableName = $self->getTagTableName($path, $tag);
  $tagTableName or $self->info("Error trying to delete the tag table of $path and $tag") and return 1;
  my $user = $self->{USER};
  $DEBUG and $self->debug(2, "Deleting entries from T${user}V$tag");
  my $query =
      "DELETE FROM $tagTableName WHERE "
    . $self->reservedWord("file")
    . " like '$path%' and "
    . $self->reservedWord("file")
    . " not like '$path/%/%'";

  $self->_do($query);

  $done = $self->delete("TAG0", "path='$path' and tagName='$tag'");
  $done and $DEBUG and $self->debug(2, "Done with $done");
  return $done;
}

sub insertIntoTag0 {
  my $self      = shift;
  my $directory = shift;
  my $tagName   = shift;
  my $tableName = shift;
  my $user      = shift || $self->{CONFIG}->{ROLE};

  $self->insert(
    "TAG0",
    { path      => $directory,
      tagName   => $tagName,
      tableName => $tableName,
      user      => $user
    }
  );
}

=item getDiskUsage($lfn)

Gets the disk usage of an entry (either file or directory)

=cut

sub getDiskUsage {
  my $self    = shift;
  my $lfn     = shift;
  my $options = shift;

  my $size = 0;
  if ($lfn =~ m{/$}) {
    $DEBUG and $self->debug(1, "Checking the diskusage of directory $lfn");
    my $pattern = $lfn;
    $pattern =~ s{^$self->{INDEX_TABLENAME}->{lfn}}{};
    my $where = "where lfn like '$pattern%'";
    $self->{INDEX_TABLENAME}->{lfn} =~ m{^$lfn} and $where = "where 1=1";
    $options =~ /f/ and $where .= " and type='f'";
    my $table = $self->{INDEX_TABLENAME}->{name};
    my $partialSize = $self->queryValue(
      "SELECT sum(" . $self->reservedWord("size") . ") from $table $where");
    $DEBUG and $self->debug(1, "Got size $partialSize");
    $size += $partialSize;
  } else {
    my $table = "$self->{INDEX_TABLENAME}->{name}";
    $lfn =~ s{^$self->{INDEX_TABLENAME}->{lfn}}{};
    $DEBUG and $self->debug(1, "Checking the diskusage of file $lfn");
    $size = $self->queryValue("SELECT " . $self->reservedWord("size") . " from $table where lfn='$lfn'");
  }

  return $size;
}

=item DropEmtpyDLTables

deletes the tables DL that are not being used

=cut

sub DropEmptyDLTables {
  my $self = shift;
  $self->info("Deleting the tables that are not being used");

  #updating the D0 of all the databases

  my $tables = $self->queryColumn("show tables like 'D\%L'")
    or return;
  foreach my $t (@$tables) {
    $self->info("Checking $t");
    $t =~ /^D(\d+)L$/ or $self->info("skipping...") and next;
    my $number = $1;
    my $n = $self->queryValue("select count(*) from $t") and next;
    $self->info("We have to drop $t!! (there are $n in $t)");
    my $indexes = $self->queryColumn("SELECT lfn from INDEXTABLE where tableName=$number");
    if ($indexes) {
      foreach my $i (@$indexes) {
        $self->info("Deleting index $i");
        $self->deleteFromIndex($i);
      }
    }
    $self->do("DROP TABLE $t");
  }

  $DEBUG and $self->debug(2, "Everything is done!!");

  return 1;
}


sub selectTable {
  my $self = shift;
  my $path = shift;

  #get table for the lfn entry from indextable
  my $entry = $self->getIndexHost($path);
  $entry or $self->info("The path $path is not in the catalogue ") and return;

  my $tableName = "L$entry->{tableName}L";
  $DEBUG and $self->debug(1, "We want to connect to $tableName");

  #set INDEXTBLENAME to that of the file in question
  $self->setIndexTable($tableName, $entry->{lfn});
  
  return $self;
}

sub getPathPrefix {
  my $self  = shift;
  my $table = shift;
  my $host  = shift;
  $table =~ s{^D(\d+)L}{$1};
  return $self->queryValue("SELECT lfn from INDEXTABLE where tableName='$table' and hostIndex='$host'");
}

sub findLFN {
  my $self = shift;
  my ($path, $file, $refNames, $refQueries, $refUnions, %options) = @_;

  #first, let's take a look at the host that we want

  my $rtables = $self->getTablesForEntry($path)
    or $self->info("Error getting the tables for '$path'")
    and return;

  my @result = ();
  my @done   = ();
  foreach my $table (@$rtables) {
    
    grep (/^$table->{tableName}$/, @done) and next;
    push @done, $table->{tableName};
    my $localpath = $table->{lfn};

    $DEBUG and $self->debug(1, "Looking in table $table->{tableName} (path $path)");

    push @result, $self->internalQuery($table, $path, $file, $refNames, $refQueries, $refUnions, \%options);
  }
  return \@result;
}

# This subroutine looks for files that satisfy certain criteria in the
# current database.
# Input:
#    $path: directory where the 'find' started'
#    $name: name of files that we are looking for
#    $refNames: reference to a list of paths with the tags
#    $refQueries: reference to a list of queries of metadata"
#    $refunion: reference to a list of unions between the queries
#    $options: d->return also the directories
# Output:
#    list of file that satisfy all the criteria
sub internalQuery {
  my $self     = shift;
  my $refTable = shift;
  my $path     = shift;
  my $file     = shift;

  my $refNames   = shift;
  my $refQueries = shift;
  my $refUnions  = shift;
  my $options    = shift;
  my $selimit    = shift || "";

  my $indexTable = "L$refTable->{tableName}L";
  my $indexLFN   = $refTable->{lfn};
  my @tagNames   = @{$refNames};
  my @tagQueries = @{$refQueries};
  my @unions     = ("and", @{$refUnions});

  my @paths   = ();
  my @queries = ();

  my @dirresults;

  my @joinQueries;

  foreach my $f (@$file) {
    if ($f ne "\\") {
      my $searchP = $path;
      my $concat  = "concat('$refTable->{lfn}', lfn)";
      $searchP =~ s/^$refTable->{lfn}// and $concat = "lfn";
      my $d = ("WHERE $concat LIKE '$searchP%$f%' and replicated=0");

      # $options->{d} or $d.=" and right(lfn,1) != '/' and lfn!= \"\"";
      $options->{d} or $d .= " and SUBSTR(lfn,-1) != '/' and ( lfn !=\'\' or lfn is null)";
      push @joinQueries, $d;
    } else {

      # query an exact file name
      push @joinQueries, ("WHERE concat('$refTable->{lfn}', lfn)='$path'");
    }
  }

  #First, let's construct the sql statements that will select all the files
  # that we want.
  $self->debug(1, "READY TO START LOOKING FOR THE TAGS");
  my $tagsDone = {};
  foreach my $tagName (@tagNames) {
    my $union      = shift @unions;
    my $query      = shift @tagQueries;
    my @newQueries = ();

    if ($tagsDone->{$tagName}) {
      $self->info("The tag $tagName has already been selected. Just add the constraint");
      foreach my $oldQuery (@joinQueries) {
        push @newQueries, "$oldQuery $union $query";
      }
    } else {
      $DEBUG and $self->debug(1, "Selecting directories with tag $tagName");

      #Checking which directories have that tag defined
      my $tables = $self->getFieldsByTagName($tagName, "tableName", 1, $refTable->{lfn});
      $tables and $#{$tables} != -1
        or $self->info("Error: there are no directories with tag $tagName in $self->{DATABASE}->{DB}")
        and return;
      foreach (@$tables) {
        my $table = $_->{tableName};
        $self->debug(1, "Doing the new table $table");
        foreach my $oldQuery (@joinQueries) {

          #This is the query that will get all the results. We do a join between
          #the D0 table, and the one with the metadata. There will be two queries
          #like these per table with that metadata.
          #The first query gets files under directories with that metadata.
          # It is slow, since it has to do string comperation
          #The second query gets files with that metadata.
          # (this part is pretty fast)

          if ($options->{'m'}) {
            $self->info("WE WANT EXACT FILES!!");
            my $l = length($refTable->{lfn});

#  push @newQueries, " JOIN $table $oldQuery $union $table.$query and substring($table.file,$l+1)=l.lfn  and left($table.file,$l)='$refTable->{lfn}'";
            push @newQueries,
                " , $table $oldQuery $union $table.$query and substr($table."
              . $self->reservedWord("file")
              . ",$l+1)=l.lfn  and substr($table."
              . $self->reservedWord("file")
              . ",1,$l) ='$refTable->{lfn}'";
          } else {

#    push @newQueries, " JOIN $table $oldQuery $union $table.$query and $table.file like '%/' and concat('$refTable->{lfn}', l.lfn) like concat( $table.file,'%') ";
            push @newQueries,
                " , $table $oldQuery $union $table.$query and $table."
              . $self->reservedWord("file")
              . "  like '%/' and concat('$refTable->{lfn}', l.lfn) like concat( $table."
              . $self->reservedWord("file")
              . ",'%') ";
            my $length = length($refTable->{lfn}) + 1;

#    push @newQueries, " JOIN $table $oldQuery $union $table.$query and l.lfn=substring($table.file, $length) and left($table.file, $length-1)='$refTable->{lfn}'";
            push @newQueries,
                " , $table $oldQuery $union $table.$query and l.lfn=substr($table."
              . $self->reservedWord("file")
              . ", $length) and substr($table."
              . $self->reservedWord("file")
              . ",1, $length-1)  ='$refTable->{lfn}'";

          }
        }
      }
    }
    @joinQueries = @newQueries;
    $tagsDone->{$tagName} = 1;
  }
  my $order  = " ORDER BY l.lfn";
  my $limit  = "";
  my $offset = "";
  $options->{'s'} and $order  = "";
  $options->{'y'} and $order  = "";
  $options->{l}   and $limit  = " $options->{l}";
  $options->{o}   and $offset = "  $options->{o}";

  my $b = $self->binary2string;
  $b =~ s/guid/l.guid/;

  map {
s/^(.*)$/$self->paginate("SELECT l.entryId,ctime,ownerId,replicated,guidtime, jobId, broken, expiretime,dir, ".$self->reservedWord("size").", gownerId,  ".$self->reservedWord("type")." ,md5,perm,concat('$refTable->{lfn}', lfn) as lfn,
$b as guid from $indexTable l $1 $order", $limit, $offset)/e
  } @joinQueries;

  $self->debug(1, "We have to do $#joinQueries +1 to find out all the entries");
  if ($options->{selimit}) {
    $self->debug(1, "Displaying only the files in a particular se");
    my $GUIDList = $self->getPossibleGuidTables($self->{INDEX_TABLENAME}->{name});
    my @newQueries;
    foreach my $entry (@$GUIDList) {
      foreach my $query (@joinQueries) {
        my $q = $query;
        $q =~ s/ from / from $entry->{db}.G$entry->{tableName}L g,$entry->{db}.G$entry->{tableName}L_PFN p, /;
        $q =~ s/ where / where g.guid=l.guid and p.guidid=g.guidid and senumber='$options->{selimit}' and /i;
        push @newQueries, $q;
      }
    }
    @joinQueries = @newQueries;
  }

  #Finally, let's do all the queries:
  my @result;
  foreach my $q (@joinQueries) {
    if ($options->{'y'}) {
      my $t = "";
      $q =~ /((JOIN)|,) (\S+VCDB) /m and $t = $3;
      if ($t) {
        $self->info("WE ARE RETRIEVING ONLY THE BIGGEST METADADATA from $t");
        $q =~ s/select .*? from /select substr(max(version_path),10) lfn from (SELECT version_path,dir_number from /si;
        $q .= ")d  group by dir_number";
      }
    }
    $DEBUG and $self->debug(1, "Doing the query $q");

    #    print "SKIPPING THE QUERIES '$q'\n";
    my $query = $self->query($q);
    push @result, @$query;
  }
  return @result;

}

sub setExpire {
  my $self    = shift;
  my $lfn     = shift;
  my $seconds = shift;
  defined $seconds or $seconds = "";
  my $table = $self->{INDEX_TABLENAME}->{name};
  $lfn =~ s{^$self->{INDEX_TABLENAME}->{lfn}}{};

  my $seconds2 = $self->_timeUnits($seconds);
  my $expire   = "now()+ $seconds2";
  if ($seconds =~ /^-1$/) {
    $expire = "null";
  } else {
    $seconds =~ /^\d+$/
      or $self->info("The number of seconds ('$seconds') is not a number")
      and return;
  }
  return $self->_do("UPDATE $table SET expiretime=$expire WHERE lfn='$lfn'");
}


sub LFN_createCollection {
  my $self   = shift;
  my $insert = shift;
  $insert->{type} = 'c';
  $self->_createEntry($insert, @_) or return;

  if (!$self->insert("COLLECTIONS", {collGUID => $insert->{guid}}, {functions => {collGUID => 'string2binary'}})) {
    $self->debug(2, "Here we have to remove the entry");
    my $tableRef  = shift             || {};
    my $tableName = $tableRef->{name} || $self->{INDEX_TABLENAME}->{name};
    my $tableLFN  = $tableRef->{lfn}  || $self->{INDEX_TABLENAME}->{lfn};
    $insert->{lfn} =~ s{^$tableLFN}{};
    $self->delete($tableName, "lfn='$insert->{lfn}'");
    return;
  }
  return 1;
}

sub _createEntry {
  my $self     = shift;
  my $insert   = shift;
  my $tableRef = shift || {};

  my $tableName = $tableRef->{name} || $self->{INDEX_TABLENAME}->{name};
  my $tableLFN  = $tableRef->{lfn}  || $self->{INDEX_TABLENAME}->{lfn};

  #  delete $insert->{table};

  $tableName =~ /^\d*$/ and $tableName = "L${tableName}L";

  $insert->{dir} = $self->getParentDir($insert->{lfn});
  $insert->{lfn} =~ s{^$tableLFN}{};

  return $self->insert($tableName, $insert, {functions => {guid => "string2binary"}});
}

sub addFileToCollection {
  my $self     = shift;
  my $filePerm = shift;
  my $collPerm = shift;
  my $info     = shift || {};
  my $collId   = $self->queryValue("SELECT collectionId from COLLECTIONS where collGUID=string2binary(?)",
    undef, {bind_values => [ $collPerm->{guid} ]})
    or $self->info("Error getting the collection id of $collPerm->{lfn}")
    and return;

  $info->{collectionId} = $collId;
  $info->{origLFN}      = $filePerm->{lfn};
  $info->{guid}         = $filePerm->{guid};

  my $done = $self->insert("COLLECTIONS_ELEM", $info, {functions => {guid => "string2binary"}, silent => 1});

  if (!$done) {
    if ($DBI::errstr =~ /Duplicate entry '(\S+)'/) {
      $self->info("The file '$filePerm->{guid}' is already in the collection $collPerm->{lfn}");
    } else {
      $self->info("Error doing the insert: $DBI::errstr");
    }

    return;
  }
  return 1;
}

sub getInfoFromCollection {
  my $self     = shift;
  my $collGUID = shift;
  $self->debug(1, "Getting all the info of collection '$collGUID'");
  return $self->query(
    "SELECT origLFN, "
      . $self->binary2string
      . " as guid,data, localName from COLLECTIONS c, COLLECTIONS_ELEM e where c.collectionId=e.collectionId and collGUID=string2binary(?)",
    undef,
    {bind_values => [$collGUID]}
  );
}

sub removeFileFromCollection {
  my $self     = shift;
  my $permFile = shift;
  my $permColl = shift;
  $self->debug(1, "Ready to delete the entry from $permColl->{lfn}");
  my $collId = $self->queryValue("SELECT collectionId from COLLECTIONS where collGUID=string2binary(?)",
    undef, {bind_values => [ $permColl->{guid} ]})
    or $self->info("Error getting the collection id of $permColl->{lfn}")
    and return;

  my $deleted = $self->delete(
    "COLLECTIONS_ELEM",
    "collectionId=? and guid=string2binary(?)",
    {bind_values => [ $collId, $permFile->{guid} ]}
  );
  if ($deleted =~ /^0E0$/) {
    $self->info("The file '$permFile->{guid}' is not in that collection");
    return;
  }
  return $deleted;
}

sub renumberLFNtable {
  my $self    = shift;
  my $table   = shift || $self->{INDEX_TABLENAME}->{name};
  my $options = shift || {};
  $self->info("How do we renumber '$table'??");

  my $info = $self->query(
"select ${table}d.entryId as t from $table ${table}d left join $table ${table}r on ${table}d.entryId-1=${table}r.entryId where ${table}r.entryId is null order by t asc"
  );

  #Let's do this part before dropping the index
  my @newlist;
  my $reduce = 0;

  while (@$info) {
    my $entry = shift @$info;
    my $r =
      $self->queryValue("select max(entryId) from $table where entryId<?", undef, {bind_values => [ $entry->{t} ]});
    if (!$r) {

      #If this is the first value of the table
      $entry->{t} < 2 and next;
      $r = 0;
    }
    $r = $entry->{t} - $r - 1;
    $reduce += $r;
    my $max = undef;
    $info and ${$info}[0] and $max = ${$info}[0]->{t};
    push @newlist, {min => $entry->{t}, reduce => $reduce, max => $max};
  }
  if ($options->{n}) {
    $self->info("Just informing what we would do...");
    foreach my $entry (@newlist) {
      my $message = "For entries bigger than $entry->{min}, we should reduce by $entry->{reduce}";
      $entry->{max} and $message .= " (up to $entry->{max}";
      $self->info($message);
    }
    return 1;
  }

  #  print Dumper(@newlist);
  defined $options->{locked} or $self->lock($table);
  $self->info("There are $#newlist +1 entries that need to be fixed");

  #  $self->do("alter table $table modify entryId bigint(11),drop primary key");
  #  $self->do("alter table $table drop primary key");
  my $changes = 0;
  foreach my $entry (@newlist) {
    my $message = "For entries bigger than $entry->{min}, we should reduce by $entry->{reduce}";
    $entry->{max} and $message .= " (up to $entry->{max}";
    $self->info($message);
    my $max1 = "";
    my $max2 = "";
    my $bind = [ $entry->{reduce}, $entry->{min} ];
    if ($entry->{max}) {
      $max1 = "and dir<?";
      $max2 = "and entryId<?";
      $bind = [ $entry->{reduce}, $entry->{min}, $entry->{max} ];
    }
    my $done = $self->do("update $table set dir=dir-? where dir>=? $max1", {bind_values => $bind});
    
    my $q = "update $table set entryId=entryId-? where entryId>=? $max2 order by entryId";
    $self->{DRIVER}=~/Oracle/i and $q = "update $table set entryId=entryId-? where entryId>=? $max2";
    my $done2=$self->do($q, {bind_values=>$bind});
    ($done and $done2)
      or $self->info("ERROR !!")
      and last;
    $changes = 1;
  }
  if ($options->{min} and $options->{min} > 1) {
    $self->info("And now, updating the minimun (to $options->{min}");
    my $max_entryId = $self->queryValue("SELECT MAX(entryId) from $table");
    my $max_dir = $self->queryValue("SELECT MAX(entryId) from $table");
    $self->do("update $table set entryId=entryId+$options->{min}-1+$max_entryId, dir=dir+$options->{min}-1+$max_dir ");
    $self->do("update $table set entryId=entryId-$max_entryId, dir=dir-$max_dir ");
    $changes = 1;
  }

  # $self->do("alter table $table modify entryId bigint(11) auto_increment primary key");
  if ($changes) {

    #$self->do("alter table $table auto_increment=1");
    $self->resetAutoincrement($table);
    $self->do("optimize table $table");
  }
  defined $options->{locked} or $self->unlock($table);

  return 1;
}

sub cleanupTagValue {
  my $self      = shift;
  my $directory = shift;
  my $tag       = shift;

  my $tags = $self->getFieldsByTagName($tag, "tableName", 1, $directory)
    or $self->info("Error getting the directories for $tag and $directory")
    and return;

  my $dirs = $self->getTablesForEntry($directory)
    or $self->info("Error getting the tables of $directory")
    and return;

  foreach my $tag (@$tags) {
    $self->info("First, let's delete duplicate entries");
    $self->lock($tag->{tableName});
    $self->{DRIVER} =~ /Oracle/ and my $global = "global";
    $self->do("create $global temporary table $tag->{tableName}temp as select "
        . $self->reservedWord("file")
        . " as f, max(entryId) as e from $tag->{tableName} group by "
        . $self->reservedWord("file"));
    $self->do("delete from  $tag->{tableName} using  $tag->{tableName},  $tag->{tableName}temp where "
        . $self->reservedWord("file")
        . "=f and entryId<e");
    $self->do("drop temporary table  $tag->{tableName}temp");
    $self->unlock($tag->{tableName});
    foreach my $host (@$dirs) {
      $self->info("Deleting the entries from $tag->{tableName} that are not in $host->{tableName} (like $host->{lfn})");
      my @bind = ($host->{lfn}, $host->{lfn});
      my $where = " and " . $self->reservedWord("file") . " like concat(?,'%') ";
      foreach my $entry (@$dirs) {
        $entry->{lfn} =~ /^$host->{lfn}./ or next;
        $self->info("$entry->{lfn} is a subdirectory!!");
        $where .= " and " . $self->reservedWord("file") . " not like concat(?,'%') ";
        push @bind, $entry->{lfn};
      }
      $self->do(
        "delete from $tag->{tableName} using $tag->{tableName} left join L$host->{tableName}L on "
          . $self->reservedWord("file")
          . "=concat(?, lfn) where lfn is null $where",
        {bind_values => \@bind}
      );
    }

  }

  return 1;
}

sub LFN_getNumberOfEntries {
  my $self    = shift;
  my $entry   = shift;
  my $options = shift;
  my $query   = "SELECT COUNT(*) from L$entry->{tableName}L";
  $options =~ /f/ and $query .= " where SUBSTR(lfn,-1) != '/'";
  return $self->queryValue($query);
}

sub updateLFNStats {
  my $self  = shift;
  my $table = shift;
  $self->info("Let's update the statistics of the table $table");

  $table =~ /^L/ or $table = "L${table}L";
  my $number = $table;
  $number =~ s/L//g;
  $self->do("delete from LL_ACTIONS where action='STATS' and tableNumber=?", {bind_values => [$number]});

  $self->do(
    "insert into LL_ACTIONS(tablenumber, time, action, extra) select ?,max(ctime),'STATS', count(*) from L${number}L",
    {bind_values => [$number]});

  my $oldGUIDList = $self->getPossibleGuidTables($number);
  $self->do("delete from LL_STATS where tableNumber=?", {bind_values => [$number]});
  $self->do(
"insert into LL_STATS (tableNumber, max_time, min_time) select  ?, concat(conv(conv(max(guidtime),16,10)+1,10,16),'00000000') max, concat(min(guidtime),'00000000')  min from $table",
    {bind_values => [$number]}
  );
  my $newGUIDList = $self->getPossibleGuidTables($number);

  my $done   = {};
  my @bind   = ();
  my $values = "";
  my $total  = $#$oldGUIDList + $#$newGUIDList + 2;
  $self->info("In total, there are $total guid tables affected");
  my $lfnRef = "$self->{CURHOSTID}_$number";
  foreach my $elem (@$oldGUIDList, @$newGUIDList) {
    $values .= " (?, 'TODELETE'), ";
    push @bind, $elem->{tableName};
    push @bind, $elem->{tableName};
    $self->info("Doing $elem->{tableName}");
    my $gtable = "$elem->{db}.G$elem->{tableName}L";

    if ($elem->{address} eq $self->{HOST}) {
      $self->debug(1, "This is the same host. It is easy");

      my $maxGuidTime = $self->queryValue(
"select substr(min(guidTime),1,8) from GUIDINDEX where guidTime> (select guidTime from GUIDINDEX where tableName=?  and hostindex=?)",
        undef,
        {bind_values => [ $elem->{tableName}, $elem->{hostIndex} ]}
      );
      my $query =
"insert into ${gtable}_REF(guidid,lfnRef) select g.guidid, ? from $gtable g join $table l using (guid) left join ${gtable}_REF r on g.guidid=r.guidid and lfnref=? where r.guidid is null and l.guidtime>=(select substr(guidtime,1,8) from GUIDINDEX where tablename=? and hostIndex=? )";
      my $bind = [ $lfnRef, $lfnRef, $elem->{tableName}, $elem->{hostIndex} ];
      if ($maxGuidTime) {
        $self->info("The next guid is $maxGuidTime");
        $query .= " and l.guidTime<?";
        push @$bind, $maxGuidTime;
      }
      $self->do(
"delete from ${gtable}_REF using ${gtable}_REF left join $gtable using (guidid) left join $table l using (guid) where l.guid is null and lfnRef=?",
        {bind_values => [$lfnRef]}
      );
      $self->do($query, {bind_values => $bind});
    } else {
      $self->info("This is in another host. We can't do it easily :( 'orphan guids won't be detected'");
      $self->do(
"update  $gtable g, $table l set lfnRef=concat(lfnRef, concat( ?, ',')) where g.guid=l.guid and g.lfnRef not like concat(',',concact(?,','))",
        {bind_values => [ $number, $number ]}
      );
    }
  }
  if ($values) {
    $self->info("And now, let's put the guid tables in the list of tables that have to be checked");
    $values =~ s/, $//;
    $self->do(
"insert  into GL_ACTIONS(tableNumber, action)  select $values from DUAL where not exists (select * from GL_ACTIONS where tableNumber=? and action='TODELETE')",
      {bind_values => [@bind]}
    );
  }
  return 1;

}

sub getPossibleGuidTables {
  my $self   = shift;
  my $number = shift;
  $number =~ s/L//g;

  return $self->query(
"select * from (select * from GUIDINDEX where 
     guidTime<(select max_time from  LL_STATS where tableNumber=?)  
    and  guidTime>(select min_time from LL_STATS where tableNumber=?)
      union
       select * from GUIDINDEX where guidTime= (select max(guidTime) from GUIDINDEX where guidTime< (select min_time from LL_STATS where tableNumber=?))) g
      ",
    undef,
    {bind_values => [ $number, $number, $number ]}
  );

}

=head1 SEE ALSO

AliEn::Database

=cut

sub getAllLFNTables {
  my $self = shift;

  my $result = $self->query("SELECT tableName from INDEXTABLE");
  defined $result
    or $self->info("Error: not possible to get all the pair of host and table")
    and return;

  return $result;
}

sub fquota_update {
  my $self  = shift;
  my $size  = shift;
  my $count = shift;
  my $user  = (shift || $self->{CONFIG}->{ROLE});

  (defined $size) and (defined $count)
    or $self->info("Update fquota : not enough parameters")
    and return;

  #$size *= $count;
  #($size ge 0) and ($count le 0) and $size = -1*$size;

  $self->info("Updating Quotas for user=$user with (count=$count and Size=$size)");

  $self->do(
"UPDATE FQUOTAS SET nbFiles=nbFiles+tmpIncreasedNbFiles+?, totalSize=totalSize+tmpIncreasedTotalSize+?, tmpIncreasedNbFiles=0, tmpIncreasedTotalSize=0 WHERE "
      . $self->reservedWord("user") . "=?",
    {bind_values => [ $count, $size, $user ]}
  ) or return;
  
  return 1;
}

1;

