package AliEn::Database::Catalogue::Shared;
use strict;

use vars qw(@ISA $DEBUG);

push @ISA, qw(AliEn::Database);

# This function is inherited by the children
#
#
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
  $self->{binary2string} = $self->binary2string("guid");
  $self->{VIRTUAL_ROLE} or $self->{VIRTUAL_ROLE} = $self->{ROLE};
  return $self->SUPER::initialize(@_);
}

##############################################################################
##############################################################################
sub setIndexTable {
  my $self  = shift;
  my $table = shift;
  my $lfn   = shift;
  defined $table or return;
  $table =~ /^\d*$/ and $table = "D${table}L";

  $DEBUG and $self->debug(2, "Setting the indextable to $table ($lfn)");
  $self->{INDEX_TABLENAME} = {name => $table, lfn => $lfn};
  return 1;
}

sub getIndexTable {
  my $self = shift;
  return $self->{INDEX_TABLENAME};
}

sub getSENumber {
  my $self    = shift;
  my $se      = shift;
  my $options = shift || {};
  $DEBUG and $self->debug(2, "Checking the senumber");
  defined $se or return 0;
  $options->{force} and AliEn::Util::deleteCache($self);
  my $cache = AliEn::Util::returnCacheValue($self, "seNumber-$se");
  $cache and return $cache;

  $DEBUG and $self->debug(2, "Getting the numbe from the list");
  my $senumber =
    $self->queryValue("SELECT seNumber FROM SE where upper(seName)=upper(?)", undef, {bind_values => [$se]});
  if (defined $senumber) {
    AliEn::Util::setCacheValue($self, "seNumber-$se", $senumber);
    return $senumber;
  }
  $DEBUG and $self->debug(2, "The entry did not exist");
  $options->{existing} and return;
  $self->{SOAP}
    or $self->{SOAP} = new AliEn::SOAP
    or return;

  my $result = $self->{SOAP}->CallSOAP("Authen", "addSE", $se) or return;
  my $seNumber = $result->result;
  $DEBUG and $self->debug(1, "Got a new number $seNumber");
  AliEn::Util::setCacheValue($self, "seNumber-$se", $senumber);

  return $seNumber;
}

##############################################################################
##############################################################################
sub actionInIndex {
  my $self   = shift;
  my $action = shift;

  #updating the D0 of all the databases
  my ($oldHost, $oldDB, $oldDriver) = ($self->{HOST}, $self->{DB}, $self->{DRIVER});
  my $tempHost;
  $self->info("Updating the INDEX table of");
  $self->do($action) or print STDERR "Warning: Error doing $action";
  $DEBUG and $self->debug(2, "Everything is done!!");

  return 1;
}

sub insertInIndex {
  my $self      = shift;
  my $hostIndex = shift;
  my $table     = shift;
  my $lfn       = shift;
  my $options   = shift;

  $table =~ s/^D(\d+)L$/$1/;
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
  my $action = "INSERT INTO $indexTable (hostIndex, tableName, $column) values('$hostIndex', '$table', $value)";
  return $self->actionInIndex($action);
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
  return $self->actionInIndex($action);

}

sub getAllIndexes {
  my $self = shift;
  return $self->query("SELECT * FROM INDEXTABLE");

}

=item executeInAllDB ($method, @args)

This subroutine calls $method in all the databases that belong to the catalogue
If any of the calls fail, it returns udnef. Otherwise, it returns 1, and a list of the return of all the statements. 

At the end, it reconnects to the initial database

=cut

sub executeInAllDB {
  my $self   = shift;
  my $method = shift;

  $DEBUG and $self->debug(1, "Executing $method (@_) in all the databases");

  my $error = 0;
  my @return;
  my $info = $self->$method(@_);
  if (!$info) {
    $error = 1;
    return;
  }
  push @return, $info;

  $error and return;
  $DEBUG and $self->debug(1, "Executing in all databases worked!! :) ");
  return 1, @return;

}

sub destroy {
  my $self = shift;

  #
  #  use Data::Dumper;
  #  my $number = $self->{UNIQUE_NM};
  #  $number               or return;
  #  $Connections{$number} or return;
  #  my @databases = keys %{$Connections{$number}};
  #
  #  foreach my $database (@databases) {
  #    $database =~ /^FIRST_DB$/ and next;
  #
  #    $Connections{$number}->{$database} and $Connections{$number}->{$database}->SUPER::destroy();
  #    delete $Connections{$number}->{$database};
  #  }
  #
  #  #  delete $Connections{$number};
  #  keys %Connections or undef %Connections;
  #
  #  #  $self->SUPER::destroy();
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
  );

  return $self->checkTable("SE", "seNumber", \%columns, 'seNumber', ['UNIQUE INDEX (seName)'], {engine => "innodb"})
    ;    #or return;
         #This table we want it case insensitive

  #  return $self->do("alter table SE  convert to CHARacter SET latin1");
}

sub reconnectToIndex {
  my $self      = shift;
  my $index     = shift;
  my $tableName = shift;
  my $data      = shift;
  return ($self, $tableName);
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
  $v = $self->queryValue("SELECT count(*) from GROUPS where Username='$user' and Groupname = '$group'");
  AliEn::Util::setCacheValue($self, "usergroup-$user-$group", $v);

  return $v;
}

# Gives the userid of a user and group. If the group is not specified, it
# gives the primary group
sub getUserid {
  my $self  = shift;
  my $user  = shift;
  my $group = shift;
  my $where = "primarygroup=1";
  $group and $where = "groupname='$group'";
  return $self->queryValue("SELECT userid from GROUPS where Username='$user' and $where");
}

sub setUserGroup {
  my $self       = shift;
  my $user       = shift;
  my $group      = shift;
  my $changeUser = shift;

  my $field = "ROLE";
  $changeUser or $field = "VIRTUAL_ROLE";

  $self->debug(1, "Setting the userid to $user ($group)");
  $self->{$field} = $user;
  $self->{MAINGROP} = $group;
  return 1;
}

sub renumberTable {
  my $self    = shift;
  my $table   = shift;
  my $index   = shift;
  my $options = shift || {};

  my $lock = "$table";
  $options->{lock} and $lock = "$options->{lock} $lock";
  my $info = $self->queryValue("select max($index)-count(1) from $table");
  $info or $info = 0;
  if ($info < 100000) {
    $self->debug(1, "Only $info. We don't need to renumber");
    return 1;
  }

  $self->info("Let's renumber the table $table");

  $self->lock($lock);
  my $ok = 1;
  $self->do(
"alter table $table modify $index int(11), drop primary key,  auto_increment=1, add new_index int(11) auto_increment primary key, add unique index (guidid)"
  ) or $ok = 0;
  if ($ok) {
    foreach my $t (@{$options->{update}}) {
      $self->debug(1, "Updating $t");
      $self->do("update $t set $index= (select new_index from $table where $index=$t.$index)") and next;
      $self->info("Error updating the table!!");
      $ok = 0;
      last;
    }
  }
  if ($ok) {
    $self->info("All the renumbering  worked! :)");
    $self->do("alter table $table drop column $index, change new_index $index int(11) auto_increment");
  } else {
    $self->info("The update didn't work. Rolling back");
    $self->do("alter table $table drop new_index, modify $index int(11) auto_increment primary key");
  }

  $self->unlock($table);

  return 1;

}

1;
