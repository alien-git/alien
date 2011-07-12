package AliEn::Database::Catalogue::Shared;
use strict;

use vars qw(@ISA $DEBUG);

push @ISA, qw(AliEn::Database);


##############################################################################
##############################################################################



##############################################################################
##############################################################################


sub reconnectToIndex {
  my $self      = shift;
  my $index     = shift;
  my $tableName = shift;
  my $data      = shift;
  return ($self, $tableName);
}


# Gives the userid of a user and group. If the group is not specified, it
# gives the primary group


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
