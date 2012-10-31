calculateFileQuota {
  my $self   = shift;
  my $silent = shift;

  ($self->{ROLE} =~ /^admin(ssl)?$/)
    or $self->info("Error: only the administrator can check the database")
    and return;

  my $method = "info";
  my @data;
  $silent and $method = "debug" and push @data, 1;

  $self->$method(@data, "Calculate File Quota");
  my $lfndb = $self->{DATABASE};

  my $calculate = 0;
  my $rtables   = $lfndb->getAllLFNTables();

  foreach my $h (@$rtables) {
    my $LTableIdx = $h->{tableName};
    my $LTableName = "L${LTableIdx}L";

    #check if all tables exist for $LTableIdx
    $lfndb->checkLFNTable("${LTableIdx}");

    $self->$method(@data, "Checking if the table ${LTableName} is up to date");
    $lfndb->queryValue(
"select 1 from (select max(ctime) ctime, count(1) counter from $LTableName) a left join LL_ACTIONS on tableName=? and action='QUOTA' where extra is null or extra<>counter or time is null or time<ctime",
      undef,
      {bind_values => [$LTableIdx]}
    ) or next;

    $self->$method(@data, "Updating the table ${LTableName}");
    $lfndb->do("delete from LL_ACTIONS where action='QUOTA' and tableName=?", {bind_values => [$LTableIdx]});
    $lfndb->do(
"insert into LL_ACTIONS(tableName, time, action, extra) select ?, max(ctime), 'QUOTA', count(1) from $LTableName",
      {bind_values => [$LTableIdx]}
    );

    my %sizeInfo;
    $lfndb->do("delete from ${LTableName}_QUOTA");
    $lfndb->do("insert into ${LTableName}_QUOTA ("
        . "userId, nbFiles, totalSize) select USERS.uId as \"user\", count(l.lfn) as nbFiles, sum(l."
        . $lfndb->reservedWord("size")
        . ") as totSize from ${LTableName} l 
        JOIN USERS ON l.ownerId=USERS.uId 
        where l.type='f' group by l.ownerId order by l.ownerId");
    $calculate = 1;
  }

  $calculate or $self->$method(@data, "No need to calculate") and return;

  my %infoLFN;
  foreach my $h (@$rtables) {
    my $tableIdx = $h->{tableName};
    my $tableName = "L${tableIdx}L";
    $self->$method(@data, "Getting from Table ${tableName}_QUOTA ");
    my $userinfo = $lfndb->query("select userId, nbFiles, totalSize from ${tableName}_QUOTA");
    foreach my $u (@$userinfo) {
      my $userid = $u->{userid};
      if (exists $infoLFN{$userid}) {
        $infoLFN{$userid} = {
          nbfiles   => $infoLFN{$userid}{nbfiles} + $u->{nbFiles},
          totalsize => $infoLFN{$userid}{totalsize} + $u->{totalSize}
        };
      } else {
        $infoLFN{$userid} = {
          nbfiles   => $u->{nbFiles},
          totalsize => $u->{totalSize}
        };
      }
    }
  }

  $self->$method(@data, "Updating FQUOTAS table");
  $self->{DATABASE}->lock("FQUOTAS write, USERS");
  $self->{DATABASE}
    ->do("update FQUOTAS set nbFiles=0, totalSize=0, tmpIncreasedNbFiles=0, tmpIncreasedTotalSize=0")
    or $self->$method(@data, "initialization failure for all users");
  foreach my $userid (keys %infoLFN) {
    $self->{DATABASE}->do(
      "update FQUOTAS set nbFiles=?, totalSize=? where userid=?", {bind_values=>[$infoLFN{$userid}{nbfiles}, $infoLFN{$userid}{totalsize},$userid]})
      or $self->$method(@data, "update failure for user $userid");
  }
  $self->{DATABASE}->unlock();
}

return 1;
