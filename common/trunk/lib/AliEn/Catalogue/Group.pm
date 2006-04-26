package AliEn::Catalogue::Group;

#use DBI;
#use AliEn::SQLInterface;
use strict;

sub f_umask {
    my $self = shift;
    my $perm = shift;
    if ( !$perm ) {
        print STDERR "Default permission bits: $self->{UMASK}\n";
        return 1;
    }
    if ( $perm =~ /[0-7]/ ) {
    }
    else {
        print STDERR "Permissions have to be in octal.\n";
        return;
    }
    if ( length $perm != 3 ) {
        print STDERR "You must specify 3 permissions.\n";
        return;
    }
    $self->{UMASK} = $perm;
    return 1;
}

sub f_groups {
  my $self = shift;
  my $user = ( shift or $self->{ROLE} );
  my $retprimary = (shift or "" );

  my ($group) = $self->{DATABASE}->getUserGroups($user);
  my ($addgroups) = $self->{DATABASE}->getUserGroups($user, 0);

  $group and $group = $group->[0];
  $group or return;

  defined $addgroups
    or return;

  my $addgrps = "";
  
  foreach (@$addgroups){
    $addgrps .= "$_,";
  }
  
  $addgrps and chop $addgrps;

  ! $self->{SILENT} && 
  print STDERR
    "User: $user\nPrimary group: $group Additional groups: $addgrps\n";
  if ($retprimary) {
    return $group;
  } else {
    return \@$addgroups;
  }
}

sub f_chmod {
  my $self = shift;
  my $perm = shift;
  my $file = shift;

  ($file)
    or print STDERR
      "Error: not enough arguments in chmod\nUsage: chmod <permissions> <fileName>\n"
	and return;

  $file = $self->GetAbsolutePath($file, 1);

  my $info=$self->checkPermissions("w",$file, undef , {RETURN_HASH=>1})
    or return;
  
  my $lfn=$self->{DATABASE}->existsEntry( $file) or
    $self->{LOGGER}->error("File", "file $file does not exist!!",1) and
      return;

  (  $info->{owner} eq $self->{ROLE}) or ($self->{ROLE}=~ /^admin(ssl)?$/) or
    $self->info("You are not the owner of $file",1) and
      return;

  if (!($perm =~ s/^0?([0-7]{3})$/$1/)) {
    my $oldperm=$info->{perm};
    $perm =~ /([ugo]*)(\+|\-{1})([rwx]*)/;
    my ($who, $subadd, $what) = ($1, $2, $3);

    my $all = $who !~ /[ugo]/;
    my @who = (($who =~ /u/ or undef), ($who =~ /g/ or undef), ($who =~ /o/ or undef));
    my @what = (($what =~ /x/ or undef), ($what =~ /w/ or undef), ($what =~ /r/ or undef));

    my @newtype;
    for (my $i=0; $i<=2; ++$i) {
      $newtype[$i] = substr $oldperm, $i, 1;
      for (my $j=0; $j<=2; ++$j) {
	if (($all or $who[$i]) and ($what[$j] and !($all and $j==1 and $i!=0))) {
	  $newtype[$i] |= (1 << $j) if ($subadd eq "+");
	  $newtype[$i] &= ~(1 << $j) if ($subadd eq "-");
	}
      }
    }
    $perm = join "", @newtype;
  }
  $self->{DATABASE}->updateFile($lfn, {perm=>$perm})
    or print STDERR "error updating file permissions for $file\n" and return;

  return 1;
}

sub f_chown {
  my $self = shift;
  ( $self->{DEBUG} > 3 )
    and print "DEBUG LEVEL 3\t\tIn GroupInterface:f_chown with @_\n";
  my $options=shift || "";
  #  Possible options:
  #    f:   do not grant the privileges on the database


  my $data = shift;
  my $file = shift;
  if (!$file) {
    print STDERR "Error: not enough arguments in chown\nUsage: chown <user>[.<group>] <file>\n";
    return;
  }

  if ($self->{DATABASE}->{ROLE} !~  /^admin(ssl)?$/) {
    print STDERR "Error: only superuser can chown\n";
    return;
  }

  my ( $user, $group ) = split ( /\./, $data );

  my $oldsilent = $self->{SILENT};
  $self->{SILENT} = 1;
  my $primarygroup = $self->f_groups($user,1);
  $self->{SILENT} = $oldsilent;
  if ( (defined $group) and ($group ne "" )) {
    $self->{SILENT} = 1;
    my $allgroups = $self->f_groups($user);
    $self->{SILENT} = $oldsilent;
    my $hasgroup = 0;
    foreach (@$allgroups) {
      if ( "$_" eq $group ) {
	$hasgroup = 1;
      }
    }
    if (!$hasgroup) {
      print STDERR "Error: $user is not member of group $group\n";
      return;
    }
  } else {
    $group = $primarygroup;
  }

  $file = $self->GetAbsolutePath($file, 1);

  $self->selectDatabase($file)
    or print STDERR "Error in selectDatabase" and return;

  my $lfn  =$self->{DATABASE}->existsEntry( $file) or 
    $self->info("chown $file: No such file or directory")
    and return;

  $self->{DATABASE}->updateFile( $lfn, {owner=>$user,gowner=>$group})
    or $self->info("Error updating the file") and return;

  $options =~ /f/ and return 1;
  #Finally, we have to grant privileges to the user
  #Since we are admin, we can do it directly:
  my $db=$self->{DATABASE}->{DB};
  my $table=$self->{DATABASE}->getIndexTable();
  return $self->{DATABASE}->do("GRANT ALL on $db.$table->{name} to $user");
}
return 1;
