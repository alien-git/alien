package AliEn::Catalogue::Tag;

use strict;

# Assigns a metadata to a directory
# Input: directory -> LFN (/alice/simulation/2003-03 )
#        tagName   -> Name of the metadata (MonteCarloRuns)
#        tagSQL    -> SQL description of the table
# Output  1 if it works, undef if it doesn't
# Call from: UI/Catalogue/LCM
sub f_addTag {
  my $self      = shift;

  my $directory = shift;
  my $tagName   = shift;
  my $tagSQL    = shift;
  my $options   =(shift or "");

  ($tagSQL)
    or print STDERR
      "Error: not enough arguments in addTag\nUsage: addTag <directory> <tag name> <tag description>\n"
	and return;

  $directory = $self->f_complete_path($directory);

  $directory =~ s/\/?$/\//;
  ( $self->checkPermissions( 'w', $directory ) ) or return;
  $self->isDirectory($directory) or
    print STDERR "$directory is not a direcotry!!\n" and return;

  $self->existsTag($directory, $tagName, "silent")
    and  $self->info("Tag already exists") and return 1;

  my $create = 1;
  my $fileLength = 255;

  $self->debug(1, "Creating only one table for all the metadata");
  my $tableName = "T$self->{DATABASE}->{USER}V$tagName";
  if (! $self->{DATABASE}->existsTable($tableName)) {
    $self->info("Creating the table $tableName...");
    if ($options){
      $self->info("In fact, we want a table like the one for directory '$options'");
      $tagSQL=$self->f_showTagDescription($tagName, $options)
	or $self->info("Error getting the description of the table") and return;
    }
    $self->selectDatabase($directory);
    my $done = $self->createRemoteTable(
					$self->{DATABASE}->{LFN_DB}->{HOST},   $self->{DATABASE}->{LFN_DB}->{DB},
					$self->{DATABASE}->{LFN_DB}->{DRIVER}, $self->{DATABASE}->{LFN_DB}->{USER},
					$tableName,"(file char($fileLength), offset int, entryId int AUTO_INCREMENT, $tagSQL , KEY (entryId), INDEX (file))"
				       );

    $done or return;
  } else {
    $self->info("The table exists");
  }

  my $done = $self->{DATABASE}->insertIntoTag0($directory, $tagName, $tableName);
  $done or $self->{LOGGER}->error("Tag", "Error inserting the entry!") and return;
    print "Tag created\n";

  return 1;
}
sub f_showTagDescription_HELP{
  return "showTagDescription: describes the metadata of a given tag
Usage:
\t\tshowTagDescription <directory> <tagName>
";
}
sub f_showTagDescription{
  my $self=shift;
  my $options=shift;
  my $directory=shift;
  my $tag=shift;
  $directory and $tag or $self->info("Error: not enough arguments\n". $self->f_showTagDescription_HELP()) and return;
  $self->info("Getting the description of $tag and $directory");
  $self->checkPermissions("r", $directory) or $self->info("Error: you can't read the directory $directory") and return;
  my $table=$self->{DATABASE}->{LFN_DB}->getTagTableName($directory, $tag)
    or $self->info("Error getting the name of the table") and return;
  $self->info("Getting the description");
  my $rows=$self->{DATABASE}->{LFN_DB}->query("describe $table") or $self->info("Error describing the table") and return;
  my $sql="";
  foreach my $row (@$rows){
    $row->{Field} =~ /^(file)|(offset)|(entryId)$/  and next;
    $sql.="$row->{Field} $row->{Type} ,";
  }
  chop $sql;
  $self->info("The statement is $sql");
  return $sql;
}

sub f_removeTag {
  my $self =shift;
  my $directory =shift;
  my $tag = shift;
  $tag or $self->{LOGGER}->error("Tag", "Error: not enough arguments in removeTag\nUsage removeTag <directory> <tag_name>") and return;

  $directory = $self->f_complete_path($directory);
  $directory =~ s/\/?$/\//;
  ( $self->checkPermissions( 'w', $directory ) ) or return;


  $self->existsTag($directory, $tag) or return;

  $self->info("Deleting Tag $tag of $directory");
  return $self->{DATABASE}->deleteTagTable( $tag, $directory);
}

sub f_showTags {
  my $self      = shift;
  my $options   = shift;
  my $directory = shift;

  ($directory) or ( $directory = $self->{CURPATH} );
  $directory = $self->f_complete_path($directory);
  $directory =~ s/\/?$/\//;

  ( $self->checkPermissions( 'r', $directory ) ) or return;

  $self->isDirectory($directory)
    or print STDERR "Error: directory $directory does not exist!\n"
      and return;

  my @tags=();
  my @hashtags=();
  my $result;
  my $return;

  if ($options =~ /all/) {
    $self->debug(1, "Getting all the tags ");  
    ($result) = $self->{DATABASE}->getAllTagNamesByPath($directory);
      foreach my $entry (@{$result}) {
      my $hashval={};
      push @tags, $entry->{tagName};
      $hashval->{tagname} = $entry->{tagName};
      push @hashtags, $hashval;
    }
    $return=$result;
  } else {
    ($result) = $self->{DATABASE}->getTagNamesByPath($directory);
    @tags=@$result;
    $return= join ( "###", @$result );
    foreach my $tagname (@$result) {
        my $hashval={};
	$hashval->{tagname} = $tagname;
	push @hashtags, $hashval;
    }
  }

  if ( !$result or $#{$result} == -1) {
      ($options =~ /z/) || $self->info("There are no tags defined for $directory");
    return ();
  }

  ($options =~ /z/) || $self->info("Tags defined for $directory \n@tags");
  if ($options =~ /z/) {
      return @hashtags;
  } else {
      return $return;
  }
}

sub existsTag {
  my $self      = shift;
  my $directory = shift;
  my $tag       = shift;
  my $silent    = (shift or 0);

  ($tag)
    or print STDERR
      "Error: not enough arguments in existsTag\nUsage existsTag <dir> <tagName>\n"
	and return;

  $self->debug(1, "Checking if tag $tag exists in $directory");

  unless ($self->isDirectory($directory)) {
    $silent or print STDERR "Error: directory $directory does not exist!\n";
    return;
  }
 # $self->selectDatabase($directory) or return;

  my $rresult = $self->{DATABASE}->getTagNamesByPath($directory);

  $self->debug(1, "Got @$rresult");

  if (! grep (/^$tag$/, @$rresult)){
    $silent or print "Tag $tag for $directory does not exist\n";
    return;
  }
  return 1;
}

sub f_updateTagValue {
  my $self=shift;
  return $self->modifyTagValue("update", @_);
}

sub f_addTagValue {
  my $self  = shift;
  return $self->modifyTagValue("add", @_);
}

sub parseTagInput{
  my $self=shift;
  my @pairs = @_;
  
  my $pair;
  my @newpair;
  my $buffer = "";
  foreach $pair (@pairs) {
    $buffer .= $pair;
    my @a = $buffer =~ /[\"\']/g;

    if ( ( $#a + 1 ) % 2 ) {
      $buffer .= " ";
    }
    else {
      $buffer =~ s/[\"\']//g;
      @newpair = ( @newpair, $buffer );
      $buffer = "";
    }
  }
  ($buffer) and print STDERR "Error: unbalanced quotes\n" and return;

  my %data;
  foreach $pair (@newpair) {
    $self->debug(1, "Checking $pair");
    my ( $var, $val ) = split "=", $pair, 2;
    
    ( defined $val )
      or print STDERR "Error: variable $var has no value\n"
	and return;
    $data{$var} = $val;
  }
  
  return (1, \%data);
}

sub modifyTagValue {
  my $self=shift;
  my $action=shift;
  my $file  = $self->GetAbsolutePath(shift);
  my $tag   = shift;

  (@_)
    or print STDERR
      "Error: not enough arguments in addValueTag\nUsage: addTagValue <file> <tag> <variable>=<value> [<variable>=<value> ...]\n"
	and return;
  $self->debug(1, "In modifyTagValue, with File=$file, tag=$tag ");

  my ($status, $rdata) = $self->parseTagInput(@_);
  $status or return;

 # ( $self->checkPermissions( 'w', $file ) ) or return;

  $file =~ s/\/$//;
  my $directory = $self->f_dirname($file);

  # my $tagTableName = $self->{DATABASE}->getTagTableName($directory, $tag);

  my $basename = $self->f_basename($file);

  $self->existsTag( $directory, $tag ) or return;
  #Here, we should make sure that if the tag is assigned to a directory, the
  #entry finishes with /
  #This is used to speed up the 'find'. 
  ($self->isDirectory($file)) and $basename.="/";
 ( $self->checkPermissions( 'w', $file ) ) or return;
  my $error = $self->{DATABASE}->insertTagValue($action, $directory, $tag, $basename, $rdata);
  ($error) or print STDERR "Error inserting the tags\n" and return;

  return 1;
}

sub f_showTagValue {
  my $self = shift;
  my $opts   = shift;
  my $path = $self->GetAbsolutePath(shift);
  my $tag = shift;
  
  my $hashtags=();
  ($tag)
    or print STDERR
      "Error: not enough arguments in showTagValue\nUsage: showTagValue <file> <tag>\n"
	and return;

  ( $self->checkPermissions( 'r', $path ) ) or return;
  my $path2=$self->{DATABASE}->existsEntry( $path) or 
    $self->info("The directory $path does not exist") and return;

  my $directory=$path2;
  $directory =~ m{/$} or $directory = $self->f_dirname($path);

  while (! $self->existsTag( $directory, $tag, "silent" )) {
    $directory =~ s{/$}{};
    $directory = $self->f_dirname($directory);
    $directory or  $self->{LOGGER}->error("Tag", "The tag $tag is not defined for $path") and return;
    $self->debug(1,"Checking if the tag is defined in $directory");
  }
  my $fileName=$path2;
  ($fileName eq "$directory") and $fileName="";
  $fileName =~ s{^(${directory}[^/]*/?).*$}{$1};
  my $where;

  $self->debug(1, "Checking $directory and $fileName");

  my $tagTableName = $self->{DATABASE}->getTagTableName($directory, $tag);

  $where = "file like '$directory%'";
  my $options={filename=>$fileName};

  $self->debug(1, "Checking the tags of $directory and $where");
  my $rTags = $self->{DATABASE}->getTags($directory, $tag, undef, $where, $options);

  my $rcolumns = $self->{DATABASE}->describeTable($tagTableName) 
    or $self->info("Error getting the description of the metadata") and return;

  my @fields;
  foreach my $rcolumn (@$rcolumns) {
    my ($name, $type) = ($rcolumn->{Field}, $rcolumn->{Type});

    my $l = length "$name($type)  ";
    $type =~ /(\d+)/ and $1 > $l and $l = $1;

    if ((!$self->{SILENT}) && ($opts !~ /z/)) {printf "%-${l}s", "$name($type)  ";}
    push @fields, [$name, $l];
  }

  if ( !$self->{SILENT} ) {
    ($opts =~ /z/) || print STDOUT "\n";
    foreach my $line (@$rTags) {
      foreach my $rfield (@fields) {
	my $value="";
	defined $line->{$rfield->[0]} and $value=$line->{$rfield->[0]};
	($opts =~ /z/) || printf( "%-" . $rfield->[1] . "s", $value );
      }
      print STDOUT "\n";
    }
  }

  if ($opts =~ /z/) {
      return @$rTags;
  } else {
      return ( $rcolumns, $rTags );
  }
}

sub f_removeTagValue {
  my $self      = shift;
  my $file      = $self->GetAbsolutePath(shift);
  my $tag       = shift;
  my $attribute = shift;

  ($tag)
    or print STDERR
      "Error: not enough arguments in removeTagValue\nUsage: removeTagValue <file> <tag> \n"
      and return;

  ( $self->checkPermissions( 'w', $file ) ) or return;

  $file =~ s/\/$//;
  my $directory = $self->f_dirname($file);

  $self->existsTag( $directory, $tag ) or return;
  my $tagTableName=$self->{DATABASE}->getTagTableName($directory, $tag);

  if ($tagTableName =~ /T\d+V$tag$/) {
    $file = $self->f_basename($file);
  }

  my $error;
  if ($attribute) {
    $error = $self->{DATABASE}->update($tagTableName, {$attribute => undef}, "file = ?", {bind_values=>[$file]});
  } else {
    ($self->isDirectory($file)) and $file.="/";

    $error = $self->{DATABASE}->delete($tagTableName, "file = '$file'");
  }
  ($error) or print STDERR "Error doing the update\n" and return;
  
  return 1;
}

sub f_showAllTagValues {
  my $self = shift;
  my $opts   = shift;
  my $path = $self->GetAbsolutePath(shift);
  my @hashresult = ();
  ($path)
    or $self->info("Error: not enough arguments in showAllTagValues\nUsage: showTagValue <file> ")
	and return;

  my $tags=$self->f_showTags("all", $path);

  $tags or return;
  my @result=();
  foreach my $entry (@{$tags}) {
    my $tag=$entry->{tagName};
    my $directory=$entry->{path};
    ($opts =~ /z/) || $self->info("Getting all the '$tag' of $path");
    my $tagTableName = $self->{DATABASE}->getTagTableName($entry->{path}, $tag);

    my $where="";
    if ($tagTableName !~ /^T\d+V$tag$/) {
      $where = "file like '$path%'";
    }

    $self->debug(1, "Checking the tags of $directory and $where ");
    my $rTags = $self->{DATABASE}->getTags($directory, $tag, undef, $where);

    my $rcolumns = $self->{DATABASE}->describeTable($tagTableName);

    push @result, {tagName=>$tag, data=>$rTags, columns=>$rcolumns};
    foreach (@$rTags) {
	my $tgcopy = $_;
	$tgcopy->{tagname} = $tag;
	push @hashresult, $tgcopy;
    }
  }

  ($opts =~ /z/) || $self->info("Done!!");

  if ($opts =~ /z/) {
      return @hashresult;
  } else {
      return \@result;
  }
}

return 1;
