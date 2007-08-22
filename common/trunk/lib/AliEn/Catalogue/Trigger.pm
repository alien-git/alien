package AliEn::Catalogue::Trigger;

use strict;

# Assigns a metadata to a directory
# Input: directory     -> LFN (/alice/simulation/2003-03 )
#        triggerName   -> Name of the trigger
#        action        -> Action to put the trigger on (insert,delete,update)
# Output  1 if it works, undef if it doesn't
# Call from: UI/Catalogue/LCM
sub f_addTrigger_HELP {
  return "addTrigger - creates a Trigger on a table
Syntax:
\taddTriger <directory> <triggerName> [<action>]


<action> can be:  insert, update or delete

<triggerName> has to be the name of a file defined in /triggers, /<vo>/triggers or ~/triggers

When the specified action happens in the directory, the script '<triggerName>' will be called, and it will receive the name of the file that has triggered the event. 

"
}
sub f_addTrigger {
  my $self      = shift;

  my $directory = shift;
  my $triggerAction   = shift;
  my $action    = shift || "insert";


  $action=~ /^(insert)|(update)|(delete)$/ or 
    $self->info("I don't understand the action '$action' for the trigger",1)
      and return;
  $triggerAction or $self->info("Error: not enough arguments\n". $self->f_addTrigger_HELP(),1) and return;
  $directory = $self->f_complete_path($directory);

  $directory =~ s/\/?$/\//;
  ( $self->checkPermissions( 'w', $directory ) ) or return;
  $self->isDirectory($directory) or
    print STDERR "$directory is not a direcotry!!\n" and return;

  $self->existsTrigger($directory, $action, )
    and $self->info("The trigger already exists") and return ;

  my $index=$self->{DATABASE}->getIndexTable();
  my $prefix=$index->{lfn};
  my $table=$index->{name};
  my $triggerName="${table}_${action}_$triggerAction";

  $triggerAction=$self->getTriggerLFN($triggerAction) 
    or return;
  $self->info("Ready to create the trigger");

  my $done = $self->{DATABASE}->{LFN_DB}->do("create trigger $triggerName after $action on $table for each row insert into TRIGGERS(lfn, triggerName) values (concat('$prefix', NEW.lfn), '$triggerAction')");
  $done or $self->{LOGGER}->error("Tag", "Error inserting the entry!") and return;
  $self->info( "Trigger created");

  return 1;
}

sub getTriggerLFN {
  my $self=shift;
  my $name=shift;
  
  my $homedir=$self->GetHomeDirectory();
  my $return="";
  my $message="";
#  my $oldmode=$self->{LOGGER}->getMode();
#  $self->{LOGGER}->silentOn();

  if ($name =~ /\//){
    $self->debug(1, "The action is '$name'" );
    $message="The file '$name' doesn't exist";
    if ($self->f_ls( "ls", "s", $name )){
      $message="The trigger is not in the right directory";
      my $org="\L$self->{CONFIG}->{ORG_NAME}\E";
      ($name =~ m{^((/$org)|($homedir))?/triggers/[^\/]*$} ) and
	$return=$name and $message="";
    }
  } else {
    foreach my $dir ($homedir, "\L/$self->{CONFIG}->{ORG_NAME}\E", ""){
      $self->f_ls(  "s", "$dir/triggers/$name") and 
	$return="$dir/triggers/$name";
    }
  }
#  $self->{LOGGER}->setMinimum(split(" ",$oldmode));
  $return or $message="The trigger '$name' is not defined";
  $message and $self->info($message) and return;

  return $return;
}

sub f_removeTrigger {
  my $self =shift;
  my $directory =shift;
  my $action    = shift || "insert";
  $directory or $self->info("Error: not enough arguments in removeTag\nUsage removeTag <directory> [<action>]", 1) and return;

  $directory = $self->f_complete_path($directory);
  $directory =~ s/\/?$/\//;
  ( $self->checkPermissions( 'w', $directory ) ) or return;


  my $triggerName=$self->existsTrigger($directory, $action) or return;

  $self->info("Deleting Trigger $triggerName");
  return $self->{DATABASE}->do("drop trigger $triggerName");
}

sub f_showTrigger {
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
  my $index=$self->{DATABASE}->getIndexTable();
  my $table=$index->{name};

  my $trigger=$self->{DATABASE}->query("show triggers like '$table'");
  return 1;
}

sub existsTrigger{
  my $self      = shift;
  my $directory = shift;
  my $action    = shift;
  my $silent    = (shift or 0);

  ($action)
    or print STDERR
      "Error: not enough arguments in existsTrigger\nUsage existsTriggerg <dir> <action>\n"
	and return;

  $self->debug(1, "Checking if trigger $action exists in $directory");

  unless ($self->isDirectory($directory)) {
    $silent or print STDERR "Error: directory $directory does not exist!\n";
    return;
  }
 # $self->selectDatabase($directory) or return;
  my $index=$self->{DATABASE}->getIndexTable();
  my $table=$index->{name};

  my ($rresult) = $self->{DATABASE}->{LFN_DB}->query("show triggers like '$table'") 
    or return;
  use Data::Dumper;
  print Dumper($rresult);

  foreach my $entry (@$rresult){
    $entry->{Timing} =~ /before/i and next;
    $entry->{Event} =~ /$action/i and return $entry->{Trigger};
  }
  $self->info("The trigger '$action' doesn't exist in '$directory'");
  return ;
}

sub f_updateTagValue2 {
  my $self=shift;
  return $self->modifyTagValue("update", @_);
}

sub f_addTagValue2 {
  my $self  = shift;
  return $self->modifyTagValue("add", @_);
}

sub parseTagInput2{
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

sub modifyTagValue2 {
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

sub f_showTagValue2 {
  my $self = shift;
  my $path = $self->GetAbsolutePath(shift);
  my $tag = shift;

  ($tag)
    or print STDERR
      "Error: not enough arguments in showTagValue\nUsage: showTagValue <file> <tag>\n"
	and return;

  ( $self->checkPermissions( 'r', $path ) ) or return;
  my $path2=$self->{DATABASE}->existsEntry( $path) or 
    $self->{LOGGER}->info("Tag", "The directory $path does not exist") and return;

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
  $fileName and $where = "file='$fileName'";

#  my $options="new";
#  $fileName and $options="one";

  $self->debug(1, "Checking the tags of $directory and $where");
  my $rTags = $self->{DATABASE}->getTags($directory, $tag, undef, $where);

  my $rcolumns = $self->{DATABASE}->describeTable($tagTableName);

  my @fields;
  foreach my $rcolumn (@$rcolumns) {
    my ($name, $type) = ($rcolumn->{Field}, $rcolumn->{Type});

    my $l = length "$name($type)  ";
    $type =~ /(\d+)/ and $1 > $l and $l = $1;

    $self->{SILENT} or printf "%-${l}s", "$name($type)  ";
    push @fields, [$name, $l];
  }

  if ( !$self->{SILENT} ) {
    print STDOUT "\n";
    foreach my $line (@$rTags) {
      foreach my $rfield (@fields) {
	my $value="";
	defined $line->{$rfield->[0]} and $value=$line->{$rfield->[0]};
	printf( "%-" . $rfield->[1] . "s", $value );
      }
      print STDOUT "\n";
    }
  }

  return ( $rcolumns, $rTags );
}

sub f_removeTagValue2 {
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
    $error = $self->{DATABASE}->update($tagTableName, {$attribute => undef}, "file =  ?", {bind_values=>[$file]});
  } else {
    ($self->isDirectory($file)) and $file.="/";

    $error = $self->{DATABASE}->delete($tagTableName, "file = '$file'");
  }
  ($error) or print STDERR "Error doing the update\n" and return;
  
  return 1;
}

sub f_showAllTagValues2 {
  my $self = shift;
  my $path = $self->GetAbsolutePath(shift);

  ($path)
    or $self->{LOGGER}->info("Tag", "Error: not enough arguments in showAllTagValues\nUsage: showTagValue <file> ")
	and return;

  my $tags=$self->f_showTags("all", $path);

  $tags or return;
  my @result=();
  foreach my $entry (@{$tags}) {
    my $tag=$entry->{tagName};
    my $directory=$entry->{path};
    $self->{LOGGER}->info("Tag", "Getting all the '$tag' of $path");
    my $tagTableName = $self->{DATABASE}->getTagTableName($entry->{path}, $tag);

    my $where="";
    if ($tagTableName !~ /^T\d+V$tag$/) {
      $where = "file like '$path%'";
    }

    $self->debug(1, "Checking the tags of $directory and $where ");
    my $rTags = $self->{DATABASE}->getTags($directory, $tag, undef, $where);

    my $rcolumns = $self->{DATABASE}->describeTable($tagTableName);

    push @result, {tagName=>$tag, data=>$rTags, columns=>$rcolumns};

  }

  $self->{LOGGER}->info("Tag", "Done!!");
  return \@result;
}

return 1;
