use Data::Dumper;
use XML::Simple;
package AliEn::Dataset;

use strict;

sub new {
    my $proto = shift;
    my $self  = {};
    bless( $self, ( ref($proto) || $proto ) );
    $self->{XMLhash} = ();
    $self->{XML} = "";
    $self->{XMLhashsplit} = 0;
    $self->{XMLflathash} = ();
    return $self;
}

sub setarray {
    my $self = shift;
    my $arrayref = shift;
    my $collectionname = (shift or "default");
    my $command = (shift or "unknown");
    my $date    = (shift or `date`);
    my $timestamp = (shift or time);
    my $creator    = (shift or "unknown");
    my $tag;

    chomp $date;

#    $tag->{name} = "collection";
    $tag->{collection}->{name} = "$collectionname";
    $tag->{collection}->{info}->{command} = $command;
    $tag->{collection}->{info}->{date}    = $date;
    $tag->{collection}->{info}->{creator} = $creator;
    $tag->{collection}->{info}->{timestamp} = $timestamp;
    my $cnt = 0;
    foreach my $levent (@$arrayref) {
	$cnt++;
#	print "Adding $levent\n";
	# loop over events
	foreach my $lfile (keys %$levent) {
#	    print "Adding $lfile\n";
	    #loop over files
	    $tag->{collection}->{event}->{$cnt}->{file}->{$lfile} = $levent->{$lfile};
	}
    }
    $self->{XMLhash} = $tag;
}

sub print {
    my $self = shift;
    my $dumper = new Data::Dumper([$self->{XMLhash}]);
    print $dumper->Dump();
}

sub getarray {
    my $self = shift;
}

sub getflathash {
    my $self = shift;
    my $deephash = shift;
    my $flathash = shift;

    $self->deeptoflathash($deephash,$flathash);
}

sub writexml {
    my $self = shift;
    my $xmlhash = (shift or $self->{XMLhash});
    my $writesplit = (shift or 0);
    my $xsimple = XML::Simple->new();
    
    if ($writesplit) {
	if (defined $self->{XMLhashsplit}) {
	    foreach (@{$self->{XMLhashsplit}}) {
#		print "Adding $_->{collection}->{index}\n";
		$xmlhash->{subcollection}->{"$_->{collection}->{index}"} = $_;
	    }
	}
    }
    $self->{XML} = $xsimple->XMLout($xmlhash, RootName => 'alien' ,
    xmldecl => '<?xml version="1.0"?>'
);
    return $self->{XML};
}


sub readxml {
    my $self = shift;
    my $xml = (shift or $self->{XML});
#    print "Reading $xml\n";
    my $xsimple = XML::Simple->new();
    $self->{XMLhash} = $xsimple->XMLin($xml, 
				       KeyAttr => {event => 'name', file => 'name', mirror => 'name'},
				       ForceArray => [ 'event' , 'file' , 'mirror' ],
				       ContentKey => '-content');
    return $self->{XMLhash};
}


# event - means to put 1 event per job
# se   - means to group the jobs depending on their SE location
# none - means to apply only the max number of files or size criteria for the splitting

sub split {
    my $self = shift;

    my $type = lc(shift or "event");  # can be event,se,none)

    my $maxnevent = (shift or "0"); # 0 = no limit
    my $maxsize  = (shift or "0"); # 0 = no limit

    my $seList = (shift or {});
      # in case of type=se seList is a hash, key is a hash ref including maxEvents and maxSize
      # e.g. $seList = { 'lxshare006d::CERN::File' => {maxEvents => 3}, 'Alice::CERN::Scratch3' => {maxEvents => 2}};
      # in case of type=proof seList is a hash, key is SE name(s), value max jobs per this se
      # e.g. $seList = { 'lxshare006d::CERN::File' => 3, 'Alice::CERN::Scratch3' => 2};
#    $seList = { 'lxshare006d::CERN::File' => 3, 'Alice::CERN::Scratch3' => 1};

    if ($type eq "event") {
      $maxnevent = 1;
    }

    my $debug = 40;

    if ($debug>0) {print "Executing Split\n";}

    if ( ($type ne "event") && ($type ne "se") && ($type ne "none") && ($type ne "proof")) {
      print STDERR "The split type has to be one of 'event'/'se'/'none'/'proof'\n";
      return;
    }

    if ($self->{XMLhash} eq "") {
      print STDERR "Not XMLhash to split!\n";
      return;
    }

    if ($debug > 10) {
      print "Input data: \n" . Data::Dumper->Dump([$self->{XMLhash}]);
    }

      # create meta data for faster processing
    my @eventMetaData;
    my ($key, $value);
    print "Processing events: " if ($debug);
    while (($key, $value) = each(%{$self->{XMLhash}->{collection}->{event}})) {
      print $key . " " if ($debug);
      my $eventMetaData = {};

      my $numberOfFiles = -1;
      my $fileSize = -1;
      my @SEs;
        # strange hack neccessary because the filename is taken as key and could be "file" or "mirror"
      if (exists $value->{file} and exists $value->{file}->{name} and ref($value->{file}->{name}) ne "HASH") {
        $numberOfFiles = 1;
        $fileSize = $value->{file}->{size};
          # check if there are several mirrors
        if (exists $value->{file}->{mirror}->{name} and ref($value->{file}->{mirror}->{name}) ne "HASH") {
          @SEs = ($value->{file}->{mirror}->{name});
        } else {
          @SEs = keys %{$value->{file}->{mirror}};
        }
      } else {
        $numberOfFiles = 0;
        $fileSize = 0;
        my %SEs;
        for (values %{$value->{file}}) {
          $numberOfFiles++;
          $fileSize += $_->{size};
          my @SEs;
            # check if there are several mirrors
          if (exists $_->{mirror}->{name} and ref($_->{mirror}->{name}) ne "HASH") {
            @SEs = ($_->{mirror}->{name});
          } else {
            @SEs = keys %{$_->{mirror}};
          }
          for (@SEs) {
            $SEs{$_}++;
          }
        }
          # only take SEs which store all the files
        for (keys %SEs) {
          push @SEs, $_ if ($SEs{$_} == $numberOfFiles);
        }
      }

      my @sortedSEs = sort(@SEs);
      my $SEs = join("!", @sortedSEs);
      unless ($SEs) {
        print "\n" if ($debug);
        print "WARNING: We have an event without subset of SEs: $key\n";
        print "Processing events: " if ($debug);
      }

      $eventMetaData->{numberOfFiles} = $numberOfFiles;
      $eventMetaData->{totalFileSize} = $fileSize;
      $eventMetaData->{SEKey} = $SEs;
      $eventMetaData->{SEs} = \@sortedSEs;
      $eventMetaData->{numberSEs} = $#SEs+1;
      $eventMetaData->{event} = $key;

      push @eventMetaData, $eventMetaData;
    }
    print "\n" if ($debug);

    if ($debug > 10) {
      print "Built eventMetaData:\n" . Data::Dumper->Dump(\@eventMetaData);
    }

      # create a SE to event metadata map
    my %SEMap;
    if ($type eq "se") {
      for (@eventMetaData) {
        unless (exists($SEMap{$_->{SEKey}})) {
          $SEMap{$_->{SEKey}} = [];
        }
	printf("Found SE $_->{SEs}\n");
        push @{$SEMap{$_->{SEKey}}}, $_;
      }

      if ($debug > 10) {
        print "Built SEMap:\n" . Data::Dumper->Dump([\%SEMap]);
      }
    }

    my @eventGroups = ();

    my @eventsToProcess;

    if ($type eq "proof") {
        # create slots
      my $slots = {};
      for (keys %$seList) {
        if ($seList->{$_} > 0) {
          $slots->{$_} = { 'events' => 0, 'numberSlots' => $seList->{$_},
                           'slots' => [], 'pos' => 0 };
        }
      }

      my $eventsLeft = [];

      my $run = 0;
      my $processed = 0;
      while ($processed < $#eventMetaData+1) {

        for (@eventMetaData) {
          if ($_->{numberSEs} == $run) {
            ++$processed;

            my $bestSE = undef;
            my $rating = undef;
            for (@{$_->{SEs}}) {
              if (exists $slots->{$_}) {
                my $newRating = $slots->{$_}->{events} / $slots->{$_}->{numberSlots};
                if (!defined $rating or $newRating < $rating) {
                  $rating = $newRating;
                  $bestSE = $slots->{$_};
                }
              }
            }
            if (defined $bestSE) {
              push @{$bestSE->{slots}->[$bestSE->{pos}]}, $_->{event};
              $bestSE->{pos} = 0 if (++$bestSE->{pos} == $bestSE->{numberSlots});
              ++$bestSE->{events};
            } else {
              push @$eventsLeft, $_;
            }
          }
        }

        print "Built Slots ($run):\n" . Data::Dumper->Dump($slots) if ($debug > 10);

        ++$run;
        if ($run == 10000) {
          print "ERROR run=$run something seems to be wrong\n";
          last;
        }
      }

      if ($debug > 10) {
        print "Built Slots:\n" . Data::Dumper->Dump($slots);
      }

      for (values %$slots) {
        for (@{$_->{slots}}) {
          if ($#{$_} > -1) {
            push @eventGroups, $_;
          }
        }
      }

      push @eventsToProcess, $eventsLeft;

    } elsif ($type eq "se") {
      @eventsToProcess = values %SEMap;
    } else {
      @eventsToProcess = (\@eventMetaData);
    }

      # external params
    my $paramMaxEvents = $maxnevent;
    my $paramMaxSize = $maxsize;

    for (@eventsToProcess) {

      if ($type eq "se") {
        if (exists($seList->{$_->[0]->{SEs}})) {
          $paramMaxEvents = ($seList->{$_->[0]->{SEs}}->{maxEvents} or 0);
          $paramMaxSize = ($seList->{$_->[0]->{SEs}}->{maxSize} or 0);
        } else {
          $paramMaxEvents = $maxnevent;
          $paramMaxSize = $maxsize;
        }
      }

      my $eventGroup = [];
      my $numberOfEvents = 0;
      my $totalFileSize = 0;

      for (@$_) {
          # entry full?
        if ($#{$eventGroup} > -1 and
            (($paramMaxEvents != 0 and $numberOfEvents+1 > $paramMaxEvents) or
            ($paramMaxSize != 0 and $totalFileSize+$_->{totalFileSize} > $paramMaxSize))) {
          push @eventGroups, $eventGroup;
          $eventGroup = [];
          $numberOfEvents = 0;
          $totalFileSize = 0;
      } 

        push @$eventGroup, $_->{event};
        $numberOfEvents++;
        $totalFileSize += $_->{totalFileSize};
      }
      if ($#{$eventGroup} > -1) {
        push @eventGroups, $eventGroup;
      }
    }

    if ($debug) {
      if ($debug > 10) {
        print "Built EventGroups:\n" . Data::Dumper->Dump(@eventGroups);
      }
      print "We group the following events together:\n";
      for (@eventGroups) {
        print join(" ", @$_), "\n";
      }
    }

    my @xmlhashsplit = ();
    my $index=0;
    for (@eventGroups) {
      my $splithash = {};
      $index++;
      $splithash->{collection}->{name}  =  "$self->{XMLhash}->{collection}->{name}-$index";
      $splithash->{collection}->{father}  =  "$self->{XMLhash}->{collection}->{name}";
      $splithash->{collection}->{index}   = $index;
      $splithash->{collection}->{info}->{split} = $type;
      $splithash->{collection}->{info}  = $self->{XMLhash}->{collection}->{info};

#      $splithash->{collection}->{event} = $_;

       $splithash->{collection}->{event} = {};
       for (@$_) {
         $splithash->{collection}->{event}->{$_} = $self->{XMLhash}->{collection}->{event}->{$_};
       }

      push @xmlhashsplit, $splithash;
    }

    if ($debug > 50) {
      print "Built xmlhashsplit:\n" . Data::Dumper->Dump(\@xmlhashsplit);
    }

    $self->{XMLhashsplit} = \@xmlhashsplit;
    return $self->{XMLhashsplit};
}

sub getSubEntryList {
  my $listRef = shift;

  my @list;

  if (ref($listRef) eq "ARRAY") {
    @list = @$listRef;
  } elsif ($listRef) {
    push @list, $listRef;
  }

  return @list;
}

sub printsplit {
    my $self = shift;
    my $array = $self->{XMLhashsplit};
    return if (!$array);
    foreach (@$array) {
	print "------------------------------------------------------------------------------------\n";
	my $dumper = new Data::Dumper([$_]);
	print $dumper->Dump();
    }

    print "------------------------------------------------------------------------------------\n";
}

sub childkeys{
    my $flathash = shift;
    my $privkey = shift;
    my $lhash = shift;
    if (ref($lhash) eq "HASH") {

        foreach (keys %$lhash ) {
            childkeys($flathash,$privkey."/".$_,$lhash->{$_});
        }
    } else {
        $flathash->{"$privkey"} = $lhash;
    }
}

# converts deep hashes like ->{1}->{2}->{3} into the 1d structure
# ->{/1/2/3}

sub deeptoflathash {
    my $self = shift;
    my $deephash = (shift or $self->{XMLhash});
    my $flathash = (shift or $self->{XMLflathash});
    my $root     = (shift or "");

    childkeys($flathash, $root,$deephash);
}
sub getAllLFN {
  my $self=shift;
  my @list=();
  $self->{XMLhash} and $self->{XMLhash}->{collection} and 
    $self->{XMLhash}->{collection}->{event} or return;
  my $events=$self->{XMLhash}->{collection}->{event};
  #Loop over the events
  foreach my $entry (keys %{$events}){
    #Loop over the files 
    $events->{$entry}->{file} or next;
    foreach my $file (keys %{$events->{$entry}->{file}} ) {
      $events->{$entry}->{file}->{$file}->{lfn} or next;
      print "Tenemos el fichero $events->{$entry}->{file}->{$file}->{lfn}\n";
      push @list, $events->{$entry}->{file}->{$file}->{lfn};
    }
  }

  return {lfns=>\@list};
}
return 1;
