package AliEn::MSS::castor;

use strict;
use AliEn::MSS;
use IO::Handle;

use vars qw(@ISA);

@ISA = ( "AliEn::MSS" );



sub mkdir {
    my $self = shift;
    my (@args) = @_;

    #    print "In castor, doing nsmkdir -p @args\n";
    my @cmd = ( "nsmkdir", "-p", @args );
    my $error = ( system(@cmd) );

    return $error;
}
sub cp {
    my $self = shift;

    $self->debug(1, "Checking if rfcp exists");
    open (OUTPUT, "which rfcp >& /dev/null|");
    my $done=close(OUTPUT);
    
    $done or return 1;
    my (@args) = @_;
    open (OUTPUT, "rfcp @args >& /dev/null|");
    $done=close(OUTPUT);
    $done or return 1;
    return 0;
}

sub mv {
    my $self = shift;
    my (@args) = @_;
    my @cmd = ( "rfrename", @args );
    return ( system(@cmd) );
}

sub rm {
    my $self = shift;
    my (@args) = @_;
    my @cmd = ( "nsrm", "-f", @args );
    return ( system(@cmd) );
}

sub sizeof {
    my $self = shift;
    my $file = shift;

#    my $size = `nsls -l $file`;
    my $size = `rfdir $file`;
    $size =~ s/^(\S+\s+){4}(\S+)\s.*$/$2/s;

    return $size;
}

sub reclist {
    my $self = shift;
    my $path = shift;
    my $fileInSE = shift;
    $path .="/";
    $path =~ s/\/\//\//g;
    my $cmd = "rfdir $path | ";
    my @dirlist="";;



#    print $cmd,"\n";
    open (INPUT, $cmd);
    while(<INPUT>) {
	my ($perm, $inode, $owner, $group,$lsize, $d1,$d2,$d3, $name) = split (" ",$_);
	if ( ($name eq '.') || ($name eq '..') || ($name eq '/') || ($name eq '') )  {
	  next;
	}
	if ( $perm =~ /drw/) {
#    print "Dir found: $perm $name\n";
	    my $newpath = $path;
	    $newpath .= $name;
	    $newpath .= '/';
	    push @dirlist, $newpath;
	} else {
	    if ( $perm =~ /\-rw/) {
		print "Adding File $path$name \t $lsize\n";
		my $file = {
		        'name'   => "$path$name",
			    'size'   => $lsize/1024,
		    };
		push @{$fileInSE}, $file;
	    }
	}
    }
    close (INPUT);

    return \@dirlist;
}

sub lslist {
    my $self = shift;
    my $searchpath = shift;
    my @alldirs="";
    my @fileInSE;
    my $lalldirs = $self->reclist("$searchpath",\@fileInSE);
    my $file;
    my $dir="";
    my @basedirs="";
    my $count;
    my $last;

    foreach $dir ( @{$lalldirs} ) {
      if (!( $dir eq "") ) {
	push @alldirs, $dir;
#	print "Pushing $dir \n";
      }
    }

    do {
      $count =0;
      @basedirs = "";
      foreach $dir ( @alldirs ) {
	if (!( $dir eq "") ) {
	  #	    print "Treating $#alldirs |$dir|\n";
	  my $newdirs = $self->reclist($dir,\@fileInSE);
	  foreach (@{$newdirs}) {
	    $count++;    
	    $last = 1;
	    #	      print "\r $count";
	    flush STDOUT;
	    push @basedirs, $_;
	    #	      print "Adding Basedir $_\n";
	  }
	}
      }
      
#      print "@basedirs\n";
      #	print "\nSize $#basedirs \n";
      @alldirs = "";
      foreach $dir ( @basedirs ) {
	if (!( $dir eq "") ) {
	  #	    print "Treating $#basedirs |$dir|\n";
	  my $newdirs = $self->reclist($dir,\@fileInSE);
	  foreach (@{$newdirs}) {
	    $count++;
	    $last = 2;
	    ##	      print "\r $count";
	    flush STDOUT;
	    push @alldirs, $_;
	    #	      print "Adding Alldir $_\n";
	  }
	}
      }
      #	  print "\nSize $#alldirs \n";

  } while ($count);
    
return \@fileInSE;
}


sub url {
    my $self = shift;
    my $file = shift;

    return "castor://$self->{HOST}$file";
}

sub setEnvironment{
  my $self=shift;
  my $vars=shift;
  use Data::Dumper;

  my @defined=("STAGE_HOST", "STAGE_POOL");

  if ($vars) {
    foreach my $item (keys %$vars) {
      $item =~ s/^VARS_// or next;
      if (grep (/^$item$/, @defined)) {
	$self->{LOGGER}->info("Castor", "Using $item=".$vars->{"VARS_$item"});

	$ENV{$item} and $self->{"PREVIOUS_$item"}=$ENV{$item};
	$ENV{$item}=$vars->{"VARS_$item"};
	next;
      }
      $self->{LOGGER}->info("Castor", "Ignoring $item");
    }
  }
  return 1;
}


sub unsetEnvironment{
  my $self=shift;

  my @defined=("STAGE_HOST", "STAGE_POOL");

  foreach my $item (@defined) {
    $self->{"PREVIOUS_$item"}and $ENV{$item}=$self->{"PREVIOUS_$item"};
  }
  return 1;
}


sub stage {
  my $self=shift;
  my $file=shift;
  $self->info("Getting ready to stage the file $file from castor");
  system("stager_get","-M", $file) and return;
  $self->info("stager_get executed correctly");
  return 1;
}

return 1;

