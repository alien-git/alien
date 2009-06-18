package AliEn::MSS::file;

@ISA = qw (AliEn::MSS);

use AliEn::MSS;

use strict;

sub mkdir {
    my $self = shift;
    my (@args) = @_;
    my @cmd = ( "mkdir", "-p", @args );

    system(@cmd);
    for (@args) {
      if (! ( -d $_ ) ) {
	return 1;
      }
    }
    return 0;
}
sub link{
  my $self=shift;
  my ($from, $to)=@_;
  (-f $from) or return 1;
  symlink ($from, $to) and return 0;
  return 1;
}
sub cp {
  my $self = shift;
  my (@args) = @_;

  my @cmd = ( "cp", @args );

  my $target=$cmd[$#cmd];
  $target=~ s{[^/]*$}{};
  $self->mkdir($target);
#  if ( $self->{SILENT} ) {
    open SAVEERR, ">&STDERR";
    open SAVEERR, ">&STDERR";
    open STDERR,  ">/dev/null";
#  }
  my $code=system(@cmd);
#  if ( $self->{SILENT} ) {
    close STDERR;
    open STDERR, ">&SAVEERR";
#  }
  return ( $code );
}

sub mv {
    my $self = shift;
    my (@args) = @_;
    my @cmd = ( "mv", @args );
    return ( system(@cmd) );
}

sub rm {
    my $self = shift;
    my (@args) = @_;
    my @cmd = ( "rm", "-f", @args );
    return ( system(@cmd) );
}

sub sizeof {
    my $self = shift;
    my $file = shift;

    if ( -f $file ) {
      my (
            $dev,  $ino,   $mode,  $nlink, $uid,     $gid,  $rdev,
            $size, $atime, $mtime, $ctime, $blksize, $blocks
          )
          = stat($file);
        return ( int($size) );
    }

    return (undef);
}


#################################################################################
# lslist returns a list of file hashes, containing the name and size of all files
# under path $path!

sub lslist {
  my $self = shift;
  my $path = shift;
  my @fileInSE;

  if (! ( -d $path )){
    return @fileInSE;
  }
  
  my $cmd = "find $path -type f -name \"*\" |";
  open (INPUT, $cmd);
  while(<INPUT>) {
    my $name = $_;
    chomp($name);
    my $size = int (($self->sizeof($name))/1024);
    if ($size == 0) { $size = 1;}
    my $file = {
		'name'   => $name,
		'size'   => $size,
	       };
    push @fileInSE, $file;
  }

  close (INPUT);
  return \@fileInSE;
}

sub url {
    my $self = shift;
    my $file = shift;

    my $host=$ENV{'ALIEN_HOSTNAME'}.".".$ENV{'ALIEN_DOMAIN'};
    chomp $host;
    ( UNIVERSAL::isa( $self, "HASH" )) and  $host=$self->{HOST};
    return "file://$host$file";
}

return 1;

