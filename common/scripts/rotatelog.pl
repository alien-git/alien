######################################
##### LOGFILE Rotater-Pipe       #####
##### V-1.1 Andreas Peters@CERN  #####
######################################

# usage $ALIEN_PERL $ALIEN_ROOT/scripts/rotatelog.pl $LOGDIR/$FILE.fifo $LOGDIR/$FILE.log $MASTERPID $ALIEN_MAXLOGFILESIZE

# if <logfile> is bigger than size, it is renamed
# to logfile.1 and gzipped, then logfile.2 aso.

use strict;
use POSIX;
use File::Basename;
my $fifo = shift;
my $filename = shift;
my $masterpid = (shift or 0);
my $truncsize = (shift or 10000000);
my $buf;
my $i;

sub alarm_zap {
#    print "Alarm zap !\n";
    if ($masterpid ne "0") {
	# check if our parent process still lives 
	if (!( kill 0, $masterpid) ) {
	    # our master process to do the logging for has died - we can die
	    print "Masterpid of $filename $masterpid died - I exit!\n";
	    exit(-1);
	} else {
	#    print "Masterpid $masterpid is still alive!\n";
	}
    }
    alarm 5;
}


print "Starting up for $filename\n";

# remove old rotate pipe
#if ( -r "$filename.rl.pid" ) {
#    system("kill -0 `cat $filename.rl.pid` && kill -9 `cat $filename.rl.pid`");
#}

system("echo $$ > $filename.rl.pid");

open FIFO , "$fifo";
print "Opened fifo for $filename\n";
#fcntl(FIFO,F_SETFL(),O_NONBLOCK) or print "Could not set FIFO to non blocking\n";

open OUTFILE , ">$filename" or print STDERR "Cannot open $filename\n" and die;

$SIG{PIPE} = sub { printf "Got Sig Pipe\n" };
my $sumread=0;

alarm 1;

while (1) {
    $SIG{ALRM} = \&alarm_zap;  # set the alarm timer handler
    my $nread = sysread (FIFO,$buf,1);

    $sumread++;
#    printf "Did read of $nread\n";
    if (defined $nread) {
	if ($nread !=0) {
	    my $nwrite=syswrite(OUTFILE,$buf,$nread);
	    if ($nwrite!=$nread) {
		print STDERR "Error writing to file $filename!\n";
		die;
	    }
	} 
    } else {
	if ($! == EAGAIN() ) {
	    next;
	} else {
	    print STDERR "Pipe is closed!\n";
	    die;
	}
    }

    my $character = ord $buf;

    if (($sumread>1024) && ($character == 10 )) {
	$sumread=0;
	my ($dev,$ino,$mode,$nlink,$uid,$gid,$rdev,$size,
	    $atime,$mtime,$ctime,$blksize,$blocks)
	    = stat($filename);

	if ($size >  $truncsize) {
	    printf "Size $size Truncsize $truncsize\n";
	    my($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)
		= localtime(time);
	    $year += 1900;
	    $mon+= 1;
	    $mon = sprintf "%02d",$mon;
	    $mday = sprintf "%02d",$mday;
	    $sec = sprintf "%02d",$sec;
	    $min = sprintf "%02d",$min;
	    $hour = sprintf "%02d",$hour;
	    close OUTFILE;
	    my $newfilename = basename $filename;
	    my $newdirname;
	    $filename =~ /(.*)\.log/;

	    if ( $1 ne "" ) {
		$newdirname = $1;
	    } else {
		$newdirname = $filename;
	    }

	    printf "$mday.$mon.$year-$hour:$min:$sec  => Rotating File $filename ....\n";
	    printf "Archive name > $newdirname.archive\n";
	    system("mkdir -p $newdirname.archive/$year/$mon");
#	    printf "$filename-archive/$year/$mon/$newfilename.$mday.$mon.$year-$hour:$min:$sec\n";
	    rename $filename,"$newdirname.archive/$year/$mon/$newfilename.$mday.$mon.$year-$hour:$min:$sec";
	    system("gzip $newdirname.archive/$year/$mon/$newfilename.$mday.$mon.$year-$hour:$min:$sec >& /dev/null &");
	    open OUTFILE , ">$filename" or print STDERR "Cannot open $filename\n" and die;
	}
    }
}


	
    
