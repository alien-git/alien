#!/usr/bin/perl -w

use strict;
use DBQueue::ProcessMonitor;

my $id = @ARGV[0];
shift @ARGV;
my $host = @ARGV[0];
shift @ARGV;
my $port = @ARGV[0];
shift @ARGV;

my $monitor = DBQueue::ProcessMonitor->new( $id, $host, port );

open SAVEOUT,  ">&STDOUT";
open SAVEOUT2, ">&STDERR";
open SAVEIN,   "<&STDIN";

open SAVEOUT,  ">&STDOUT";
open SAVEOUT2, ">&STDERR";
open SAVEIN,   "<&STDIN";

if ( !open STDOUT, ">/tmp/AliLite/proc$id/stdout" ) {
    open STDOUT, ">&SAVEOUT";
    die "stdout not opened!!";
}

if ( !open( STDERR, ">/tmp/AliLite/proc$id/stderr" ) ) {
    open STDOUT, ">&SAVEOUT";
    open STDERR, ">&SAVEOUT2";
    die "stderr not opened!!";
}

if ( !open( STDIN, "/tmp/AliLite/proc$id/stdin" ) ) {
    open( STDOUT, ">&SAVEOUT" );
    open( STDIN,  "&SAVEIN" );
    open( STDERR, ">&SAVEOUT2" );
    die "stdin not opened!!";
}

my @list = split " ", '/tmp/AliLite/proc$id/command';

$ENV{ALIEN_PROC_ID} = $id;

my $error = system(@list);

close STDOUT;
close STDERR;

#    close STDIN;

open STDOUT, ">&SAVEOUT";
open STDERR, ">&SAVEOUT2";
open STDIN,  "<&SAVEIN";
print STDERR "Command executed with $error.\n";
$monitor->finishMonitor();
exit $error;
