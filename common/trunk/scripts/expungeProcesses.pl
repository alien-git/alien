#!/usr/bin/perl -w

use AliEn::UI::Catalogue;

use strict;

my $cat = AliEn::UI::Catalogue->new( { "user", "admin" } );

($cat) or print STDERR "Error getting the catalog\n" and exit;

$cat->execute( "cd", "/proc" );
$cat->execute("host");

my $db =
  AliEn::Database->new(
    { "HOST", "aliendb", "DRIVER", "mysql", "DB", "processes", } );

($db) or print STDERR "Error getting the database\n" and exit;

$db->validateUser("admin")
  or print STDERR "Error validating the database\n"
  and exit;

my (@procs) = $db->query("SELECT queueId FROM QUEUE where status='KILLED'");

my $proc;

foreach $proc (@procs) {

    print STDERR "Deleting $proc\n";
    $cat->execute( "rmdir", "/proc/$proc/" );

    $db->insert("DELETE FROM QUEUE where queueId='$proc'");

}

print STDERR "DONE!!\n";
$db->destroy();
$cat->close();

