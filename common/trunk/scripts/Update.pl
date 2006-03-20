#!/usr/bin/perl -w

use AliEn::Config;
use AliEn::Database;
use AliEn::Services::Sync;
use strict;
use Getopt::Long ();

my $options = {
    'user'   => 'admin',
    'debug'  => 0,
    'silent' => '1',
};

(
    Getopt::Long::GetOptions(
        $options,     "help",     "exec=s", "token=s",
        "password=s", "silent=n", "debug=n"
    )
  )
  or exit;

my $config = AliEn::Config->new();

($config) or print STDERR "Error getting the configuration\n" and exit;

my $db = AliEn::Database->new(
    {
        "DB"     => $config->{CATALOG_DATABASE},
        "HOST"   => $config->{CATALOG_HOST},
        "DRIVER" => $config->{CATALOG_DRIVER},
        "DEBUG"  => $options->{debug},
        "SILENT" => $options->{silent}
    }
);

($db) or exit;
$db->validateUser( $options->{user} ) or exit;
$db->{noproxy} = 1;
print STDERR "Update of whole database\n";
AliEn::Services::Sync->updateDatabase($db);
print STDERR "Update done!!\n";

$db->destroy;

