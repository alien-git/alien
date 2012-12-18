use strict;
use Test;
use Net::Domain qw(hostname hostfqdn hostdomain);

BEGIN { plan tests => 1 }
{

    eval "require AliEn::Database" 
	or print "Error requiring the package\n $! $@\n" and exit(-2);

    eval "require AliEn::Database::AdminDatabase" 
	or print "Error requiring the package\n $! $@\n" and exit(-2);

    print "Creating a new Database\n";
    my $passwd='pass';
    my $admin=new AliEn::Database::AdminDatabase($passwd);
    $admin or print "Error getting the admin database\n" and exit(-2);
#    my $host=Net::Domain::hostname();
#    my $d=AliEn::Database->new({DB=>"processes", HOST=>$host,
#				DRIVER=>"mysql"});
#    $d or print "Error getting the database\n" and exit -1;
 #   $d->validate or exit;
    print "Done!!\n";
}
