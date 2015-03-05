#!/usr/bin/perl -w

use Getopt::Long ();
use AliEn::Broker;
{
    $options = {
        'user'     => "",
        'debug'    => 0,
        'password' => "",
    };

    (
        Getopt::Long::GetOptions(
            $options, "help", "user=s", "password=s", "debug=n"
        )
      )
      or exit;

    my $broker = AliEn::Broker->new($options);

    $broker or exit;
    $broker->checkSubmittedJobs();
}

exit;
