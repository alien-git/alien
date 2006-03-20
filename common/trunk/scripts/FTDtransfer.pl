use strict;
use URI::URL;
use SOAP::Lite on_fault => sub { };

use Getopt::Long;

my $options = {
    'v' => 0,
    'B' => 0,
    'h' => 0
};

( Getopt::Long::GetOptions( $options, "v!", "B!", "h!" ) ) or exit;

my $method    = shift;
my $localURL  = shift;
my $remoteURL = shift;

if ( $options->{h} ) {
    usage();
    exit;
}

if ( !($localURL) ) {
    usage();
    exit;
}

if ( ( $method ne "put" ) && ( $method ne "get" ) ) {
    usage();
    exit;
}

transfer( $method, $localURL, $remoteURL );

sub transfer {
    my $method    = shift;
    my $localURL  = shift;
    my $remoteURL = shift;
    if ( !($localURL) ) {
        return 0;
    }
    my $rURL = new URI::URL($remoteURL);
    my $lURL = new URI::URL($localURL);

    if ( $options->{v} ) {
        if ( $method eq "put" ) {
            print "Trying to put $localURL to $remoteURL\n";
        }
        if ( $method eq "get" ) {
            print "Trying to get$remoteURL to $localURL\n";
        }
    }

    my $FTDhost = $lURL->host;
    my $FTDport = $lURL->port;
    if ( !$FTDport ) {
        $FTDport = 8091;
        $lURL->port($FTDport);
    }
    my $response =
      SOAP::Lite->uri("AliEn/Services/FTD")->proxy("http://$FTDhost:$FTDport")
      ->requestTransfer( $lURL->as_string, $rURL->as_string, $method );

    print "Contacting FTD at $FTDhost:$FTDport\n";
    if ( !($response) ) {
        print "Error contacting FTD http://$FTDhost:$FTDport\n";
        exit;
    }

    my $ID = $response->result;
    if ( $ID < 0 ) {
        print "Server returned an error of  $ID\n";
        exit;
    }

    if ( $options->{B} ) {
        my $run = 1;
        while ($run) {
            sleep(20);
            if ( $options->{v} ) {
                print "Asking FTD about filetransfer with is=$ID\n";
            }
            my $soap =
              SOAP::Lite->uri("AliEn/Services/FTD")
              ->proxy("http://$FTDhost:$FTDport")->inquireTransferByID($ID);
            if ( !$soap ) {
                print "Error contacting http://$FTDhost:$FTDport";
                $run = 0;
                exit;
            }
            my $STATUS = $soap->result();
            my $URL    = new URI::URL( $soap->paramsout );
            if ( $STATUS eq "FAIL" ) {
                $run = 0;
                print "Transportation of file to "
                  . $URL->path . " on "
                  . $URL->host
                  . " failed.\n";
                exit;
            }
            if ( $STATUS eq "DONE" ) {
                $run = 0;
                print "File exists in "
                  . $URL->path . " on "
                  . $URL->host . "\n";
            }
        }
    }
}

sub usage {
    print "Usage: [options] {get|put} localURL remoteURL\n";
    print " Options:\n";
    print "  -v: Print information to stdout\n";
    print "  -B: Block. Will block til transfer is done\n";
    print "  -h: Print this screen\n";

}
