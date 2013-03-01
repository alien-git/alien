package AliEn::LCM;
select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use AliEn::Logger;
use vars qw($VERSION);

use AliEn::Config;
use strict;

use AliEn::X509;

use AliEn::SE::Methods;
use AliEn::Database::LCM;
use AliEn::MSS::file;
use AliEn::Service::SubmitFile;
use AliEn::MD5;

use vars qw (@ISA $DEBUG);
push @ISA, 'AliEn::Logger::LogObject';
$DEBUG = 0;

# Use this a global reference.
##############################################################################
#Private functions
##############################################################################
sub startTransferDaemon {
  my $self        = shift;
  my $pfn         = shift;
  my $certificate = (shift or "");
  my $use_cert    = shift;
  defined $use_cert or $use_cert = 0;

  $self->{FILEDAEMON} = AliEn::Service::SubmitFile->new(
    { "pfn"      => $pfn,
      "SEcert"   => $certificate,
      "USE_CERT" => $use_cert,
    }
  ) or return;

  return $self->{FILEDAEMON}->{pfn};
}

sub stopTransferDaemon {
  my $self = shift;

  $self->{FILEDAEMON}->stopTransferDaemon;

  return 1;
}

sub bringRemoteFile {

  my $self      = shift;
  my $pfn       = shift;
  my $SE        = shift;
  my $localFile = (shift or "");

  my $size;
  my $result = "";

  #First, ask to get the file

  $DEBUG and $self->debug(1, "Asking the SE to bring the file");

  my ($transferID) = $self->{RPC}->CallRPC("SE", "bringRemoteFile", $pfn, $SE, $localFile) or return;

  #    my $response=$self->{SE}->bringRemoteFile( $pfn, $SE, $localFile );

  return $self->inquireTransfer("SE", $transferID);
}

sub getLocalCopy {
  my $self      = shift;
  my $guid      = shift;
  my $localFile = shift;

  $guid or return;
  my ($data) = $self->{TXTDB}->queryRow("SELECT localpfn,size,md5sum FROM LOCALGUID where guid='$guid'");
  $DEBUG and $self->debug(1, "Looking for local copies");
  if ($data and $data->{localpfn}) {
    my $file = $data->{localpfn};

    if (-f $file) {
      my $size = -s $file;
      if ($size eq $data->{size}) {
        my $md5sum = AliEn::MD5->new($file);
        if ($md5sum eq $data->{md5sum}) {
          $DEBUG and $self->debug(1, "Giving back the local copy $file");
          $localFile or return $file;
          $DEBUG and $self->debug(1, "Copying '$file' to '$localFile'");
          if ($file ne $localFile) {
            $self->debug("They are not the same ? copy $localFile to $file");
            my $targetDir = $localFile;
            $targetDir =~ s/[^\/]*$//;
            AliEn::Util::mkdir($targetDir) or print STDERR "Error creating $targetDir\n" and return;

            (AliEn::MSS::file::cp({}, $file, $localFile))
              and print STDERR "ERROR copying $file to $localFile $!\n"
              and return;
          }
          return $localFile;
        } else {
          $self->info("The localfile had a different md5sum that we expected...($md5sum instead of $data->{md5sum})");

        }
      } else {
        $self->info("The localfile had a different size that we expected...($size instead of $data->{size})");
      }
    }
    $DEBUG and $self->debug(1, "File $data does no longer exist");
    $self->{TXTDB}->delete("LOCALGUID", "guid='$guid'");
  }
  return;
}



##############################################################################
#Public functions
##############################################################################
sub new {
  my $proto = shift;
  my $self  = {};
  $self->{CONFIG} = shift;
  $self->{CONFIG}->{host} or $self->{CONFIG} = AliEn::Config->new();

  my $class = ref($proto) || $proto;

  $self->{CONFIG} or print STDERR "Error getting the configuration in LCM\n" and return;

  bless($self, $class);
  $self->SUPER::new() or return;

  $self->{RPC} = new AliEn::RPC;

  # Initialize the logger
  $self->{SILENT} = ($self->{CONFIG}->{silent} or 0);

  $self->{X509} = new AliEn::X509;
  my $inittxt = "Initializing Local Cache Manager";
  $self->{DEBUG} = 0;
  if ($self->{debug}) {
    $self->{DEBUG} = 5;
    $self->{LOGGER}->debugOn($self->{debug});
    $inittxt .= " in debug mode";
  }

  $DEBUG and $self->debug(1, $inittxt);

  $self->{CACHE_DIR} = $self->{CONFIG}->{CACHE_DIR};
  if (!(-d $self->{CACHE_DIR})) {
    mkdir $self->{CACHE_DIR}, 0777;
  }
  $self->{TXTDB} =
    AliEn::Database::LCM->new({"DEBUG", $self->{DEBUG}, "CONFIG", $self->{CONFIG}, "LOGGER", $self->{LOGGR}});

  $self->{TXTDB} or $self->{LOGGER}->error("LCM", "Error getting the text database") and return;

  return $self;
}

sub quit {
  my $self = shift;
  $DEBUG and $self->debug(1, "Killing SE\n");

}

sub getFile {
  my $self        = shift;
  my $pfn         = shift;
  my $SE          = (shift or "");
  my $localFile   = (shift or "");
  my $opt         = (shift or "");
  my $lfn         = (shift or "");
  my $guid        = (shift or "");
  my $md5         = (shift or "");
  my $xurl        = (shift or "");
  my $envelope    = (shift or "");
  my $oldEnvelope = (shift or 0);

  $self->info("In getfile, with $SE ");

  ($pfn)
    or $self->{LOGGER}->warning("LCM", "Error no file specified")
    and return;
  $self->{SILENT} or $self->info("Getting the file $pfn");

  if ($opt =~ /f/) {
    $DEBUG and $self->debug(1, "Deleting the local copies of $pfn");
    $self->{TXTDB}->delete("LOCALGUID", "guid='$guid'");
  }

  $DEBUG and $self->debug(1, "Evaluating $pfn");

  my ($result, $size);
  eval {
    my $file = AliEn::SE::Methods->new(
      { DEBUG => ($DEBUG || $self->{DEBUG}),
        PFN => $pfn,
        DATABASE    => $self->{DATABASE},
        LOCALFILE   => $localFile,
        XURL        => $xurl,
        ENVELOPE    => $envelope,
        OLDENVELOPE => $oldEnvelope
      }
    );
    ($file) or die("We are not able to parse the pfn $pfn\n");
    if ($opt =~ /l/) {
      $result = $file->getLink("-s");
    } elsif ($DEBUG) {
      $result = $file->get();
    } else {
      $result = $file->get("-s");
    }
  };

  $DEBUG and $@ and $self->debug(1, "There was an error: $@");

  if ($result) {
    if (($md5) and ($md5 ne "00000000000000000000000000000000")) {
      $self->debug(1, "The copy worked! Let's check if the md5 is right");
      my $newMd5 = AliEn::MD5->new($result);
      $newMd5 eq $md5
        or $self->info(
        "WARNING: The md5sum of the file doesn't match what it is supposed to be (it is $newMd5 instead of $md5)", 1)
        and return;
    }

    $self->info("Everything worked and got $result");
    $self->{TXTDB}->insertEntry($result, $guid);
  } else {
    $self->info("Error getting the file");
  }
  return $result;
}


sub checkPFNisLocal {
  my $self = shift;
  my $local_pfn = shift or return;
  $DEBUG and $self->debug(1, "Checking if $local_pfn is a localfile");
  $local_pfn =~ /^file:\/\// or return;

  my $shortName = $self->{CONFIG}->{HOST};
  $shortName =~ s/\..*$//;

  $local_pfn =~ /^(localhost)|($self->{CONFIG}->{HOST})|($shortName)|(\/)/ or return;
  $DEBUG and $self->debug(1, "It is a local file");
  return 1;
}

sub RegisterInRemoteSE {
  my $self      = shift;
  my $local_pfn = shift;
  my $envelope  = (shift || "");

  #  $ENV{ALIEN_XRDCP_ENVELOPE}=$envelope->{envelope};
  #  $ENV{ALIEN_XRDCP_URL}=$envelope->{turl};

  my $localfile = $self->checkPFNisLocal($local_pfn);

  if (not $localfile) {
    $self->info("The file is not local. Let's retrieve it first");
    my $url = AliEn::SE::Methods->new($local_pfn)
      or $self->info("Error creating the url of $local_pfn")
      and return;
    ($localfile) = $url->get();
    $localfile or $self->info("Error getting the file '$local_pfn'", 1) and return;
    print "Got $localfile\n";
    $local_pfn = "file://$self->{CONFIG}->{HOST}$localfile";
  }
  $self->debug(1, "Trying to upload the file $local_pfn to the SE $envelope->{se}");

  my $url = AliEn::SE::Methods->new({PFN => $local_pfn, DEBUG => $DEBUG})
    or $self->info("Error creating the url of $local_pfn while uploading to the SE: " . $envelope->{se})
    and return;

  if ($url->method() =~ /^file/ and $url->host() !~ /^($self->{CONFIG}->{HOST})|(localhost)$/) {
    $self->info("Error: we are in $self->{CONFIG}->{HOST}, and we can't upload a file from " . $url->host());
    return;
  }

  my $info = {};
  $info->{size} = $url->getSize();
  defined $info->{size}
    or $self->info("Error getting the size of $local_pfn")
    and return;
  $info->{md5} = AliEn::MD5->new($local_pfn);

  $info = $self->getPFNName($info, $envelope);
  $info or return;

  my $url2 = AliEn::SE::Methods->new(
    { PFN       => $info->{pfn},
      DEBUG     => $DEBUG,
      LOCALFILE => $url->path()
    }
    )
    or $self->info("Error creating the url of $local_pfn")
    and return;

  my $done = $url2->put()
    or $self->info("Error uploading the file: $local_pfn")
    and return;

  $self->info("File saved successfully in SE: " . $envelope->{se});
  return ($info);
}

sub getStat {
  my $self      = shift;
  my $local_pfn = shift;
  my $envelope  = (shift || "");

  #my $info=$self->getPFNName($envelope );
  #my $url2=AliEn::SE::Methods->new(PFN=>$info->{pfn},DEBUG=>$DEBUG,
  #                                  LOCALFILE=>$url->path()})
  #  or $self->info("Error creating the url of $local_pfn") and return;

  #return $stat->getStat();
  return 1;
}

#sub getPFNName {
#  my $self=shift;
#  my $info=$self->getPFNNameFromEnvelope(@_);
#  $info and return $info;
#  $self->info("Couldn't get the pfn from the envelope...");
#  $self->getPFNNameFromSE(@_);
#}

sub getPFNName {
  my $self     = shift;
  my $info     = shift;
  my $envelope = shift;
  $self->debug(1, "Can we get the guid and the pfn from the envelope???");

  $envelope and defined($envelope->{turl})
    or $self->info("There is no envelope to get the filename", 1)
    and return;
  my @pfnsplit = split(/\/\//, $envelope->{turl}, 3);
  scalar(@pfnsplit) eq 3 or $self->info("We couldn't get the PFN from the TURL.", 1) and return;
  my $pfn = $pfnsplit[2];

  $info->{guid} = $envelope->{guid};
  if (!$info->{guid}) {
    $info->{guid} = $pfn;
    $info->{guid} =~ s/^.*\/([^\/]*)$/$1/;
    $info->{guid} =~ /^[\dabcdef-]{36}$/i or $self->info("It doesn't have the right format ($info->{guid})") and return;

  }
  $info->{guid} or return;

  $info->{pfn} = $self->rewriteCatalogueRegistrationPFN($envelope->{turl}, $pfn) or return;
  $self->debug(1, "According to the envelope: $pfn and $info->{guid}");
  return $info;
}

sub rewriteCatalogueRegistrationPFN {
  my $self            = shift;
  my $url             = (shift || return);
  my $pfn             = (shift || return);
  my $registrationPFN = $url;
  $registrationPFN =~ s{^([^/]*//[^/]*)//(.*)$}{$1/$pfn};
  $registrationPFN =~ m{root:////} and return;
  return $registrationPFN;
}

sub eraseFile {
  my $self = shift;
  my $url  = shift;

  my $protocol = AliEn::SE::Methods->new({PFN => $url, DEBUG => $self->{DEBUG}})
    or $self->info("Error getting the method for $url")
    and return;

  return $protocol->remove();
}

sub listTransfer {
  my $self      = shift;
  my $doSummary = grep (/^-?-summary$/, @_);
  my $jdl       = grep (/^-jdl$/, @_);
  my $verbose   = grep (/^-verbose$/, @_);
  my $list;
  my $limit = 49;
  $self->info("Checking the transfer @_");
  my ($result) = $self->{RPC}->CallRPC("Manager/Transfer", "listTransfer", @_)
    or return;


  $result =~ /^listTransfer: returns all the transfers/
    and $self->info($result)
    and return;

  my $message = "TransferId\tStatus\t\tUser\t\tDestination\t\t\tSize\t\tSource\t\tAttmpts\n";
  my $format  = "%6s\t\t%-8s\t%-10s\t%-15s\t\t%-12.0f\t\%12s\%12s";
  if ($verbose) {
    $message =~ s/\n/\t\tError reason\n/;
    $format .= "%s";
  }
  if ($jdl) {
    $message =~ s/\n/\t\tJDL\n/;
    $format .= "%s";
  }
  my @transfers   = @$result;
  my $summary     = "";
  my $info        = {};
  my $transferNum = 0;
  $list = join(" ", @_);

  #$self->info("List = $aux and @ = @_");
  $list =~ m/-?-list=(\d+)?/;
  if ($1) {
    if ($1 < $limit) { $limit = $1; }
    elsif ($#transfers > $limit && $1 > $limit) { $self->info("The limit of transfer to show is " . ($limit + 1)); }
  } else {
    if ($#transfers > $limit) {
      $self->info("There are more than $limit tranfers, just showing the " . ($limit + 1) . " first ones");
    }
  }

  #  $self->info("Transfers = ".$#transfers);
  foreach my $transfer (@transfers) {

    #    $DEBUG and $self->debug(3, Data::Dumper($transfer));
    $transferNum++;
    my $tId = $transfer->{transferId} || $transfer->{transferid};
    my (@data) = (
      $tId, $transfer->{status}, $transfer->{user},
      $transfer->{destination}               || "",
      $transfer->{size}                      || 0,
      $transfer->{attempts}, $transfer->{SE} || ""
    );
    $verbose and push @data, $transfer->{reason} || "";
    $jdl     and push @data, $transfer->{jdl}    || "";

    #    #Change the time from int to string

    my $string = sprintf "$format", @data;

    #    $self->info("List = ".$transfer->{transferId});
    if ($transferNum <= $limit) { $message .= "$string\n"; }

    if ($doSummary) {
      my $number = 1;
      $info->{$transfer->{status}} and $number += $info->{$transfer->{status}};
      $info->{$transfer->{status}} = $number;
    }
  }

  if ($doSummary) {
    $message .= "\n\nSummary of all the transfers:\n";
    $self->debug(4, "Ready to do the summary");
    foreach my $status (keys %$info) {
      $message .= "\t\t$status -> $info->{$status}\n";
    }
  }

  $self->info($message, undef, 0);

  return $result;
}

sub killTransfer {
  my $self = shift;
  $self->info("Killing the transfers @_");
  my $user = $self->{CONFIG}->{ROLE};
  my ($result) = $self->{RPC}->CallRPC("Manager/Transfer", "killTransfer", $user, @_) or return;
  

  $self->info("\t" . join("\n\t", @$result), 0, 0);

  return $result;
}

sub checkDiskSpace {
  my $self      = shift;
  my $space     = shift || 0;
  my $localFile = shift;

  $self->debug(1, "Checking if we have $space bytes of diskspace to get the file");

  return 1;

}

sub resubmitTransfer {
  my $self = shift;
  my @args = @_;
  push(@args, "-reset");
  my ($res) = $self->{RPC}->CallRPC("Manager/Transfer", "resubmitTransfer", @args);

  if (!$res) {
    $self->{LOGGER}->error("LCM", "In resubmitTransfer while calling resubmitTransfer, in Manager/Transfer");
    ($res) = $self->{RPC}->CallRPC("Manager/Transfer", "resubmitTransferHelp", @args);
    return;
  }

  $self->info("Transfer Resubmitted");
  return 1;
}

sub getTransferHistory {
  my $self = shift;

  my ($done) = $self->{RPC}->CallRPC("Manager/Transfer", "getTransferHistory", @_);

  $done
    or $self->{LOGGER}->error("LCM", "In getTransferHistory error while calling resubmitTransfer, in Manager/Transfer")
    and return -1;

  $self->info("Transfer history for : @_");
  print "$done\n";
  return 1;
}

sub resubmitFailedTransfers {
  my $self = shift;

  my ($res) = $self->{RPC}->CallRPC("Manager/Transfer", "resubmitFailedTransfers", @_);
  $self->info("Resubmitting all failed Transfers");
  return 1;
}

return 1;

