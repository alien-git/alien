package AliEn::Command;

use strict;
use AliEn::Config;
use AliEn::Logger;
use AliEn::MSS::file;
use LWP::UserAgent;

use AliEn::SOAP;

sub new {
  my $proto = shift;
  my $class = ref($proto) || $proto;
  my $self  = (shift or {});
  bless($self, $class);

  $self->{CONFIG} = AliEn::Config->new();
  $self->{CONFIG} or return;

  $self->{LOGGER} = new AliEn::Logger;
  $self->{LOGGER} or return;

  $self->{SOAP} = new AliEn::SOAP;
  return $self;
}

sub Initialize {
  my $self = shift;

  # Default working directory is home direcotry
  $self->{WORK_DIRECTORY} = $ENV{ALIEN_WORKDIR};

  chdir $self->{WORK_DIRECTORY}
    or print STDERR "ERROR: Cannot cd $self->{WORK_DIRECTORY}}  -> $!\n" and return;

  print "Working Directory is $self->{WORK_DIRECTORY}\n";

  system("ls", "-l", $self->{WORK_DIRECTORY});
  system("printenv | grep ALIEN");
  my $pack;
  my $version;

  #    $self->{LOGGER}->debugOn();
  #    $self->{CONFIG}=$self->{CONFIG}->Reload({"PACKCONFIG", 1, "force",1, "DEBUG","PackMan"});
  #    print "LISTING ALL THE PACKAGES\n";
  #    $self->{CONFIG}->{PACKMAN}->List;
  $self->{LOGGER}->info("Command", "Packages required $ENV{ALIEN_PACKAGES}");

  foreach my $pack (split(" ", $ENV{ALIEN_PACKAGES})) {
    $self->{LOGGER}->info("Command", "Adding $pack");
    my $version = "";
    $pack =~ s/::(\S*)$// and $version = $1;
    $version or $version = $self->{CONFIG}->{PACKMAN}->GetLatestVersion($pack);

    #	$version and $version =~ s/^v//i;
    $self->{"require"}->{$pack} = $version;
  }
  my $packref  = $self->{"require"};
  my @packages = keys(%$packref);

  foreach $pack (@packages) {
    $version = ($self->{"require"}->{$pack} or "current");

    $self->{CONFIG}->{PACKMAN}->Configure("$pack", $version) or return;
  }

  $self->InitializeSaveSE   or return;
  $self->InitializeLocalMSS or return;
  $self->InitializeSOAP     or return;
  return 1;
}

sub InitializeSOAP {
  my $self = shift;
  $self->{SOAP}->{CLUSTERMONITOR} =
    SOAP::Lite->uri("AliEn/Service/ClusterMonitor")->proxy("http://$ENV{ALIEN_CM_AS_LDAP_PROXY}");
  return $self->{SOAP};
}

sub InitializeLocalMSS {
  my $self = shift;

  #    $self->{LOGGER}->debugOn();
  my $name = ($ENV{ALIEN_SE_FULLNAME} || "");
  $name
    or $self->{LOGGER}->warning("Command", "Warning! Data is not kept in the local mass storage system")
    and return 1;

  $self->{CONFIG}->{SE_FULLNAME} = $name;

  my $type = ($ENV{ALIEN_SE_MSS} || "");

  $self->{CONFIG}->{SE_MSS}     = $type;
  $self->{CONFIG}->{SE_SAVEDIR} = $ENV{ALIEN_SE_SAVEDIR};

  $name = "AliEn::MSS::$type";
  eval "require $name"
    or $self->{LOGGER}->warning("Command", "Error: $name does not exist $!")
    and return;
  print "Making a local copy in $name (in $ENV{ALIEN_SE_SAVEDIR}) \n";

  # don't create the LVM for the temporary MSS
  $self->{MSS} = $name->new($self, "0");

  $self->{MSS}
    or $self->{LOGGER}->warning("Command", "Error: getting an instance of $name")
    and return;

  return 1;
}

sub InitializeSaveSE {
  my $self = shift;

  $self->{SaveSEs} = ($ENV{ALIEN_SaveSEs} || "");

  if ($self->{SaveSEs}) {
    my @sites = split("###", $self->{SaveSEs});
    print "Copying the output in @sites\n";
  }
  return 1;
}

sub changeStatus {
  my $self = shift;
  foreach my $data (split(/\s+/, $ENV{ALIEN_VOs})) {
    my ($org, $cm, $id, $token) = split("#", $data);
    $self->{SOAP}->{"CLUSTERMONITOR_$org"} = SOAP::Lite->uri("AliEn/Service/ClusterMonitor")->proxy("http://$cm");

    $self->{LOGGER}->info("Command", "Putting the status of $id to @_");
    my $done = $self->{SOAP}->CallSOAP("CLUSTERMONITOR_$org", "changeStatusCommand", $id, @_);

    $done and $done = $done->result;
    if (!$done) {

      print STDERR "Error contacting the ClusterMonitor\nGoing to the Manager/Job";
      my $done = $self->{SOAP}->CallSOAP("Manager_Job_$org", "changeStatusCommand", $id, @_);
    }
  }
}

sub putJobLog {
  my $self = shift;
  foreach my $data (split(/\s+/, $ENV{ALIEN_VOs})) {
    my ($org, $cm, $id, $token) = split("#", $data);
    $self->{SOAP}->{"CLUSTERMONITOR_$org"} = SOAP::Lite->uri("AliEn/Service/ClusterMonitor")->proxy("http://$cm");

    $self->{LOGGER}->info("Command", "putting joblog for job $id:  @_");
    my $done = $self->{SOAP}->CallSOAP("CLUSTERMONITOR_$org", "putJobLog", $id, @_);

    $done and $done = $done->result;
    if (!$done) {

      print STDERR "Error contacting the ClusterMonitor\nGoing to the Manager/Job";
      my $done = $self->{SOAP}->CallSOAP("Manager_Job_$org", "putJobLog", $id, @_);
    }
  }
}

sub Submit {
  my $self = shift;

}

sub GetDefaultVersion {
  my $self = shift;
  return "";
}

sub Execute {
}

sub Register {
  my $self       = shift;
  my $file       = shift;
  my $output     = shift;
  my $pfnoptions = (shift or "");
  my $localdir   = (shift or "");
  my $selist     = (shift or "");

  ($file) or print STDERR "No file to register\n" and return;
  ($output) or print STDERR "No name in the catalog to register\n" and return;

  my $savename = "";
  $localdir and $savename = "$localdir/$output";

  #    print "Registering $file as $output\n";
  my $url    = "";
  my $size   = AliEn::MSS::file->sizeof($file);
  my $lfn    = "/proc/$ENV{ALIEN_PROC_ID}/job-output/$output";
  my $lfndir = "/proc/$ENV{ALIEN_PROC_ID}/job-output";
  my $done;
  my $alldone = "OK";

  #    $self->{CATALOGUE}->execute("whoami");

  # we register with aioput directly !

  my @registerselist = split '\|', $selist;
  my $registersetag;
  my $registeranchor = "";
  my $anchorfiles    = "";
  $self->{LOGGER}->info("Command", "Registration: @registerselist");

  if ($#registerselist == -1) {
    push @registerselist, "closeSE";
  }

  foreach $registersetag (@registerselist) {

    my $registerse;
    my $registerprotocol;

    if ($registersetag =~ /\#/) {
      if ($registersetag =~ /(.*)\^(.*)(\#)(.*)(\[)(.*)(\])/) {
        $registerse       = $1;
        $registerprotocol = $2;
        if ($4 ne "") {
          $registeranchor = $4;
        } else {
          $registeranchor = "";
        }
        if ($6) {
          $anchorfiles = $6;
        } else {
          $anchorfiles = "";
        }
      } else {

        # not yet specified
      }
    } else {
      if ($registersetag =~ /(.*)\^(.*)/) {
        $registerse       = $1;
        $registerprotocol = $2;
      } else {
        $registerse       = $registersetag;
        $registerprotocol = "local";
      }
    }

    if ((($registerse eq "") && ($self->{CONFIG}->{SE_FULLNAME} =~ /LCG/)) || ($registerse =~ /LCG/)) {
      $registerprotocol = "local";
    }

    $self->{LOGGER}->info("Command", "Registration: File $file LFN $lfn SE $registerse PROTOCOL $registerprotocol");
    if ($registerprotocol eq "aio") {

      #        $self->{LOGGER}->info("Command", "aioput $file $lfn $registerse");
      $done = $self->{CATALOGUE}->aioput($file, $lfn, $registerse);
      if (!$done) {
        $done             = "OK";
        $registerprotocol = "local";
      }
    }

    if ($registerprotocol eq "aioforce") {

      #        $self->{LOGGER}->info("Command", "aioput $file $lfn $registerse");
      do {
        $done = $self->{CATALOGUE}->aioput($file, $lfn, $registerse, $pfnoptions);
        if (!$done) {
          sleep 60;
        }

        $self->{LOGGER}->info("Command", "Registration: aioput gave $done");
        if ($done eq "-1") {
          $self->{LOGGER}->info("Command", "Registration: Fatal! I cannot get my aio keys!");
          return;
        }
      } while (!$done);
    }

    if ($registerprotocol eq "mirror") {
      $self->{LOGGER}->info("Command", "mirror $lfn $registerse");
      my $options = "-b -n";
      $done = $self->{CATALOGUE}->execute("mirror", $lfn, $_, $options);
    }

    if ($registerprotocol eq "mirrormaster") {
      $self->{LOGGER}->info("Command", "mirror $lfn $registerse");
      my $options = "-b -n -m";
      $done = $self->{CATALOGUE}->execute("mirror", $lfn, $_, $options);
    }

    if (($registerprotocol eq "local") || ($registerprotocol eq "")) {
      $self->{LOGGER}->info("Command", "register local $lfn $registerse");
      if ($self->{MSS}) {
        my $result = $self->{SOAP}->CallSOAP("SE", "getFileName", $size, $savename);
        if (!($self->{SOAP}->checkSOAPreturn($result, "SE"))) {
          $self->{LOGGER}->error("Command", "Error getting the file name");
          $done = "";
          next;
        }
        my $target = $result->result;
        $url = $self->{MSS}->save($file, $target);

        if (!$url) {
          $self->{LOGGER}->warning("Command", "Error: copying the file $file to $savename");
          $self->{SOAP}->CallSOAP("SE", "removeFileFromLVM", $size, $savename);
          $done = "";
          next;
        }
        $done = $self->{CATALOGUE}->execute("register", $lfn, "$url", "$size", $ENV{ALIEN_SE_FULLNAME});
      }

      if ($self->{SaveSEs}) {
        my @sites = split("###", $self->{SaveSEs});

        foreach (@sites) {
          print "\n\nCopying $url in $_  $savename\n";
          if ($url) {
            print "This is a mirror of the file\n";
            my $options = "-n";
            $localdir and $options .= "f";

            $self->{CATALOGUE}->execute("mirror", $lfn, $_, $options);
          } else {
            print "This is the first copy\n";
            $done = $self->{CATALOGUE}->execute("add", $lfn, $url, $_, $ENV{ALIEN_SE_MSS}, $localdir, "-n");
            (my $newse, $url) = $self->{CATALOGUE}->execute("whereis", $lfn);
          }
        }
      }
    }

    if (($registeranchor =~ /addmirroranchor/) || ($registeranchor =~ /addmirrorlinkanchor/)) {
      print "Adding mirrors to the anchor files";
      my @afiles = split ",", $anchorfiles;
      (my $newse, $url) = $self->{CATALOGUE}->execute("whereis", $lfn);
      foreach (@afiles) {
        if ($url =~ /\#/) {
          print "Fatal: the master file contains already an anchor, cannot add another one!\n";
          $done = "";
        } else {
          my ($url1, $url2) = split '\?', $url;
          my $newurl = "$url1" . '\#' . "$_";
          if ($url2 ne "") {
            $newurl .= "\?$url2";
          }

          if ($registeranchor =~ /addmirroranchor/) {
            $done = $self->{CATALOGUE}->execute("addMirror", "$lfndir/$_", "$newurl", "$newse");
            if (!$done) {
              print "Fatal: Cannot add URL $newurl/$newse to $lfndir/$_\n";
              last;
            }
          }

          if ($registeranchor =~ /addmirrorlinkanchor/) {
            my $lfnlink = "LFN://$_";
            $done = $self->{CATALOGUE}->execute("addMirror", "$lfndir/$_", "$lfnlink", "LINK");
            if (!$done) {
              print "Fatal: Cannot add URL $newurl/$newse to $lfndir/$_\n";
              last;
            }
          }

        }
      }
    }
    if (!$done) {
      $alldone = "";
    }
  }

  return $alldone;
}

sub Clean {
  my $self = shift;
  print "Don't delete the working directory yet...\n";

  #    system( "/bin/rm -r " . $self->{WORK_DIRECTORY} );
}

sub Save {
  my $self = shift;

  $self->{CATALOGUE} = AliEn::UI::Catalogue::LCM->new();
  $self->{CATALOGUE} or print STDERR "Error connecting to the server\n" and return;
  my $procdir = "/proc/$ENV{ALIEN_PROC_ID}/";

  $self->{output} or $self->{output} = "";
  print "\n\nSaving the output ($self->{output})\n";
  $self->changeStatus("%", "SAVING");
  my $localdir = "";  #sprintf("%s/V%s/%05.5d/%05.5d",$self->{round},$ENV{ALIROOT_VERSION},$self->{run},$self->{event});
  my $lsfile;
  my $allregistered = 1;

  foreach $lsfile (split(",,", $self->{output})) {
    print STDOUT "Registering |$lsfile|\n";

    my @lsfiles = ();
    my $dolsfor = "";
    my $selist  = "";
    if ($lsfile =~ /(.*)\@(.*)/) {
      $dolsfor = $1;
      $selist  = $2;
    } else {
      $dolsfor = $lsfile;
      $selist  = "";
    }

    my $lsfor;
    my $pfnoptions = "";

    if ($dolsfor =~ /\?/) {
      $dolsfor =~ /(.*)\\\?(.*)/;

      if ($1 ne "") {
        $lsfor      = $1;
        $pfnoptions = $2;
      } else {
        $lsfor = $dolsfor;
      }
    } else {
      $lsfor = $dolsfor;
    }

    print STDOUT "Looking for local file  |$lsfor|\n";

    open OUTFILES, "ls -1 $lsfor |";
    while (<OUTFILES>) {
      my $chfile = $_;
      chomp $chfile;
      if ($chfile eq "") {
        next;
      }
      my $fset = {};
      $fset->{'name'} = $chfile;
      if ($pfnoptions ne "") {
        $chfile .= "?$pfnoptions";
      }
      $fset->{'extname'}    = $chfile;
      $fset->{'pfnoptions'} = $pfnoptions;

      print STDOUT "Adding $fset->{'name'} / $fset->{'extname'}\n";
      push @lsfiles, $fset;
    }
    close OUTFILES;
    foreach (@lsfiles) {
      if ($_->{'name'} eq "") {
        next;
      }
      my $file = "$self->{WORK_DIRECTORY}/$_->{'name'}";
      print STDOUT "Registering |$_->{'name'}|->|alien:$file| into $selist\n";
      $self->putJobLog("trace", "Registering file: |$_->{'name'}|->|alien:$file| into $selist");
      my $localdone = $self->Register($file, "$_->{'name'}", "$_->{'pfnoptions'}", $localdir, $selist)
        or print STDERR "Cannot register |$_->{'extname'}|->|$file|\n";
      if (!$localdone) {
        $self->putJobLog("error", "Registering file failed: |$_->{'name'}|->|alien:$file| into $selist");
        $allregistered = 0;
      }
    }
  }

  if (!$allregistered) {
    $self->changeStatus("%", "ERROR_SV");
    print "Not all files saved !!\n\n";
  } else {
    print "Output saved successfully!!\n\n";
  }

  $self->{CATALOGUE}->close();
  return 1;
}

sub GetWebPresentation {
}

sub GetWebArguments {
}

sub GetWebSubmitionCommand {
}

sub Validate {
  my $self = shift;
  $self->{QUEUEID} = shift;
  my $host = shift;
  my $port = shift;
  ($port)
    or print STDERR
    "Error: not enough arguments in AliRoot Validate!!\nUsage: Validate <queueId> <HostMonitor> <port>\n"
    and return;
  $self->{CM_ADDRESS} = "$host:$port";

  $self->{CATALOGUE} = AliEn::UI::Catalogue::LCM->new({"silent", "0", "debug", 0});
  ($self->{CATALOGUE}) or $self->ValidateError();

  $self->ParseOutput() or $self->ValidateFailed($self->{ERROR});

  $self->SaveResult() or $self->ValidateError($self->{ERROR});

  return 1;

}

sub ParseOutput {
  my $self = shift;

  my $dir = "$self->{CONFIG}->{SE_SAVEDIR}/proc/$self->{QUEUEID}";
  mkdir $dir, 0777;

  print "Parsing the output for errors...\n";
  print "\tbringing the file......";

  my @files = ("stdout", "stderr");
  my $file;
  foreach $file (@files) {
    if (!($self->{CATALOGUE}->execute("get", "/proc/$self->{QUEUEID}/$file", "$dir/$file"))) {

      my $message = "No $file of the job";
      my $action  = "resubmit $self->{QUEUEID}";
      $self->{CATALOGUE}->execute("ls", "/proc/$self->{QUEUEID}/$file")
        and $message = "Not possible to get the $file"
        and $action  = "validate $self->{QUEUEID}";

      $self->ValidateError($action, $message);
    }
  }

  print "ok\n";

  print "Opening the file....";
  open(FILE, "$dir/stdout")
    or print STDERR "Error opening the file $dir/stdout!!" and $self->ValidateError();

  my @file = <FILE>;
  close FILE;

  my $line = join "", @file;

  $self->{OUTPUT} = "";
  print "ok\nReading event, run, version and round....";

  my @all = ($line =~ /ALIEN-COMMENT:(.*)=(.*)/g);
  while (@all) {
    my ($key, $value) = (shift @all, shift @all);
    $self->{$key} = $value;
  }

  map { print "$_ => " . $self->{$_} . "\n" } (keys %{$self});

  if ($self->{ROUND}) {

    my $se = "Alice::CERN::scratch";
    my @where = $self->{CATALOGUE}->execute("whereis", "/proc/$self->{QUEUEID}/stdout");
    print "@where\n";
    if (!(grep (/$se/, @where))) {
      my $olddir = $dir;
      print "ok\nMoving the directory to $dir";
      $dir =~ s/\/[^\/]*$/\/validated\/$self->{ROUND}/;

      foreach (split("/", $dir)) {
        my $tdir .= "/$_";
        mkdir $tdir, 0777;
      }
      print "ok\nMoving the directory to $olddir to $dir\n";
      mkdir $dir, 0777;
      `mv $olddir $dir`;
      $dir = "$dir/$self->{QUEUEID}";
      my $se_address = "$self->{CONFIG}->{SE_HOST}:$self->{CONFIG}->{SE_PORT}";
      $self->{CATALOGUE}->execute(
        "update", "/proc/$self->{QUEUEID}/stdout",
        "-pfn", "soap://$se_address$dir/stdout?URI=SE",
        "-se", "$se"
      );
      $self->{CATALOGUE}->execute(
        "update", "/proc/$self->{QUEUEID}/stderr",
        "-pfn", "soap://$se_address$dir/stderr?URI=SE",
        "-se", "$se"
      );
    }
  }

  print "ok\nGetting the errors of the command";
  $self->{RUN}   = sprintf "%05d", $self->{RUN};
  $self->{EVENT} = sprintf "%05d", $self->{EVENT};

  $self->ValidateInitialize(@_) or return;

  print "ok\nParsing the output....";
  my @errors = ();

  my @possibleErrors = ();
  $self->{STDOUT_ERRORS}
    and @possibleErrors = @{$self->{STDOUT_ERRORS}};
  foreach my $error (@possibleErrors) {
    @errors = (@errors, grep(/$error/, @file));
  }

  if (@errors) {
    $self->{ERROR} = join("", @errors);
    print "\nPOSSIBLE errors=$self->{ERROR}\n";
    return;
  }

  print "ok\nParsing the stderr for errors...";

  open(FILE, "$dir/stderr")
    or print STDERR "Error opening the file $dir/stderr!!" and alien_error();
  @file = <FILE>;
  close FILE;

  @possibleErrors = ();
  $self->{STDERR_ERRORS}
    and @possibleErrors = @{$self->{STDERR_ERRORS}};
  foreach my $error (@possibleErrors) {
    @errors = (@errors, grep(/$error/, @file));
  }

  print "ok\nChecking if the output file is there...";

  foreach (split(",", $self->{OUTPUT})) {
    print "ok\n\t$_     ";
    ($self->{CATALOGUE}->execute("ls", "/proc/$self->{QUEUEID}/$_"))
      or print STDERR "Error file $_ is not there!!\n"
      and $self->{ERROR} = " file $_ is not in the catalogue"
      and return;
  }
  print "ok\nJob validate !!\n";

  return 1;
}

sub ValidateError {
  my $self = shift;

  my $suggestion = shift;
  my $reason     = shift;
  $reason .= "   job $self->{QUEUEID} at $self->{CM_ADDRESS}";
  print STDERR "Problems in alien validating  $self->{QUEUEID}\n";

  $self->ChangeValidationStatus("ERROR_V", "$suggestion\n# $reason\n");
  $self->{CATALOGUE}->close();

  exit 1;
}

sub ValidateFailed {
  my $self = shift;

  my $error = shift;

  print "Error: Job $self->{QUEUEID} not validated...\nUpdating the counter...";

  if ($self->{RUN} and $self->{ROUND}) {
    $self->ValidateUpdateDatabase("failed");
  }

  $self->ChangeValidationStatus("FAILED", "HOST:$self->{CM_ADDRESS}\n$error");
  $self->{CATALOGUE}->close();

  exit 1;
}

sub ValidateUpdateDatabase {
  my $self   = shift;
  my $update = shift;

  print "Increasing the number of $update events...";
  my $config   = AliEn::Config->new();
  my $database = new AliEn::Database(
    { "DB"     => "$config->{QUEUE_DATABASE}",
      "HOST"   => "$config->{QUEUE_DB_HOST}",
      "DRIVER" => "$config->{QUEUE_DRIVER}",
      "SILENT" => 1
    }
  );

  ($database) or $self->ValidateError();

  #($database->validate()) or $self->ValidateError();

  $database->update(
    "runs",
    {$update => "$update+1"},
    "run = ? and version = ?",
    {bind_values => [ $self->{RUN}, $self->{ROUND} ]}
  );

  $database->destroy;

  print "ok\n";

}

sub ChangeValidationStatus {
  my $self   = shift;
  my $status = shift;
  my $email  = shift;

  print "Contacting $self->{CM_ADDRESS} to update the status of $self->{QUEUEID} ($status)\n";
  my $done =
    SOAP::Lite->uri("AliEn/Service/ClusterMonitor")->proxy("http://$self->{CM_ADDRESS}")
    ->changeStatusCommand($self->{QUEUEID}, "DONE", $status);

  $done and $done->result;

  #    $self->SendEmail( $status, $email);
}

sub ValidateSucceeded {
  my $self = shift;

  my $data = shift;

  if ($self->{RUN} and $self->{ROUND}) {
    $self->ValidateUpdateDatabase("finishedevents");
  }

  $self->ChangeValidationStatus("VALIDATED", "");
  $self->{CATALOGUE}->close();

  exit 0;
}

sub SendEmail {
  my $self = shift;

  my $status = shift;
  my $body = (shift or "");

  #    return 1;
  my $user = "pablo.saiz\@cern.ch";
  print "Sending an email to $user...";
  my $ua = new LWP::UserAgent;
  $ua->agent("AgentName/0.1 " . $ua->agent);

  # Create a request
  my $req = HTTP::Request->new(POST => "mailto:$user");
  $req->header(
    Subject => "Job $self->{QUEUEID} at $self->{CM_ADDRESS} finished with $status (validation $ENV{ALIEN_PROC_ID})");
  $req->content("Job $self->{QUEUEID} finished with $status\n$body");

  # Pass request to the user agent and get a response back

  my $res = $ua->request($req);

  print "ok\n";

}

sub SaveResult {
  my $self = shift;

  #    my $data=shift;

  ($self->{EVENT}) or error();

  my $dir = "$self->{PATH}/$self->{EVENT}";

  print "PATH $dir\n";
  ($self->{CATALOGUE}->execute("mkdir", "$dir", "-p"))
    or print STDERR "Error creating the directory!!\n"
    and $self->ValidateError("mkdir -p $dir\nvalidate $self->{QUEUEID}", "Problems creating the dir");
  print "Directory done!\nCopy the file in the catalog...";

  #    $catalog->execute("debug", 5);

  my @list = split(",", $self->{OUTPUT});
  @list or $self->ValidateError("resubmit $self->{QUEUEID}", "There are no /proc/$self->{QUEUEID}/\%.root");
  @list = (@list, "stderr", "stdout");
  print "Files to Copy: @list\n";
  my $proc = "/proc/$self->{QUEUEID}";
  foreach (@list) {
    $self->{CATALOGUE}->execute("remove", "-s", "$dir/$_");
    ($self->{CATALOGUE}->execute("cp", "$proc/$_", "$dir/$_"))
      or $self->ValidateError("cp $proc/$_ $dir/$_\nvalidate $self->{QUEUEID}", "Problems copying the file");
  }

  $self->ValidateSucceeded();

}

return 1;

