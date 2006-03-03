=head1 NAME

AliEn::Service::API - Service to access the catalogue functions of AliEn

=head1 DESCRIPTION

The AliEn::Service::API module provides access to all AliEn catalogue functions by the SOAP
protocol.
The module contains except from neccessary conversions only wrapper functions to the catalogue

=cut


package AliEn::API;

use AliEn::Catalogue;
use AliEn::Config;
use AliEn::UI::Catalogue;
use AliEn::UI::Catalogue::LCM;
use AliEn::LCM;
use AliEn::CE;
use strict;
use AliEn::Server::SOAP::Transport::HTTP;
use Socket;
use Carp;
use AliEn::ProofClient;


$SIG{INT} = \&catch_zap;    # best strategy
my $self  = {};

sub new {
   my $proto = shift;
   my $class = ref($proto) || $proto;
   bless( $self, $class );
   my $options={};

   $options->{role} = (shift or "");
   $self->{USER} = $options->{role};

   $self->{CATALOG} = AliEn::Catalogue->new($options);
   $options->{DATABASE} = $self->{CATALOG}->{DATABASE};
   #$self->{LCM} = AliEn::LCM->new($options);
   $self->{UI} = AliEn::UI::Catalogue::LCM->new($options);
   $self->{CE} = AliEn::CE->new($options);
   $self->{CONFIG} = new AliEn::Config();
   $self->{LOGDIR} = $self->{CONFIG}->{'LOG_DIR'};
   $self->{CATALOG}->{DEBUG} = 5;
   $self->{CATALOG}->{DATABASE}->{DEBUG} = 5;
   $self->{DATABASE}->{DEBUG} = 5;
   $self->{UI}->{DEBUG} = 5;
   $self->{ORGANISATION} = $self->{CONFIG}->{'ORG_NAME'};
   $self->{CATALOG} or return;
   $self->{UI} or return;
   $self->{CE} or return;
   return $self;
}

#print &AlienRemoveTag(undef, "/na48/user/p/peters/TestKit/TestKit", "TestKitTag");
#print &AlienCp(undef, "/na48/user/p/peters/TestKit/file1", "/na48/user/p/peters/TestKit/file2");
#print &AlienAddTag(undef, "/na48/user/p/peters/TestKit/TestKit", "TestKitTag");

#print &AlienGetFileURL(undef, "NA48::CERN::CTRL", "soap://pcna48fs1.cern.ch:8091/home/alien/SE/00001/00061.1060789247?URI=SE");

#AliEn::Service::testAPI::gSOAP();

#print &AlienDeleteFile(undef, "/na48/user/p/peters/TestKit/TestKit/TestKitFile2");
#print &AlienAddTag(undef, "/na48/user/p/peters/TestKit/Tag/", "TestKitTag");
#print @{&AlienGetAttributes(undef, "/na48/user/p/peters/TestKit/TestKit/RenamedFile", "TestKitTag")};
#print &AlienRemoveTag(undef, "/na48/user/p/peters/TestKit/Tag/", "TestKitTag");

#exit;
#&AlienGetJobStatus(undef, 1929);
#exit;

#print @{&AlienGetAttributes(undef, "/na48/user/p/peters/TestKit/TestKit/RenamedFile", "TestKitTag")};
#exit;
#print &AlienRegisterFile(undef, "http://www.cern.ch/index.html", "/na48/user/p/peters/TestKit/testfile");
#&start();

#print $self->{CATALOG}->f_complete_path("/na48/user/p/peters/test/"), "\n";
#print $self->{CATALOG}->f_complete_path("/na48/user/p/peters/test"), "\n";
#print $self->{CATALOG}->checkPermissions("r", "/na48/user/p/peters/test/file4");


sub gSOAP {
  my $self = shift;
  my $soapcall = shift ;
  my $args = shift;
  my @callargs  = split "###", $args;

  for (@callargs) {
    $_ =~ s/\\\#/\#/g;
  }

  #$self->{LOGGER}->debug("IS", "Service $self->{SERVICE} call for gSOAP $soapcall");
  if (! defined $soapcall) {
    SOAP::Data->name("result" => "----");
  } else {
    my $resultref = eval('$self->' . $soapcall . '(@callargs)');
    my @results;
    if (ref($resultref) eq "HASH") {
      @results = %$resultref;
    } elsif (ref($resultref) eq "ARRAY") {
      @results = @$resultref;
    } elsif (ref($resultref) eq "SCALAR") {
      @results = $$resultref;
    } else {
      @results = $resultref;
    }

#    print "Results @results\n";
    for (@results) {
      $_ =~ s/\#/\\\#/g;
    }

    my $soapreturn = join "###", @results;

	print "|", $soapreturn, "|\n";

    SOAP::Data->name("result" => "$soapreturn");
  }
}

sub AlienGetDir {
	my ($this, $dir, $options) = @_;
	print "GetDir $dir ...\n";
	$dir =~ s/\*/%/g;
	$dir =~ s/\?/_/g;

	$options .= "a";

	my @res = $self->{CATALOG}->f_lsInternal($options, $dir);
	shift @res;
	shift @res;
    my $rresult = shift @res;

    my @returnarr;
	if (!defined($rresult)) {
		push @returnarr, -1;
	}
	elsif ($#{$rresult} == -1) {
		push @returnarr, 0;
	}
	else {
		push @returnarr, 6;
		for (@$rresult) {
			push @returnarr, ($_->{type}, $_->{name}, $_->{owner}, $_->{ctime}, $_->{comment}, $_->{gowner}, $_->{size});
		}
	}
	return (\@returnarr);
}

sub AlienMkDir {
	my ($this, $dir, $option) = @_;
	my $result = $self->{CATALOG}->f_mkdir($option, $dir);
	return (defined($result)) ? "0" : "";
}

sub AlienRmDir {
	my ($this, $dir, $option) = @_;
	my $result = $self->{CATALOG}->f_rmdir($option, $dir);
	return (defined($result)) ? "0" : "";
}

sub AlienRm {
	my ($this, $file, $option) = @_;
	my $result = $self->{CATALOG}->f_removeFile($option, $file);
	return (defined($result)) ? "0" : "";
}

sub AlienCp {
	my ($this) = shift;
	my $result = $self->{CATALOG}->f_cp("", @_);
	return (defined($result)) ? "0" : "";
}

sub AlienMv {
	my ($this) = shift;
	my $result = $self->{CATALOG}->f_mv("", @_);
	return (defined($result)) ? "0" : "";
}

sub AlienAddFile {
	my $this = shift;
	my $result = $self->{CATALOG}->f_registerFile(@_);
	return (defined($result)) ? "0" : "";
}

sub AlienAddFileMirror {
	my $this = shift;
	my $result = $self->{CATALOG}->f_addMirror(@_);
	return (defined($result)) ? "0" : "";
}

sub AlienRegisterFile {
	my $this = shift;
	my $result = $self->{UI}->register(@_);
	return (defined($result)) ? "0" : "";
}

sub AlienGetPhysicalFileNames {
	my $this = shift;
	my @pfn = $self->{CATALOG}->f_getFile("s", @_);

	my @result;
		# field size
	push @result, 1;
	for (@pfn) {
		push @result, split ("###", $_);
	}

	return \@result;
}

sub AlienAddTag {
	my $this = shift;
	my $result = $self->{UI}->addTag(@_);
	return (defined($result)) ? "0" : "";
}

sub AlienRemoveTag {
	my $this = shift;
	my $result = $self->{CATALOG}->f_removeTag(@_);
	return (defined($result)) ? "0" : "";
}

sub AlienGetTags {
	my $this = shift;
	my $options ="";
	my $tags = $self->{CATALOG}->f_showTags("",@_);

	my @result;
		# field size
	push @result, 0;
	push @result, split ("###", $tags);

	return \@result;
}

sub AlienAddAttribute {
	my $this = shift;
	my $result = $self->{CATALOG}->f_updateTagValue(shift, shift, (shift) . "=" . (shift));
	return (defined($result)) ? "0" : "";
}

sub AlienAddAttributes {
        my $this = shift;
	my $file = shift;
	my $tag  = shift;
	my $newattribute;
	my @attributelist;

	while ($newattribute = shift) {
	  my $newattributevalue = shift;
	  if ($newattributevalue) {
	    push @attributelist,"$newattribute=$newattributevalue";
	  }
	}
	my $result = $self->{CATALOG}->f_updateTagValue($file,$tag,@attributelist);
	return (defined($result)) ? "0" : "";
}

sub AlienDeleteAttribute {
	my $this = shift;
	my $result = $self->{CATALOG}->f_removeTagValue(@_);
	return (defined($result)) ? "0" : "";
}

sub AlienGetAttributes {
	my $this = shift;
	print @_;
	my ($rfields, $rdata) = $self->{CATALOG}->f_showTagValue(@_);

    $rfields and $rdata and $rdata->[0] or return [0];

	my @result;
		# field size
	push @result, 1;
	foreach my $rfield (@$rfields) {
    	push @result, $rfield->{Field};
        push @result, $rdata->[0]->{$rfield->{Field}};
    }

    return \@result;
}

sub AlienChmod {
	my $this = shift;
	my $result = $self->{CATALOG}->f_chmod(@_);
	return (defined($result)) ? "0" : "";
}

sub AlienChown {
	my $this = shift;
	my $file = shift;
	my $user = shift;
	my $group = shift;
	my $data = $user;
	($group) and ($data .= "." . $group);
	my $result = $self->{CATALOG}->f_chown($data, $file);
	return (defined($result)) ? "0" : "";
}

sub AlienGetFile {
        my $this = shift;
        my $file = shift;
        my $localfile = shift;
        my $result = $self->{UI}->get("-s",$file,$localfile);
        return (defined($result)) ? \$result : "";
}

sub AlienSubmitJob {
	my $this = shift;
	my $lineargs = join " ",@_;
	my @args = split " ",$lineargs;
	my $result = $self->{CE}->submitCommand(@args);
	return (defined($result)) ? \$result : "";
}

sub AlienGetJobStatus {
	my $this = shift;
	my $id = shift;
	my @jobs = $self->{CE}->f_ps("-XA -id $id","-j");
	my @result;
	my @first = split("###", $jobs[0]);
	push @result, (scalar(@first) - 1);
	for (@jobs) {
		push @result, split("###", $_);
	}
	return \@result;
}

sub AlienKillJob {
	my $this = shift;
	my $result = $self->{CE}->f_kill(@_);
	return (defined($result)) ? "0" : "";
}

sub AlienResubmitJob {
	my $this = shift;
	my $result = $self->{CE}->resubmitCommand(@_);
	return (defined($result)) ? \$result : "";
}

sub AlienGetAioCert {
        my $this = shift;
        my $user         = $self->{USER};
	my $organisation = $self->{ORGANISATION};
	print "Getting AioCert for $user and $organisation\n";
	my @result = $self->{UI}->aiocert($user,$organisation);
	return ($#result > -1) ? \@result : "";
}

sub AlienGetAccessPath {
	my $this = shift;
	my $lfn = shift;
	my $mode = (shift == 1) ? "WRITE" : "READ";
	my $wishse = (shift or "NA48::CERN::CTRL"); #DEBUG FAKE
	my @result = $self->{UI}->accessRaw($mode, $lfn, $wishse);
	return ($#result > -1) ? \@result : "";
}

sub AlienGetFileURL {
	my $this = shift;
	print @_, "\n";
	my $result = $self->{UI}->mssurl(@_);
	return $result;
}

sub AlienFind {
	my $this = shift;
	my @result = $self->{CATALOG}->f_find(@_);
    unshift @result, 0;

	return \@result;
}

sub AlienFindEx {
	my $this = shift;

	my $rresult = $self->{CATALOG}->findEx(@_);
	$rresult 
		or return "";
		
	my @result;
		# field size before pfns
	push @result, 6;
	for (@$rresult) {
		push @result, $_->{lfn};
		
  		my @res = $self->{CATALOG}->f_lsInternal("", $_->{lfn});
		shift @res;
		shift @res;
	    my $rresult = shift @res;

		if (!defined($rresult) or $#{$rresult} == -1) {
			for (1..6) {
				push @result, '';
			}
		} else {
			my $data = $$rresult[0];
			push @result, ($data->{type}, $data->{owner}, $data->{ctime}, $data->{comment}, $data->{gowner}, $data->{size});
		}
			# number of pfns
		push @result, ($#{$_->{pfns}}+1);
		for (@{$_->{pfns}}) {
			push @result, $_->{pfn};
			push @result, $_->{se};
		}
	}
	
	\@result;
}

sub AlienFindExCount {
	my $this = shift;

	my $rresult = $self->{CATALOG}->findEx(@_);
	$rresult 
		or return "";
		
	my %pfnCount;
	my %mirrorCount;
	
	for (@$rresult) {
		my $first = 1;
		for (@{$_->{pfns}}) {
			if ($first) {
				$first = 0;
				$pfnCount{$_->{se}}++;
			} else {
				$mirrorCount{$_->{se}}++;
				$pfnCount{$_->{se}} or
					$pfnCount{$_->{se}} = 0;
			}
		}
	}
		
	my @result;
		# field size
	push @result, 2;
	
	for (keys %pfnCount) {
		push @result, $_;
		push @result, $pfnCount{$_} || 0;
		push @result, $mirrorCount{$_} || 0;
	}
	
	\@result;
}

sub AlienProofRequestSession() {
  my $this = shift;
  my @result;
  my $proof = new AliEn::ProofClient();
  $proof->init($self->{USER});
  
  my $site;
  my $ntimes;
  do {
    $site   = (shift or "");
    $ntimes = (shift or "");
    if ( ( $site ne "") && ($ntimes ne "") ) {
      $proof->addSite($site,$ntimes);
    }
  } while ($site ne "");
  
    
  my $call = $proof->CallService();
  $call or return;
  push @result, 9;
  push @result, $proof->{SESSIONID};
  push @result, $proof->{NMUX};
  push @result, $proof->{MUXHOST};
  push @result, $proof->{MUXPORT};
  push @result, $proof->{LOGINUSER};
  push @result, $proof->{LOGINPWD};
  push @result, $proof->{CONFIGFILE};
  push @result, $proof->{MASTERURL};
  push @result, $proof->{SITELIST};
  return \@result;
}

sub AlienProofStatusQuery() {
  my $this = shift;
  my $sessionId = shift or return;
  my @result;
  my $proof = new AliEn::ProofClient();
  $proof->init($self->{USER});
  my $call = $proof->QueryStatus($sessionId);
  $call or return;
  push @result, 9;
  push @result, $proof->{SESSIONID};
  push @result, $proof->{NMUX};
  push @result, $proof->{NREQUESTED};
  push @result, $proof->{NASSIGNED};
  push @result, $proof->{MUXHOST};
  push @result, $proof->{MUXPORT};
  push @result, $proof->{STATUS};
  push @result, $proof->{SCHEDULEDTIME};
  push @result, $proof->{VALIDITYTIME};
  push @result, $proof->{SESSIONUSER};
  return \@result;
}

sub AlienProofCancelSession() {
  my $this = shift;
  my $sessionId = shift or return;
  my $proof = new AliEn::ProofClient();
  $proof->init($self->{USER});
  my $call = $proof->CancelSession($sessionId);
  $call or return;

  my @result;
  push @result, 1;
  push @result, $proof->{SESSIONID};
  return \@result;
}


sub AlienProofListSessions() {
  my $this = shift;
  print "List Proof Sessions\n";
  my $proof = new AliEn::ProofClient();
  $proof->init($self->{USER});
  my $call = $proof->ListSessions();
  $call or return;
  print "Survied List Proof Sessions\n";
  my @result;
  my (@allsessions) = split "#LINEBREAK#",$proof->{SESSIONLIST};
  my $first = 1;

  foreach (@allsessions) {
    my (@values) = split '###', $_;
    if ($first) {
      my $nfields = $#values + 1;
      push @result, $nfields; # push the field size;
      $first = 0;
    }
    push @result, @values;
  }
  return \@result;
}

sub AlienProofListDaemons() {
  my $this = shift;
  print "List Proof daemons\n";
  my $proof = new AliEn::ProofClient();
  $proof->init($self->{USER});
  my $call = $proof->ListDaemons();
  $call or return;
  print "Survived List Proof daemons\n";
  my @result;
  my (@alldaemons) = split "#LINEBREAK#",$proof->{DAEMONLIST};
  my $first = 1;

  foreach (@alldaemons) {
    my (@values) = split '###', $_;
    if ($first) {
      my $nfields = $#values + 1;
      push @result, $nfields; # push the field size;
      $first = 0;
    }
    push @result, @values;
  }
  return \@result;
}

### now follows debug stuff


# -- SOAP::Lite -- guide.soaplite.com -- Copyright (C) 2001 Paul Kulchenko --



sub hi {
	return "bla!";
}

sub bye {
    return "goodbye, cruel world";
}

sub languages {
    return ("Perl", "C", "sh");
}


sub start {
    # find a free port in the high port range 15000 - 16000
    my $self = shift;
    my $checkport;
    my $portstart = 15000;
    my $portstop  = 16000;
    my $proto = getprotobyname('tcp');
    my $newport = 0;
    my $myhost = $ENV{'ALIEN_HOSTNAME'}.".".$ENV{'ALIEN_DOMAIN'};
    chomp $myhost;

    for $checkport ( $portstart .. $portstop) {
      if ( (socket(Server, PF_INET, SOCK_STREAM, $proto) && (setsockopt(Server, SOL_SOCKET, SO_REUSEADDR, pack("l",1)) ) && (bind(Server, sockaddr_in($checkport, INADDR_ANY))))) {
	# socket is free ...
	$newport = $checkport;
	last;
      }
    }
    
    print "\nPORT: $newport\n";

    # kill an old running API on this machine ...
    my $killpid;
    my $user;
    if ($self->{USER} eq "") {
      $user =$ENV{'USER'};
    } else {
      $user = $self->{USER};
    }

    open (PIDLOG,"$self->{LOGDIR}/API.$myhost.$user.pid");
    while (<PIDLOG>) {
      $killpid = $_;
      chomp $killpid;
    }
    
    my $allpids = `ps -eo \"pid ppid\" | grep $killpid | awk '{print \$1}'`;
    my @splitpids = split " ",$allpids;
    foreach (@splitpids) {
      kill 9, $_;
    }

    $self->{SERVERPID} = fork();
    my $spid = $self->{SERVERPID};
    if (-e "$self->{LOGDIR}/API.$myhost.$user.pid") {
	unlink "$self->{LOGDIR}/API.$myhost.$user.pid";
    }
    if (-e "$self->{LOGDIR}/API.$myhost.$user.port") {
	unlink "$self->{LOGDIR}/API.$myhost.$user.port";
	}

    if ($self->{SERVERPID}) {

      print "\nPID: $spid\n";
    } else {
      

      #log the pid and the port ....
      open (OUTPUT,"> $self->{LOGDIR}/API.$myhost.$user.pid");
      print OUTPUT "$$";
      close OUTPUT;

      open (OUTPUT,"> $self->{LOGDIR}/API.$myhost.$user.port");
      print OUTPUT "$newport";
      close OUTPUT;

#      system ("echo $$ > $self->{LOGDIR}/API.$myhost.$user.pid; echo $newport > $self->{LOGDIR}/API.$myhost.$user.port");
      
      $self->{DAEMON} = AliEn::Server::SOAP::Transport::HTTP
	-> new({
		LocalAddr => "$myhost",
		LocalPort => $newport,
		Listen => 1,
		Prefork => 5}
	      )->dispatch_and_handle('AliEn::API');
      close(STDOUT);
      close(STDERR);
    }
}

return 1;
### end of debug stuff
