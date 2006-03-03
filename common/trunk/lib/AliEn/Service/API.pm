=head1 NAME

AliEn::Service::API - Service to access the catalogue functions of AliEn

=head1 DESCRIPTION

The AliEn::Service::API module provides access to all AliEn catalogue functions by the SOAP
protocol.
The module contains except from neccessary conversions only wrapper functions to the catalogue

=cut


package API;

use AliEn::Catalogue;
use AliEn::UI::Catalogue;
use AliEn::UI::Catalogue::LCM;
use AliEn::LCM;
use AliEn::CE;
use strict;
use AliEn::Server::SOAP::Transport::HTTP;

#use AliEn::Service;
#use AliEn::Service::API;

#use vars qw(@ISA);

#@ISA=("AliEn::Service");

my $self;
my $options={};
my $daemon;
$options->{role} = "newuser";
$self->{CATALOG} = AliEn::Catalogue->new($options);
$options->{DATABASE} = $self->{CATALOG}->{DATABASE};
#$self->{LCM} = AliEn::LCM->new($options);
$self->{UI} = AliEn::UI::Catalogue::LCM->new($options);
$self->{CE} = AliEn::CE->new($options);
$self->{CATALOG}->{DEBUG} = 5;
$self->{CATALOG}->{DATABASE}->{DEBUG} = 5;
$self->{DATABASE}->{DEBUG} = 5;
$self->{UI}->{DEBUG} = 5;

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
&start();

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

  #$self->debug(1, "Service $self->{SERVICE} call for gSOAP $soapcall");
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
	my $option="";
	my $tags = $self->{CATALOG}->f_showTags($options, @_);

	my @result;
		# field size
	push @result, 0;
	push @result, split ("###", $tags);

	return \@result;
}

sub AlienAddAttribute {
	my $this = shift;
	my $result = $self->{CATALOG}->f_addTagValue(shift, shift, (shift) . "=" . (shift));
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

sub AlienSubmitJob {
	my $this = shift;
	my $result = $self->{CE}->submitCommand(@_);
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
	$daemon = AliEn::Server::SOAP::Transport::HTTP
    -> new({
#	LocalAddr => "pcna48fs1",
	LocalPort => 10010,
	Listen => 1,
	Prefork => 1}
	   )->dispatch_and_handle('API');
}

### end of debug stuff
