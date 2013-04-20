package AliEn::CE;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use DBI;
use strict;
use POSIX;
use AliEn::UI::Catalogue::LCM;
use AliEn::Database::TaskQueue;


use AliEn::Database::CE;
use AliEn::LQ;
use AliEn::RPC;
use AliEn::Util;
use Tie::CPHash;

use AliEn::Service::JobAgent::Local;
use AliEn::Classad::Host;
use AliEn::X509;
use Data::Dumper;

use Switch;

use vars qw (@ISA $DEBUG);
push @ISA, 'AliEn::Logger::LogObject';
$SIG{INT} = \&catch_zap;    # best strategy

$DEBUG = 0;

sub new {
	my $proto = shift;
	my $class = ref($proto) || $proto;
	my $self  = {};

	my $options = shift;

  

	#my $user=($options->{user} or getpwuid($<));

	#    my $user = "aliprod";
	bless($self, $class);
	$self->SUPER::new() or return;
  $self->info("CREATING A NEW CE");

	$self->{RPC} = new AliEn::RPC or $self->info("Error creating the RPC module") and return;

	$self->{PASSWD} = ($options->{passwd} or "");

	$self->{DEBUG} = ($options->{debug} or 0);
	($self->{DEBUG}) and $self->{LOGGER}->debugOn($self->{DEBUG});
	$self->{SILENT} = ($options->{silent} or 0);
	$DEBUG and $self->debug(1, "Creating a new RemoteQueue");
	$self->{CONFIG} = new AliEn::Config() or return;

	my @possible = ();
	$self->{CONFIG}->{CEs} and @possible = @{$self->{CONFIG}->{CEs}};
	$DEBUG and $self->debug(1, "Config $self->{CONFIG}->{SITE} (Possible queues @possible)");

	($self->{CONFIG})
		or $self->{LOGGER}->warning("CE", "Error: Initial configuration not found!!")
		and return;

	$self->{HOST} = $self->{CONFIG}->{HOST};

	$self->{QUEUEID} = "";
	$self->{COMMAND} = "";

	$self->{WORKINGPGROUP} = 0;

	$DEBUG and $self->debug(1, "Connecting to the file catalog...");

	$self->{CATALOG} = ($options->{CATALOG} or AliEn::UI::Catalogue::LCM->new($options));
	$self->{CATALOG} or return;

	my $queuename = "AliEn::LQ";
	($self->{CONFIG}->{CE})
		and $queuename .= "::$self->{CONFIG}->{CE_TYPE}";

	$DEBUG and $self->debug(1, "Batch sytem: $queuename");

	eval "require $queuename"
		or $self->{LOGGER}->error("CE", "Error requiring '$queuename': $@")
		and return;
	$options->{DEBUG} = $self->{DEBUG};
	$self->{BATCH}    = $queuename->new($options);

	$self->{BATCH} or $self->info("Error getting an instance of $queuename") and return;

	$self->{LOGGER}->notice("CE", "Starting remotequeue...");

	my $pOptions = {};

	$options->{PACKMAN} and $self->{PACKMAN} = $pOptions->{PACKMAN} = $options->{PACKMAN};
	my $ca = AliEn::Classad::Host->new($pOptions) or return;

	$self->info($ca->asJDL);
	$self->{X509} = new AliEn::X509         or return;
	$self->{DB}   = new AliEn::Database::CE or return;

	my $role = $self->{CATALOG}->{CATALOG}->{ROLE} || "";

	if ($role eq "admin") {
		my ($host, $driver, $db) =
			split("/", $self->{CONFIG}->{"JOB_DATABASE"});

		$self->{TASK_DB}
			or $self->{TASK_DB} = AliEn::Database::TaskQueue->new(
			{    PASSWD            => "$self->{PASSWD}",
				DB                => $db,
				HOST              => $host,
				DRIVER            => $driver,
				ROLE              => 'admin',
				SKIP_CHECK_TABLES => 1
			}
			);
		$self->{TASK_DB}
			or $self->{LOGGER}->error("CE", "In initialize creating TaskQueue instance failed")
			and return;
		$self->{TASK_DB}->setSiteQueueTable();

	}

	if ($options->{MONITOR}) {
		AliEn::Util::setupApMon($self);
		AliEn::Util::setupApMonService($self, "CE_$self->{CONFIG}->{CE_FULLNAME}");
	}

	return $self;
}

sub checkZipArchives {
	my $self   = shift;
	my $job_ca = shift;
	my ($ok, @list) = $job_ca->evaluateAttributeVectorString("InputZip");
	$ok or return 1;
	$self->info("There are some zip archives!! @list");

	my $pwd    = $self->{CATALOG}->{CATALOG}->{DISPPATH};
	my $change = 0;
	foreach my $f (@list) {
		$f =~ s{^LF:}{} and $change = 1;
		if ($f !~ m{^/}) {
			$f      = "$pwd/$f";
			$change = 1;
		}
	}

	if ($change) {
		$self->info("Updating the list to @list");
		$job_ca->insertAttributeString("InputZip", @list);
	}

	return 1;
}


#_____________________________________________________________________________
#sub defaultRequirements {
#
#  my $self=shift;
#  my $req = " (other.Type==\"machine\") ";
#  return $req;
##}

#_____________________________________________________________________________
sub checkRequirements {
	my $self   = shift;
	my $job_ca = shift;

	my $default = " (other.Type==\"machine\") ";

	foreach my $method ("Input", "Packages", "Memory") {
		$DEBUG and $self->debug(1, "Checking the requirements from $method");
		my $methodName = "requirementsFrom$method";
		my $sereq      = $self->$methodName($job_ca);
		(defined $sereq) or return;
		$default .= $sereq;
	}

	$self->checkInputDataCollections($job_ca) or return;
	$self->checkZipArchives($job_ca)          or return;
	$DEBUG and $self->debug(1, "Checking requirements of the job");
	my ($ok, $origreq) = $job_ca->evaluateExpression("Requirements");

	if ($ok) {
		$DEBUG and $self->debug(1, "Adding requirements from the job");
		$default .= " && $origreq";
		$job_ca->set_expression("OrigRequirements", $origreq)
			or $self->info("Error with the original requirements")
			and return;
	}

	$DEBUG and $self->debug(1, "Checking if the job is going to be splitted");
	my $split;
	($ok, $split) = $job_ca->evaluateAttributeString("Split");
	if ($ok) {
		if ($split =~ /^none$/i) {

		} elsif (
			grep (/^$split/,
				(     "xml", "se", "event", "directory", "parentdirectory", "entries", "se_advanced", "file", "\-1", "\-2", "\-5",
					"\-10", "\-15", "\-20", "\-50", "\-100", "ce"
					))
			) {
			$DEBUG and $self->debug(1, "Job is going to be splitted by $split");

			#$default ="other.SPLIT==1";
			} elsif ($split =~ /production:(.+)-(.+)/) {
			$self->info("Job is going to be splitted for production, running from $1 to $2");
			} else {
			$self->{LOGGER}->warning("CE", "I don't know how to split by '$split'");
			return;
		}
	}

	($ok, my $ttl) = $job_ca->evaluateExpression("TTL");
	if ($ttl) {
		$default .= " && (other.TTL>$ttl)";
	}

	($ok, my $price) = $job_ca->evaluateExpression("Price");
	if ($price) {
		$default .= " && (other.Price<=$price) ";
	} else {
		$default .= " && (other.Price<=0) ";
	}

	$DEBUG and $self->debug(1, "All the requirements '$default'");

	$job_ca->set_expression("Requirements", $default) and return 1;

	$self->{LOGGER}->warning("CE", "Error setting the requirements for the job: '$default'");
	return;
}

#_____________________________________________________________________________
sub checkType {

	my $self = shift;
	my $ca   = shift;

	$DEBUG and $self->debug(1, "Checking the type of the job");
	my ($ok, $type) = $ca->evaluateAttributeString("Type");
	if ($ok && lc($type) ne "job") {
		$self->{LOGGER}->info("CE", "JDL is not of type Job !\n");
		return;
	} elsif (!$ok) {
		$ca->insertAttributeString("Type", "Job");
	}

	$DEBUG and $self->debug(1, "Type ok");
	return 1;
}

sub checkInputDataCollections {
	my $self = shift;
	my $ca   = shift;

	my ($ok, @inputdata) = $ca->evaluateAttributeVectorString("InputDataCollection");
	$ok or return 1;
	my @newlist;
	my $modified = 0;
	my $pwd      = $self->{CATALOG}->{CATALOG}->{DISPPATH};
	foreach my $file (@inputdata) {
		$self->info("Checking the input collection $file");
		$file =~ s/^LF:// or $self->info("Wrong format with $file. It doesn't start with 'LF:'", 1) and return;

		if ($file =~ m{^/}) {
			push @newlist, "\"LF:$file\"";
		} else {
			$self->info("That was relative path. Prepending the current directory");
			$modified = 1;
			push @newlist, "\"LF:$pwd$file\"";
		}
	}
	if ($modified) {
		$self->info("Updating the inputdatacollection");
		$ca->set_expression("InputDataCollection", "{" . join(",", @newlist) . "}")
			or $self->info("Error updating the InputDataCollection", 1)
			and return;
	}
	return 1;
}

#_____________________________________________________________________________
# This in fact just expands the pattern matching. It doesn't check requirements
#
sub requirementsFromInput {
	my $self = shift;
	my $ca   = shift;

	# Look for InputData attribute

	my $ok;

	#If there is no input data, everything is fine
	($ca->lookupAttribute("InputData")) or return "";

	($ok, my @inputdata) = $ca->evaluateAttributeVectorString("InputData");

	$ok
		or $self->info("Attribute InputData is not a vector of string", 1)
		and return;

	my @inputdataset;
	my $findset = "";
	$#inputdataset = -1;

	($ok, @inputdataset) = $ca->evaluateAttributeVectorString("InputDataSet");

	if ($#inputdataset > -1) {
		$findset = "-i " . join(",", @inputdataset);
	}

	my @allreq          = ();
	my @allRequirements = ();
	$#allRequirements = -1;

	my $num = $#inputdata + 1;
	$self->info("There are $num input files");
	if ($num > 1000) {
		$self->info("The job is trying to access more than 1000 files... not submitting it", 1);
		return;
	}
	my @flatfiles;

	my $i        = 0;
	my $pwd      = $self->{CATALOG}->{CATALOG}->{DISPPATH};
	my $modified = 0;
	foreach my $origlfn (@inputdata) {
		my ($file, @options) = split(',', $origlfn);
		$file =~ s/\\//g;
		foreach my $option (@options) {
			$option =~ /^nodownload$/i and next;
			$self->info("Error: options $option not understood in the lfn $origlfn", 1);
			return;
		}
		$i++;
		($i % 100) or $self->info("Already checked $i files\n", undef, 0);
		$DEBUG and $self->debug(1, "Checking the file $file");
		if ($file =~ /^PF:/) {
			$self->{LOGGER}->error("CE", "No PF allowed !!! Go to your LF !");
			next;
		}
		($file =~ s/^LF://i);
#			or $self->{LOGGER}->error("CE", "Malformed InputData -> $file - File Ignored.")
#			and next;

		if ($file !~ m{^/}) {
			$modified = 1;
			$file     = "$pwd$file";
		}

		if ($file !~ /\*/) {
			push @flatfiles, join(",", "LF:$file", @options);
			next;
		}
		$modified = 1;
		$DEBUG and $self->debug(1, "'$file' is a pattern");
		my $name = "";
		my $dir;
		my @list;
		if ($file =~ /^([^\*]*)\*(.*)$/) { $dir = $1; $name = $2 }
		if ($name =~ /(.*)\[(\d*)\-(\d*)\]/) {
			$name = $1;
			my $start = $2;
			my $stop  = $3;
			$self->info("Doing: find -silent -l $stop $findset $dir $name");
			@list = $self->{CATALOG}->execute("find", "-silent", "-l $stop", "$findset", "$dir", "$name");
		} else {
			$name eq "" and $name = "*";
			@list = $self->{CATALOG}->execute("find", "-silent", $findset, $dir, $name);
		}
		@list or $self->info("Error: there are no files that match $file") and return;
		my $nfiles = $#list + 1;
		$self->info("OK: I found $nfiles files for you!");
		map { $_ = join(",", "\"LF:$_\"", @options) } @list;
		push @flatfiles, @list;
		$name =~ s/^.*\/([^\/\*]*)$/$1/;
	}
	if ($modified) {
		$self->info("Putting the inputdata to @flatfiles");
		$ca->set_expression("InputData", "{" . join(",", @flatfiles) . "}")
			or $self->info("Error updating the InputData", 1)
			and return;
	}
	return "";
}

sub checkInputFiles {
	my $self   = shift;
	my $job_ca = shift;

	$DEBUG and $self->debug(1, "Checking the input box");

	#Checking the input sandbox
	my ($ok, @input, @inputName);
	($ok, @input)     = $job_ca->evaluateAttributeVectorString("InputFile");
	($ok, @inputName) = $job_ca->evaluateAttributeVectorString("InputName");

	my $input;
	my $name;

	foreach $input (@input) {
		$DEBUG and $self->debug(1, "Checking input $input");
		$name = shift @inputName;
		$name or ($input =~ /([^\/]*)$/ and $name = $1);
		if ($input =~ s/^PF://) {
			$self->info("You have to specify an lfn (instead of PF:$input). Please, register the file ");
			return;

		} elsif ($input =~ s/^LF://) {
			$self->{CATALOG}->execute("ls", "-silent", "$input")
				or $self->info("Error InputFile $input does not exist in the catalogue", 1)
				and return;
		} else {
			$self->info("Error with InputFile $input (it's nor LF: neither PF:", 1);
			return;
		}
	}
	$DEBUG and $self->debug(1, "INPUTFILES ARE OK!!");
	return 1;
}

sub modifyJobCA {

	my $self   = shift;
	my $job_ca = shift;

	$DEBUG and $self->debug(1, "Getting the name of the executable");
	my ($ok, $command) = $job_ca->evaluateAttributeString("Executable");

	$DEBUG and $self->debug(1, "Getting the attributes");
	my $arg;
	($ok, $arg) = $job_ca->evaluateAttributeString("Arguments");

	$arg and $DEBUG and $self->debug(1, "Got arguments $arg");

	my $fullPath;

	if (!$command) {
		$self->info("Error: the executable is missing in the jdl", 1);
		$self->info("Usage:  submitCommand <command> [arguments] [--name <commandName>][--validate]");
		return;
	}
	my $homedir = $self->{CATALOG}->{CATALOG}->GetHomeDirectory();
	$homedir =~ s/\/$//;
	if ($command =~ /\//) {
		$DEBUG and $self->debug(1, "Checking if '$command' exists");
#		$self->{CATALOG}->execute("ls", "-silent", "$command")
#			and $fullPath = "$command";
#
#		my $org = "\L$self->{CONFIG}->{ORG_NAME}\E";
#		($command =~ m{^((/$org)|($homedir))?/bin/[^\/]*$})
#			or $fullPath = "";
		!($command =~ /\/$/ ) and $self->{CATALOG}->execute("ls", "-silent", "$command") and $fullPath=$command;
	} else {
		my @dirs = ($homedir, "/\L$self->{CONFIG}->{ORG_NAME}\E", "");
		foreach (@dirs) {
			$DEBUG and $self->debug(1, "Checking if '$command' is in $_");
			$self->{CATALOG}->execute("ls", "-silent", "$_/bin/$command")
				and $fullPath = "$_/bin/$command"
				and last;
		}
	}

	($fullPath)
		or $self->info(
"Error: command $command is not in an executable directory (/bin, /$self->{CONFIG}->{ORG_NAME}/bin, or $homedir/bin) or does not exist",
		1
		)
		and return;

	# Clear errors probably occured while searching for files
	$self->{LOGGER}->set_error_no();
	$self->{LOGGER}->set_error_msg();

	$self->info("Submitting job '$fullPath $arg'...");

	$job_ca->insertAttributeString("Executable", $fullPath);

	$self->checkInputFiles($job_ca) or return;

	$self->checkType($job_ca) or return;

	$self->checkTimeToLive($job_ca) or return;

	$self->checkPrice($job_ca) or return;

	$self->checkRequirements($job_ca) or return;
	
	$self->checkExpire($job_ca) or return;

	$job_ca->insertAttributeString("User", $self->{CATALOG}->{CATALOG}->{ROLE});

	# Just a safety belt to insure we've not destroyed everything
	if (!$job_ca->isOK()) {
		return;
	}
	$DEBUG and $self->debug(1, "Job is OK!!");

	return 1;
}

# MaxWaitingTime check
sub checkExpire {
	my $self   = shift;
	my $job_ca = shift;
	$DEBUG and $self->debug(1, "Checking the max waiting time of this job");

	my ($ok, $mwt) = $job_ca->evaluateAttributeString("MaxWaitingTime");
	if ($ok) {
		if ($mwt && ($mwt =~ /^\d+\s*[smh]?$/i) ) {
			$self->info("Max Waiting Time defined to $mwt");
		}
		else{
			$self->info("No Max Waiting Time [correctly] defined");
			return;
		}
	} 
	return 1;
}


sub checkPrice {
	my $self   = shift;
	my $job_ca = shift;

	$DEBUG and $self->debug(1, "Checking the price of this job");

	my ($ok, $price) = $job_ca->evaluateAttributeString("Price");

	if (!$price) {
		$self->info("There is no price defined for this job in the jdl. Putting the default '1.0' ");
		$job_ca->set_expression("Price", 1.0);
	} else {

		#check if the price value is floating point number with optional fractional parts
		$price =~ m/^\s*\d+[\.\d+]*\s*$/
			or (
			$self->info(
"The defined price $price is not valid. Price value  must be numeric. \n\tExample: Price=\"1\" or  Price=\"3.14\""
			)
			and return
			);

		# will round price to 2 digits precision
		my $p = sprintf("%.2f", $price);
		$job_ca->set_expression("Price", $p);

	}
	return 1;

}

sub checkTimeToLive {
	my $self   = shift;
	my $job_ca = shift;
	$DEBUG and $self->debug(1, "Checking the time to live of this job");

	my ($ok, $ttl) = $job_ca->evaluateAttributeString("TTL");
	my $realTTL = 0;
	if (!$ttl) {
		$self->info("There is no time to live (TTL) defined in the jdl... putting the default '6 hours'");
		$realTTL = 6 * 3600;
	}

	($ttl =~ s/\s*(\d+)\s*h(our(s)?)?//)    and $realTTL += $1 * 3600;
	($ttl =~ s/\s*(\d+)\s*m(inute(s)?)?//)  and $realTTL += $1 * 60;
	($ttl =~ s/\s*(\d+)\s*s(second(s)?)?//) and $realTTL += $1;
	$ttl =~ s/^\s*(\d+)\s*$// and $realTTL += $1;
	if ($ttl !~ /^\s*$/) {
		$self->info(
"Sorry, I don't understand '$ttl' of the time to live. The sintax that I can handle is:\n\t TTL = ' <number> [hours|minutes|seconds]'.\n\tExample: TTL = '5 hours 2 minutes 30 seconds"
		);
		return;
	}
	$job_ca->set_expression("TTL", $realTTL);
	return 1;
}

sub getJdl {
	my $self = shift;
	my $arg  = join " ", @_;

	($arg)
		or $self->{LOGGER}->error("CE",
"Error: Not enough arguments in submit.\n Usage: submit <jdl file in the catalogue>| < <local jdl file> | <<EOF  job decription EOF"
		)
		and return;

	my $content;
	if ($arg =~ /<<\s*EOF/) {

		shift =~ /EOF/ or shift;

		#READING FROM THE STDIN
		$self->info("Enter the input for the job (end with EOF)");
		$content = "";
		my $line = <>;
		while ($line ne "EOF\n") {
			$line !~ /^\#/ and $content .= $line;
			$line = <>;
		}
		$self->info("Thanks!!", undef, 0);
	} elsif ($arg =~ /</) {

		#READING FROM A LOCAL FILE
		my $filename;
		$arg =~ /<\s*(\S+)/ and $filename = $1;
		shift =~ /../ or shift;
		$filename or $self->error("CE", "Error: Filename not defined!!") and return;
		open FILE, "<$filename"
			or $self->{LOGGER}->error("ERROR opening local file $filename")
			and return;

		my @content = grep (!/^\#/, <FILE>);
		close FILE;

		$content = join "", @content;
	} else {

		#File in the catalogue
		$DEBUG and $self->debug(1, "READING A FILE FROM THE CATALOGUE");
		my $filename = shift;
		my ($file) = $self->{CATALOG}->execute("get", "-silent", "$filename");
		$file
			or $self->{LOGGER}->error("CE", "Error getting the file $filename from the catalogue")
			and return;
		$DEBUG and $self->debug(1, "File $file");
		open FILE, "<$file"
			or $self->{LOGGER}->error("CE", "ERROR opening local file $file")
			and return;
		my @content = grep (!/^\#/, <FILE>);
		close FILE;
		$content = join "", @content;
	}

	#Checking for patterns:
	my $template = $content;
	my $i        = 1;
	my $homedir  = $self->{CATALOG}->{CATALOG}->GetHomeDirectory();

	$content =~ s/\$HOME/$homedir/g;

	while ($content =~ /\$$i\D/) {
		my $data = shift;
		(defined $data)
			or $self->{LOGGER}->error("CE", "Error: jdl requires at least $i arguments\nTemplate :\n$template\n")
			and return;
		$DEBUG and $self->debug(1, "Using $data for \$$i");
		$content =~ s/\$$i/$data/g;
		$i++;
	}
	$content =~ /(\$\d)/
		and $self->{LOGGER}
		->warning("CE", "Warning! Argument $i was not in the template, but there is $1\nTemplate:\n$template");

	$content or $self->{LOGGER}->error("Error: no description for the job") and return;

	return $content;
}

sub submitCommand_HELP {
	return "submit: sends a JDL to be executed on the grid

Usage:

submit [-n] (<jdl in the catalogue> | < <local jdl file> | <<EOF job description  EOF) 


Options: 
   -n: Do not submit the job. Run it on the current machine
"
}

sub submitCommand {
	my $self    = shift;
	my @arg     = grep (!/-(z|n)/, @_);
	my $content = "";

	my $zoption = grep (/-z/, @_);
	my $noption = grep (/-n/, @_);
	my $user = $self->{CATALOG}->{CATALOG}->{ROLE};

	my @quotas = $self->{CATALOG}->{CATALOG}->checkFileQuota($user, 0);
	if (@quotas) {
		if (($quotas[2] and $quotas[2] >= 0.9) || ($quotas[3] and $quotas[3] >= 0.9)) {
			$self->info("WARNING!!!! Your file quotas are 90% full!!!");
		}
	}

	if ($arg[0] eq "==<") {

		# this is the submission via gShell and GCLIENT_EXTRA_ARG
		shift @arg;
		my @lines = split('\n', shift @arg);
		my @newlines = grep (!/^\#/, @lines);

		$content = (join "\n", @newlines);

		#Checking for patterns:
		my $template = $content;
		my $i        = 1;

		while ($content =~ /\$$i\D/) {
			my $data = shift @arg;
			(defined $data)
				or $self->{LOGGER}->error("CE", "Error: jdl requires at least $i arguments\nTemplate :\n$template\n")
				and return;
			$DEBUG and $self->debug(1, "Using $data for \$$i");
			$content =~ s/\$$i/$data/g;
			$i++;
		}
		$content =~ /(\$\d)/
			and $self->{LOGGER}
			->warning("CE", "Warning! Argument $i was not in the template, but there is $1\nTemplate:\n$template");
		$content or $self->{LOGGER}->error("CE", "Error: no description for the job") and return;
	} elsif ($arg[0] eq "=<") {
		shift @arg;
		$content = (join " ", @arg);
	} else {
		$content = $self->getJdl(@arg) or return;
	}

	$DEBUG and $self->debug(1, "Description : \n$content");

	my $job_ca = AlienClassad::AlienClassad->new("[\n$content\n]");

	my $dumphash;
	$dumphash->{jdl} = $content;

	my $dumper = new Data::Dumper([$dumphash]);

	if (!$job_ca->isOK()) {
		$self->{LOGGER}->error("CE", "=====================================================");
		$self->{LOGGER}->error("CE", $dumper->Dump());
		$self->{LOGGER}->error("CE", "=====================================================");
		$self->{LOGGER}->error("CE", "Incorrect JDL input\n $content");
		return;
	}
	my $jdl = $job_ca->asJDL;
	$DEBUG and $self->debug(1, "Modifying the job description");
	if (!$self->modifyJobCA($job_ca)) {

		#    print STDERR $dumper->Dump();
		#    print STDERR "Input job suspicious\n$jdl\n";
		return;
	}
	$DEBUG and $self->debug(1, "Job description" . $job_ca->asJDL());

	#  my @filesToDownload;
	#  if ($self->{INPUTBOX}) {
	#    my $l = $self->{INPUTBOX};

	#    my @list  = sort keys %$l;
	#    my @list2 = sort values %$l;
	#    $self->info("Input Box: {@list}");
	#    $DEBUG and $self->debug(1, "Input Box: {@list2}");
	#    foreach my $entry (@list) {
	#      my $entry2 = shift @list2;
	#      push @filesToDownload, "\"${entry}->$entry2\"";
	#    }
	#  }
	#  (@filesToDownload)
	#    and $job_ca->set_expression("InputDownload", "{" . join(",", @filesToDownload) . "}");

	if ($noption) {
		$self->info("Instead of running the job, let's execute it ourselves");

		#(@filesToDownload) and
		#$job_ca->set_expression("InputDownload", "{". join(",", @filesToDownload)."}");

		my $agent = AliEn::Service::JobAgent::Local->new({CA => $job_ca}) or return;

		$agent->CreateDirs({remove => 1})
			or $self->info("Error creating the directories for the execution of the job")
			and return;

		$agent->checkJobJDL()
			or $self->info("Error checking the jdl at the startup of the jobagent")
			and return;

		$self->info("Ready to run the agent!!!");
		my $d = $agent->executeCommand();
		$self->info(
"And the job was executed with $d. You can find the output of the job in $agent->{WORKDIR} (on your local machine)"
		);

		return 1;
	}

	my ($jobId) = $self->{RPC}->CallRPC("Manager/Job", 'enterCommand', "$user\@$self->{HOST}", $job_ca->asJDL());
	
	if (!$jobId) {
		$self->info("=====================================================
Cannot enter your job. The Job Manager did not reply 
=====================================================");
		return;
	}

	if (ref $jobId eq "ARRAY"){
    $self->info("We have an array back!");
    my (@info)=@$jobId;
    $self->info("The job manager didn't want to insert the job: $info[1]");
    return;
  }
 

	if ($self->{WORKINGPGROUP} != 0) {
		$self->f_pgroup("add", "$jobId");
		$self->info("Job $jobId added to process group $self->{WORKINGPGROUP}\n");
	}

	my ($okf, @files)    = $job_ca->evaluateAttributeVectorString("OutputFile");
	my ($oka, @archives) = $job_ca->evaluateAttributeVectorString("OutputArchive");

	(@files and scalar(@files) > 0)
		and $self->{LOGGER}->warning("CE",
		"ATTENTION. You just submitted a JDL containing the tag 'OutputFile'. The OutputFile and OutputArchive");

	(@archives and scalar(@archives) > 0)
		and $self->{LOGGER}->warning("CE",
		"ATTENTION. You just submitted a JDL containing the tag 'OutputArchive'. The OutputFile and OutputArchive");

	if ((@files and scalar(@files) > 0) or (@archives and scalar(@archives) > 0)) {
		$self->info(
"tags will be dropped in future versions of AliEn. For the moment the old tags work as usual, but\nplease update your JDLs in the near future to utilize the 'Output' tag:\n\nThe syntax of the actual entries is still the same, but now you can just mixup files and archives, as e.g.:\n           Output = { \"fileA,fileB,*.abc\" , \"myArchive:fileC,fileD,*.xyz\" } ;\n    Thanks a lot!\n",
			undef, 0
		);
	}
	$self->info("OK, all right!");

  $self->info("Command submitted (job $jobId)!!");
  #$self->info("Updating the jobQuotas as well!!");
  #$self->calculateJobQuota(1) or return;
  $self->info("Job ID is $jobId - $zoption");
  if ($zoption) {
    my @aresult;
    my $hashresult;
    $hashresult->{"jobId"} = $jobId;
    push @aresult, $hashresult;
    return @aresult;
  }

	return $jobId;
}

sub f_queueStatus {
	my $self = shift;
	$self->info("Checking the status of the local batch system");
	my $free = $self->getNumberFreeSlots();
	$self->info("There are $free places to run agents");
	return 1;
}

sub getNumberFreeSlots {
	my $self = shift;

	my $free_slots = $self->{BATCH}->getFreeSlots();
 
 
  my ($info)=$self->{RPC}->CallRPC("ClusterMonitor", "getNumberJobs", $self->{CONFIG}->{CE_FULLNAME}, $free_slots);
	$info or $self->info("Error asking the ClusterMonitor") and return;

  defined $info->{RUNNING} and defined $info->{SAVING} and defined $info->{ASSIGNED} and defined $info->{STARTED}
   or $self->info("Error receiving some data from Manager/Job !!!!!") and return;
	
  my ($max_queued, $max_running) =($info->{maxqueuedjobs}, $info->{maxjobs});
  my $currentload= $info->{RUNNING}+ $info->{SAVING}+ $info->{ASSIGNED}+$info->{STARTED};
  if ($currentload >=$max_running){
    $self->info("We are currently running more than we are supposed to ($currentload and $max_running)");
    return $max_running-$currentload;
    
  }
	$self->info("According to the manager, we can queue max $max_queued and manage max $max_running. Now, we are running $currentload");
  $max_running or return;

	my $queued = $self->{BATCH}->getNumberQueued();
	if ($queued) {
		$self->info("There are queued $queued job agents");
		if ($queued < 0) {
			$self->info("There was a problem getting the number of queued job agents");
			return;
		}
	}
	my $running = $self->{BATCH}->getNumberRunning();
	if (!defined $running) {
		$self->info("Error getting the number of running jobs");
		$running = $max_running;
	}
	$running eq "" and $running = 0;

	my $free = ($max_queued - $queued);

	(($max_running - $running) < $free) and $free = ($max_running - $running);
	
  (($max_running - $currentload) < $free ) and $free = ($max_running - $currentload);
	my $file = "$ENV{ALIEN_HOME}/alien_$self->{CONFIG}->{CE_NAME}_number.txt";

	if (-f $file) {
		$self->info("The file $file exists. Reading the limit from there");
		if (open(FILE, "<$file")) {
			my $number = join("", grep (!/^\s*#/, <FILE>));
			chomp $number;
			$self->info("The file says '$number'");
			($number - $running < $free) and $free = $number - $running;
			close FILE;
		} else {
			$self->info("Error opening $file");
		}
	}
	$self->info("Returning $free free slots, with " . ($running - $queued) . " running jobs");

	if ($self->{MONITOR}) {
		$self->{MONITOR}->sendParams(
			{    'jobAgents_queued'  => $queued,
				'jobAgents_running' => ($running - $queued),
				'jobAgents_slots', ($free < 0 ? 0 : $free)
			}
		);
		$self->{MONITOR}->sendBgMonitoring();
	}
	return $free;
}

sub offerAgent_HELP {
	return "request - offer agents to be executed in the CE
Usage:

request [-n]

Options: 
  -n: Do not start the agents. Just verify that there are jobs waiting to be executed
";
}

sub offerAgent {
	my $self = shift;
	my $opt  = {};
	@ARGV = @_;
	Getopt::Long::GetOptions($opt, "n")
		or $self->info("Error parsing the arguments to request" . $self->offerAgent_HELP())
		and return;
	@_ = @ARGV;
	my $silent = (shift or 0);

	my $mode = "info";
	$silent and $mode = "debug";

	$DEBUG and $self->debug(1, "Requesting a new command...");

	my $done;
	my $user = $self->{CATALOG}->{CATALOG}->{DATABASE}->{USER};

	my $free_slots = $self->getNumberFreeSlots();
	($free_slots and ($free_slots > 0))
		or $self->{LOGGER}->$mode("CE", "At the moment we are busy (we can't request new jobs)")
		and return;

	my $ca = AliEn::Classad::Host->new({PACKMAN => $self->{PACKMAN}}) or return;
	$ca->set_expression("LocalDiskSpace", 100000000);
	$ca = $self->{BATCH}->prepareForSubmission($ca)
		or $self->info("Error asking the CE to prepare for submission loop")
		and return;
	my $classad = $ca->asJDL;

	($done) = $self->{RPC}->CallRPC("Broker/Job", "offerAgent", $user, $self->{CONFIG}->{HOST}, $classad, $free_slots);

	$done or return;
	$done eq -2 and $self->info("Nothing to execute :-)") and return -2;
	$DEBUG and $self->debug(1, "Got back that we have to start  agents");
	my ($numAgents, $jdl)= @$done;

	if (!$numAgents ) {
		$self->info("Nothing to execute");
		$opt->{n} or return -2;
	}
	if ($opt->{n}) {
		my $total = 0;
		$self->info("We do not start the agents. This is just for info. We could start $numAgents");
		return 1;
	}

	$DEBUG and $self->debug(1, "Got back that we have to start $numAgents   agents");
	my $script = $self->createAgentStartup() or return;
	my $count=$numAgents;
  $self->info("Starting $count agent(s) for $jdl ");
	$classad = AlienClassad::AlienClassad->new($jdl);

  $self->SetEnvironmentForExecution($jdl);
	
	while ($count--) {
		$self->info("*********READY TO SUBMIT $script $ENV{ALIEN_CM_AS_LDAP_PROXY}");
		my $error = $self->{BATCH}->submit($classad, $script);
		if ($error) {
				$self->info("Error starting the job agent");
				last;
		} else {
			my $id = $self->{BATCH}->getBatchId();
			if ($id) {
			  $self->info("Inserting $id in the list of agents");
		 		$self->{DB}->insertJobAgent(
						{       batchId => $id,
							agentId => $ENV{ALIEN_JOBAGENT_ID}
						}
					);
				}
			}
	}

	#  unlink $script;

	$self->UnsetEnvironmentForExecution();

	$self->info("All the agents have been started");

	if ($self->{COUNTER} % 100) {
		$self->info("Submitted $self->{COUNTER}. Delete the old ones");
		system("rm -rf $self->{CONFIG}->{LOG_DIR}/AliEn.JobAgent.$$.*");
	}
	return 1;
}

sub createAgentStartup {
	my $self          = shift;
	my $returnContent = (shift || 0);

	#  my $proxy=$self->{X509}->checkProxy();
	my $hours = ($self->{CONFIG}->{CE_TTL} || 12 * 3600) / 3600;
	my $proxy = $self->{X509}->createProxy($hours, {silent => 1});

	my $proxyName = ($ENV{X509_USER_PROXY} || "/tmp/x509up_u$<");

	my $before      = "";
	my $after       = "";
	my $alienScript = "$ENV{ALIEN_ROOT}/bin/alien";

	if ($self->{CONFIG}->{CE_INSTALLMETHOD}) {
		$self->info("We are installing alien with $self->{CONFIG}->{CE_INSTALLMETHOD}");
		my $method = "installWith" . $self->{CONFIG}->{CE_INSTALLMETHOD};
		eval { ($alienScript, $before, $after) = $self->$method(); };
		if ($@) {
			$self->info("Error calling $method: $@");
			return;

		}
	}

	if ($proxy) {

		open(PROXY, "<$proxyName") or $self->{LOGGER}->error("CE", "Error opening $proxyName") and return;
		my @proxy = <PROXY>;
		close PROXY;
		my $jobProxy = "$self->{CONFIG}->{TMP_DIR}/proxy.\$\$.`date +\%s`";
		$self->{USER} or ($self->{USER}) = $self->{CATALOG}->execute("whoami");
		my $debugTag = $self->{DEBUG} ? "--debug $self->{DEBUG}" : "";
		$before .= "echo 'Using the proxy'
mkdir -p $self->{CONFIG}->{TMP_DIR}
export ALIEN_USER=$self->{USER}
file=$jobProxy
cat >\$file <<EOF\n" . join("", @proxy) . "
EOF
chmod 0400 \$file
export X509_USER_PROXY=\$file;
echo USING \$X509_USER_PROXY
$alienScript proxy-info";
		$after .= "

rm -rf \$file\n";

	}
	my $content = "$before
$alienScript RunAgent
$after";

	if (!$returnContent) {
		my $script = "$self->{CONFIG}->{TMP_DIR}/agent.startup.$$";

		if (!-d $self->{CONFIG}->{TMP_DIR}) {
			my $dir = "";
			foreach (split("/", $self->{CONFIG}->{TMP_DIR})) {
				$dir .= "/$_";
				mkdir $dir, 0777;
			}
		}

		open(FILE, ">$script") or $self->{LOGGER}->error("CE", "Error opening the file $script") and return;
		print FILE "#!/bin/bash\n$content";
		close FILE;
		chmod 0750, $script;
		$content = $script;
	}
	return $content;
}

sub torrentScript {
	my $self = shift;
	my $path = shift;
	my $ver = $self->{CONFIG}->{VERSION};
	# ver = v[0-9]+[._][0-9]+
	$ver =~ s/_/./;
	$ver =~ s/(\.[0-9]+)//;
	
	
	return "DIR=$path 
mkdir -p \$DIR
echo \"Ready to install alien in \$DIR\"
date
cd \$DIR
wget http://alien.cern.ch/alien-installer -O alien-auto-installer
chmod +x alien-auto-installer
./alien-auto-installer -type workernode -batch -torrent -install-dir \$DIR/alien -version $ver

echo \"Installation completed!!\"

"
}

sub installWithTorrent {
	my $self = shift;
	$self->info("The worker node will install with the torrent method!!!");

	return "./alien/bin/alien", $self->torrentScript("\`pwd\`/alien_installation.\$\$"), "rm -rf \$DIR";
}

sub installWithTorrentPerHost {
	my $self = shift;
	$self->info("The worker node will install with the torrent  method!!!");

	return "../alien/bin/alien", $self->torrentScript("$self->{CONFIG}->{WORK_DIR}/alien_torrent"), "";
}

sub checkQueueStatus() {
	my $self   = shift;
	my $silent = (shift or 0);
	my $mode   = "info";
	$silent and $mode = "debug";

	$DEBUG and $self->debug(1, "Checking my queue status ...");

	my $user = $self->{CATALOG}->{CATALOG}->{DATABASE}->{USER};

	my @queueids = $self->{BATCH}->getQueuedJobs();

	if (!@queueids) {
		$self->{LOGGER}->error("CE", "Could not retrieve Queue information!") and return;
	} else {
		foreach (@queueids) {
			$self->info("Found Job-Id $_ in the Queue!");
		}
	}

	my ($done) = $self->{RPC}->CallRPC("ClusterMonitor", "checkQueueStatus", $self->{CONFIG}->{CE_FULLNAME}, @queueids);
	$done or return;

	if ($done->result eq "0") {
		$self->info("There was a queue inconsistency!");
	} else {
		$self->info("The queue was consistent!");
	}

	$self->info("Executed checkQueueStatus!!");

	return 1;
}

sub SetEnvironmentForExecution {
	my $self = shift;

	my $org = $self->{CONFIG}->{ORG_NAME};
	$self->{COUNTER} or $self->{COUNTER} = 0;

	# This variable is set so that the LQ and the JobAgent know where the output
	# is
	$ENV{ALIEN_LOG}         = "AliEn.JobAgent.$$.$self->{COUNTER}";
	$ENV{ALIEN_JOBAGENT_ID} = "${$}_$self->{COUNTER}";
	$self->{COUNTER}++;

	$ENV{"ALIEN_${org}_CM_AS_LDAP_PROXY"} = $ENV{ALIEN_CM_AS_LDAP_PROXY} =
		"$self->{HOST}:$self->{CONFIG}->{CLUSTERMONITOR_PORT}";

	$ENV{ALIEN_SE_MSS}      = ($self->{CONFIG}->{SE_MSS}      or "");
	$ENV{ALIEN_SE_FULLNAME} = ($self->{CONFIG}->{SE_FULLNAME} or "");
	$ENV{ALIEN_SE_SAVEDIR}  = ($self->{CONFIG}->{SE_SAVEDIR}  or "");

	$ENV{ALIEN_SaveSE} = ($self->{CONFIG}->{SaveSE_FULLNAME} or "");

	$ENV{ALIEN_SaveSEs} = "";

	delete $ENV{IFS};
	$ENV{PATH} =~ s{$ENV{ALIEN_ROOT}/bin:+}{};

	($self->{CONFIG}->{SaveSEs_FULLNAME})
		and $ENV{ALIEN_SaveSEs} = join "###", @{$self->{CONFIG}->{SaveSEs_FULLNAME}};


	return 1;
}

sub UnsetEnvironmentForExecution {
	my $self = shift;

	$DEBUG and $self->debug(1, "Removing the environment variables");

	map { delete $ENV{$_} }
		("ALIEN_CM_AS_LDAP_PROXY", "ALIEN_SE_MSS", "ALIEN_SE_FULLNAME", "ALIEN_SE_SAVEDIR", "ALIEN_SaveSE",
		"ALIEN_SaveSEs");
	$ENV{PATH} = "$ENV{ALIEN_ROOT}/bin:$ENV{PATH}";

	return 1;
}


sub getTopFromDB {
  my $self = shift;
  
  #my $args =join (" ", @_);
  my $date = time;

  my $usage="\n\tUsage: top [-status <status>] [-user <user>] [-host <exechost>] [-command <commandName>] [-id <queueId>] [-split <origJobId>] [-all] [-all_status] [-site <siteName>] [-r]";

  $self->info( "Asking for top..." );

  my $where=" join QUEUE_USER using (userid) join QUEUE_COMMAND using (commandId) JOIN QUEUE_HOST s on (submithostid=s.hostId) WHERE 1=1";
  my $columns="queueId, statusId, command name, s.host submitHost, user ";
  my $all_status=0;
  my $error="";
  my $data;

  my @columns=(
	       {name=>"user", pattern=>"u(ser)?",
		start=>"user='",end=>"'"},
	       {name=>"host", pattern=>"h(ost)?",
		start=>"e.host='",end=>"'"},
	       {name=>"submithost", pattern=>"submit(host)?",
		start=>"s.host='",end=>"'"},
	       {name=>"id", pattern=>"i(d)?",
		start=>"queueid='",end=>"'"},
	       {name=>"split", pattern=>"s(plit)?",
		start=>"split='",end=>"'"},
	       {name=>"status", pattern=>"s(tatus)?",
		start=>"statusId='",end=>"'"},
	       {name=>"command", pattern=>"c(ommand)?",
		start=>"name='",end=>"'"}
	      );

  while (@_) {
    my $argv=shift;
    $self->info("Doing $argv");
    ($argv=~ /^-?-all_status=?/) and $all_status=1 and  next;
    ($argv=~ /^-?-a(ll)?=?/) and $columns.=", e.host execHost, received, started, finished, split, resubmission" and $where= " left join QUEUE_HOST e on (e.hostid=exechostid) $where"
      and next;
    # Added for -r parameter
    ($argv=~ /^-?-r/) and !($argv=~ /^-?-a(ll)?=?/) and $columns.=", resubmission" and next; 
     
    if ($argv=~ /^-?-site=?/){
       my $siteName=shift;
       my $siteid=$self->{TASK_DB}->findSiteId($siteName);
       $siteid or $self->info("Error: the site $siteName does not exist ") and return;
       $where .=  " and siteid=$siteid";
       next;
     
    }  
    my $found;
    foreach my $column (@columns){
      if ($argv=~ /^-?-$column->{pattern}$/ ){
	     $found=$column;
	     last;
      }
    } 
    $found or  $error="argument '$argv' not understood" and last;
    my $type=$found->{name};

    my $value=shift or $error="--$type requires a value" and last;
    $data->{$type} or $data->{$type}=[];
    
    # For QUEUESTATUS TABLE
    $type =~ /status/ and $value = AliEn::Util::statusForML($value);

    push @{$data->{$type}}, "$found->{start}$value$found->{end}";
  }
  if ($error) {
    my $message="Error in top: $error\n$usage";
    $self->info($message);
    return ;
  }

  foreach my $column (@columns){
    $data->{$column->{name}} or next;
    $where .= " and (".join (" or ", @{$data->{$column->{name}}} ).")";
  }
  $all_status or $data->{status} or $data->{id} or $where.=" and ( statusId in (10,5,21,6,1,7,11,17,18,19,12))";
  #$all_status or $data->{status} or $data->{id} or $where.=" and ( status='RUNNING' or status='WAITING' or status='OVER_WAITING' or status='ASSIGNED' or status='QUEUED' or status='INSERTING' or status='STARTED' or status='SAVING' or status='TO_STAGE' or status='STAGGING' or status='A_STAGED' or status='STAGING' or status='SAVED')";

  $where.=" ORDER by queueId";

  $self->debug(1,  "In getTop, doing query $columns, $where" );

  my $rresult = $self->{TASK_DB}->getFieldsFromQueueEx($columns, $where)
    or $self->info( "In getTop error getting data from database" )
      and return (-1, "error getting data from database");
  
  my @entries=@$rresult;
  $self->debug(1, "Top done with $#entries +1");

  return $rresult;
}


sub f_top {
	my $self = shift;
	my @args = @_;

	$DEBUG and $self->debug(1, "In RemoteQueue::top @_");

	my $result = $self->getTopFromDB( @_) or return;

	my @jobs = @$result;
	my $job;
	my $columns = "JobId\tStatus\t\tUser\t\tCommand name\t\t\t\t\tSubmithost";
	my $format  = "%6s\t%-8s\t%-12s\t%-40s\t%-20s";
	if (grep (/-?-a(ll)?$/, @_)) {
		$DEBUG and $self->debug(1, "Printing more information");
		$columns .= "\t\t\tExechost\t\t\tReceived\t\t\tStarted\t\t\t\tFinished\tMasterJob\t\t\tResubmission";
		$format  .= "\t%-20s\t\%s\t\%s\t\%s\t%6s\t%6s";
	}
	
	# Check -r param
	if ( grep (/-?-r$/, @_) && !grep (/-?-a(ll)?$/, @_) ) {
 		$DEBUG and $self->debug(1, "Printing resubmission information");
		$columns .= "\t\t\tResubmission";
		$format  .= "\t\%6s";
	}
	
	$self->info($columns."\n", undef, 0);

	foreach $job (@jobs) {
		$DEBUG and $self->debug(3, Dumper($job));
		my (@data) = (
			$job->{queueId},
			$job->{status},
			$job->{user},
			$job->{name},
			$job->{submitHost}   || "",
			$job->{execHost}     || "",
			$job->{received}     || "",
			$job->{started}      || "",
			$job->{finished}     || "",
			$job->{split}        || "",
			$job->{resubmission},			
		);

		$data[4] or $data[4] = "";

		# Change the time from int to string
		$data[6] and $data[6] = localtime $data[6];
		$data[7] and $data[7] = localtime $data[7];
		$data[8] and $data[8] = localtime $data[8];

		# If -r but no -all
		(grep(/-?-r$/, @_)) and !(grep(/-?-a(ll)?$/, @_)) and $data[5]=$data[10];

		my $string = sprintf "$format\n", @data;
		$self->info($string, undef, 0);
	}
	return @jobs;
}



sub f_queueinfo {
	my $self = shift;
	my $jdl="";
  grep (/^-jdl$/, @_) and $jdl="jdl,";
  @_=grep (!/^-jdl$/, @_);
  my $silent=0;
  grep (/^-silent$/, @_) and $silent=1;
  my $site = (shift or '%');
  my $sql="site,blocked, status, statustime,$jdl ". join(", ", @{AliEn::Util::JobStatus()});
  $self->info("Quering  $sql");
  my $done = $self->{TASK_DB}->getFieldsFromSiteQueueEx($sql,"where site like '$site' ORDER by site");
  ($done) or return;

  $silent or 
	 $self->f_queueprint($done, $site);
	return $done;
}

sub f_queueprint {
	my $self = shift;
	my $done = shift;
	my $site = (shift or "");
	my $k;

	my $sum = {};

	my $tmpString = sprintf("%-24s%-16s%-18s%-14s", "Site", "Blocked", "Status", "Statustime");

	foreach $k (@{AliEn::Util::JobStatus()}) {
		$k =~ s/^ERROR_(..?).*$/ER_$1/ or $k =~ s/^(...).*$/$1/;
		$tmpString .= sprintf("%-5s ", $k);
	}
	$self->info(
		"$tmpString
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n",
		undef, 0
	);

	foreach (@$done) {

		$tmpString =
			sprintf("%-24s%-16s%-18s%-14s", $_->{'site'}, ($_->{'blocked'} || ""), $_->{'status'}, $_->{'statustime'});

		foreach $k (@{AliEn::Util::JobStatus()}) {
			$_->{$k} or $_->{$k} = 0;
			$tmpString .= sprintf("%-5s ", $_->{$k});
			(defined $sum->{$k}) or $sum->{$k} = 0;

			($_->{$k}) and $sum->{$k} += int($_->{$k});
		}
		$_->{jdl} and $tmpString = sprintf("$_->{jdl}");
		$self->info("$tmpString\n", undef, 0);
	}
	$self->info(
"----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n",
		undef, 0
	);

	if ($site eq '%') {
		my $sumsite = "Sum of all Sites";
		my $empty   = "----";
		my $zero    = "0";
		$tmpString = sprintf("%-24s%-16s%-18s%-14s", $sumsite, $empty, $empty, $empty);

		foreach $k (@{AliEn::Util::JobStatus()}) {
			if (defined $sum->{$k}) {
				$tmpString .= sprintf("%-5s ", $sum->{$k});
			} else {
				$tmpString .= sprintf("%-5s ", $zero);
			}
		}
		$self->info(
			"$tmpString
----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n",
			undef, 0
		);
	}
	return $done;
}

sub f_priorityprint() {
	my $self = shift;
	my $done = shift;
	my $lkeys;
	my $firstentry = @$done[0];
	my $out        = "user";
	$self->info(
"\n----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n",
		undef, 0
	);
	my $tmpString = sprintf("%-16s", $out);

	foreach $lkeys (keys %$firstentry) {
		if ($lkeys eq "user") {
			next;
		}
		$tmpString .= sprintf("%-20s", $lkeys);
	}
	$self->info($tmpString . "\n", undef, 0);
	$self->info(
"\n==================================================================================================================================================================================\n",
		undef, 0
	);
	foreach (@$done) {
		$tmpString = sprintf("%-16s", $_->{"user"});
		foreach $lkeys (keys %$firstentry) {
			if ($lkeys eq "user") {
				next;
			}
			$tmpString .= sprintf("%-20s", $_->{$lkeys});
		}
		$self->info($tmpString, undef, 0);
	}
	$self->info(
"\n----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n",
		undef, 0
	);
}

sub f_spy_HELP {
	return "spy: check the output of a job while it is still running\nUsage:
\tspy <job id> <filename> [<options>]

If <filename> is 'workdir', spy will return the current directory on the worker node.

Possible options include:
\tgrep <pattern>: Instead of returning all the lines, the worker node will only return the lines that match the pattern.
  head <number>: Return the first <number> lines of the file
  tail <number>: Return the last <number> lines of the file
";
}

sub f_spy {
	my $self    = shift;
	my $queueId = shift or $self->{LOGGER}->error("CE", "You have to specify the job id, you want to spy on") and return;
	my $spyfile = shift
		or $self->{LOGGER}->error("CE",
"You have to specify a filename to spy on, or \n\t'workdir'\t to see the job working directory or\n\t'nodeinfo'\t to see information about the worker node"
		)
		and return;

	$queueId =~ /^[0-9]+$/ or $self->info("The id '$queueId' doesn't look like a job id...\n" . f_spy_HELP()) and return;
	my $options = {grep => []};
	while (@_) {
		my $option = shift;
		if ($option =~ /^grep$/) {
			my $pattern = shift or $self->info("Missing pattern") and return;
			push @{$options->{grep}}, "($pattern)";
		} elsif ($option =~ /^tail/) {
			$options->{tail} = shift or $self->info("Missing number of lines") and return;
		} elsif ($option =~ /^head/) {
			$options->{head} = shift or $self->info("Missing number of lines") and return;
		} else {
			$self->info("Unknown option: $option");
			return;
		}
	}

	if (@{$options->{grep}}) {
		$options->{grep} = join("|", @{$options->{grep}});
	} else {
		$options->{grep} = undef;
	}

	my ($done) = $self->{RPC}->CallRPC("Manager/Job", "spy", $queueId, $spyfile, $options);
	$done or $self->info("The job $queueId is no longer in the queue, or no spyurl available") and return;
		
	$self->info("We are supposed to contact the cluster at $done");

	$self->{RPC}->Connect("JobAgent_$done", "http://$done");
	
	my ($data) =$self->{RPC}->CallRPC("JobAgent_$done", 'getFile', $spyfile, $options);

	$self->info("Finished Contacting the jobagent at $done");

	if (!$data) {
		$self->info("Could not get file via RPC, trying to get it via LRMS");
		$data = $self->{BATCH}->getOutputFile($queueId, $spyfile);
		$data or $data = "";
	}

	$self->info("Got $data");

	$self->info($done, undef, 0);
	return 1;
}

sub f_jobsystem {
	my $self        = shift;
	my @jobtag      = @_;
	my $username    = $self->{CATALOG}->{CATALOG}->{ROLE};
	my $callcommand = "getSystem";

	# if we get a job id(s) as arguments, we call the getJobInfo routine
	if (@jobtag) {
		my $jobid;
		eval { $jobid = sprintf "%d", $jobtag[0]; };
		if ($jobid > 0) {
			$callcommand = "getJobInfo";
		}
	}

	$DEBUG and $self->debug(1, "Connecting to $self->{HOST}:$self->{CONFIG}->{CLUSTERMONITOR_PORT}");

	my $done;
	if (@jobtag) {
		($done) = $self->{RPC}->CallRPC("Manager/JobInfo", $callcommand, $username, @jobtag);
	} else {
		($done) = $self->{RPC}->CallRPC("Manager/JobInfo", $callcommand, $username);
	}
	($done) or my $error = "Error connecting to the Manager/Job !! \n The Manager/Job did not return anything for $callcommand";

	if ($error) {
		$self->info("The Manager/Job returned error $error");
		return (-1, $error);
	}

	if ($callcommand eq "getJobInfo") {
		$self->f_printjobinfo($done);
	}

	if ($callcommand eq "getSystem") {
		$self->f_printsystem($done);
	}
}

sub f_printjobinfo() {
	my $self   = shift;
	my $result = shift;
	
	$self->info("==========================================================================\n", undef, 0);
	foreach (keys %$result) {
		if (defined $result->{$_}) {
			$self->info(sprintf("  %12s :   %6s", $_,, $result->{$_}), undef, 0);
		}
	}
	$self->info("==========================================================================\n", undef, 0);
}

sub f_printsystem() {
	my $self   = shift;
	my $result = shift;

	my $user = sprintf("%10s", $self->{CATALOG}->{CATALOG}->{ROLE});

	$self->info(
		"==========================================================================
= AliEn Queue                   all          ${user}         [%%]
--------------------------------------------------------------------------", undef, 0
	);

	foreach (@{AliEn::Util::JobStatus()}) {
		my $status = lc($_);
		$self->info(
			sprintf(
				"  %12s         %12s      %14s      %6.02f",
				"\u$status",
				($result->{"n$status"}     || 0),
				($result->{"nuser$status"} || 0),
				($result->{"frac$status"}  || 0)
			),
			undef, 0
		);
	}
	$self->info(
		"\n==========================================================================
= Job Execution                 all          $user
-------------------------------------------------------------------", undef, 0
	);
	my @list = (
		[ 'Exec. Efficiency ' => "" ],
		[ 'Assign.    Ineff.' => 'assignin' ],
		[ 'Submission Ineff.', 'submissionin' ],
		[ 'Execution  Ineff.', 'executionin' ],
		[ 'Validation Ineff.', 'validationin' ],
		[ 'Expiration Ineff.' =>, 'expiredin' ]
	);
	foreach (@list) {
		my ($title, $var) = @{$_};
		my $total = "${var}efficiency";
		my $user  = "user${var}efficiency";
		$self->info(sprintf("  $title     %12.02f %%      %12.02f %%", $result->{$total}, $result->{$user}, undef, 0));
	}
	$self->info(
		"\n==========================================================================
= Present Resource Usage        all          $user
--------------------------------------------------------------------------", undef, 0
	);
	$self->info(sprintf(
		"  CPU [GHz]            %12.02f        %12.02f",
		$result->{'totcpu'} / 1000.0,
		$result->{'totusercpu'} / 1000.0,
		undef, 0
	));
	$self->info(sprintf(
		"  RSize [Mb]           %12.02f        %12.02f",
		$result->{'totrmem'} / 1000,
		$result->{'totuserrmem'} / 1000,
		undef, 0
	));
	$self->info(sprintf(
		"  VSize [Mb]           %12.02f        %12.02f",
		$result->{'totvmem'} / 1000,
		$result->{'totuservmem'} / 1000,
		undef, 0
	));
	$self->info(
		"==========================================================================
= Computing Resource Account    all          $user     \n
--------------------------------------------------------------------------", undef, 0
	);
	$self->info(sprintf(
		"  CPU Cost [GHz*sec]   %12.02f        %12.02f\n",
		$result->{'totcost'}, $result->{'totusercost'},
		undef, 0
	));
	$self->info(
		"==========================================================================
= Site Statistic
--------------------------------------------------------------------------", undef, 0
	);
	my (@allsites) = split '####', $result->{'sitestat'};

	foreach (@allsites) {
		my (@siteinfo) = split '#', $_;
		$self->info(
			sprintf(
				"  %-30s %-5s %-5s %-5s %-5s %-5s %-5s %-5s %-5s %-5s",
				$siteinfo[0], $siteinfo[1], $siteinfo[2], $siteinfo[3], $siteinfo[4],
				$siteinfo[5], $siteinfo[6], $siteinfo[7], $siteinfo[8], $siteinfo[9]
			),
			undef, 0
		);
	}
	$self->info("==========================================================================\n", undef, 0);

	return "Done Jobs $result->{'ndone'}";
}

sub f_system {
	my $self     = shift;
	my $username = $self->{CATALOG}->{CATALOG}->{ROLE};

	return $self->f_jobsystem();
}

sub f_ps_traceAgent {
  my $self = shift;
  return  $self->{RPC}->CallRPCAndDisplay(0, "Manager/JobInfo", "getTraceAgent",  @_);
}

sub f_ps_trace {
	my $self = shift;
	my $id   = shift;
	if (!$id) {
		$self->info("Usage: ps trace <jobid> [tags]");
		return;
	}

	my ($done) = $self->{RPC}->CallRPC("Manager/JobInfo", "getTrace", "trace", $id, @_);
	
	$done or $self->info("Error talking to the JobInfo ") and return;
	my @trace;
	
	my $cnt    = 0;
	my @jobs   = split "\n", $done;

	#  my @returnjobs;
	foreach (@jobs) {
		$cnt++;
		my $printout = $_;
		$DEBUG and $self->debug(1, "Let's print '$printout'");
		$printout =~ s/\%/\%\%/g;
		if ($printout =~ s/^(\d+)//) {
			$printout = localtime($1) . $printout;
		}
		my $string = sprintf("%03d $printout\n", $cnt);
		$self->info($string, undef, 0);
		my @elements = split " ", $_;
		my $hashcnt  = 0;
		my $newhash  = {};
		my $newtrace = "";
		foreach my $elem (@elements) {

			# fix time format
			if ($hashcnt == 0) {
				my $newelem = localtime($elem);
				chomp $newelem;
				$newtrace .= $newelem;
			} else {
				$newtrace .= $elem;
			}
			$newtrace .= " ";
			$newhash->{$hashcnt} = $elem;
			$hashcnt++;
		}

		$newhash->{trace} = $newtrace;
		push @trace, $newhash;
	}
	return \@trace;
}

=item getPs

Gets the list of jobs from the queue

Possible flags:

=cut 

sub getPsFromDB {
  my $self = shift;
  my $options= shift;
  

  my $status="statusId in (10,5,21,6,1,3,2,7,11)";
  #my $status="status='RUNNING' or status='WAITING' or status='OVER_WAITING' or status='ASSIGNED' or status='QUEUED' or status='INSERTING' or status='SPLIT' or status='SPLITTING' or status='STARTED' or status='SAVING'";
  my $site="";

  $self->info( "Asking for ps (@_)..." );

  my $user="";
  my @userStatus;
  my $allStatus=0;
  $options->{d} and push @userStatus, 15; #DONE
  $options->{f} and push @userStatus, -12,-1,-2,-3, -4, -5, -7, -8, -9,-14,-13,-15,-10,-11,-16,-17,-18;
  $options->{r} and push @userStatus, 10,11,5,21,6,4,1,3,7,2;
  
  $options->{A} and $allStatus=1;
  $options->{I} and push @userStatus, 9,8,24; #IDLE INTERACTIV FAULTY

  $options->{z} and push @userStatus,-15; #ZOMBIE
  
  if ($options->{status}){
    for my $s (@{$options->{status}}){
       push @userStatus, AliEn::Util::statusForML($s);
    }
  }
  @userStatus and $status="statusId in (".join(',', @userStatus).")" ;


  my @userSite;
  if ($options->{site}){
   for my $s (@{$options->{site}}){
      push @userSite, "site='$s'";
   }
  }
  @userSite and $site="and ( ". join (" or ", @userSite). ")";

  ($allStatus) and  $status = "1=1";
   
  
  my $where = "WHERE $status $site ";


  $options->{user} and $where.=" and user='$options->{user}'";
  if ($options->{id}){
   for my $id (@{$options->{id}}){
     $where .=" and ( p.queueid='$id' or q.split='$id')";  
   }
  }  
  
  
  if ($options->{s}) {
    $where .=" and (upper(origJdl) like '\%SPLIT\%' or q.split>0 ) ";
  } elsif (not $options->{S}) {
    $where .=" and ((q.split is NULL) or (q.split=0))";
  }
  $self->info( "In getPs getting data from database \n $where" );


  my $rresult = $self->{TASK_DB}->query("select q.queueId, st.status, command name, e.host execHost, 
     s.host submitHost, runtime, cpu, mem, cputime, rsize, vsize, ncpu, cpufamily, cpuspeed, p.cost, 
       maxrsize, maxvsize, site, n.host node, q.split, procinfotime,received,q.started,finished,user
      from QUEUE q join QUEUEPROC p using(queueid) join QUEUEJDL qj using (queueid) 
          join QUEUE_COMMAND using (commandId) left join QUEUE_HOST e on (exechostId=e.hostid)
           left join QUEUE_HOST s on (submithostid=s.hostid) join SITEQUEUES using (siteid)
           left join QUEUE_USER using (userid) left join QUEUE_HOST n on (nodeid=n.hostid)
           join QUEUE_STATUS st using  (statusid)
           $where ORDER by queueid")
    or $self->info( "In getPs error getting data from database" )
      and return;

  $DEBUG and $self->debug(1, "In getPs getting ps done" );

  my @jobs;
    
  for (@$rresult) {
    $DEBUG and $self->debug(1, "Found jobid $_->{queueId}");
	$_->{cost} or $_->{cost}=0;
	$_->{runtime} or $_->{runtime}=0;
    $_->{mem} or $_->{mem} = 0;
    $_->{cputime} or $_->{cputime} = 0;
    $_->{cpu} or $_->{cpu} = 0;
    $_->{rsize} or $_->{rsize} = 0;
    $_->{vsize} or $_->{vsize} = 0;
    $_->{ncpu} or $_->{ncpu} = 0;
    $_->{cpufamily} or $_->{cpufamily} = 0;
    $_->{cpuspeed} or $_->{cpuspeed} = 0;
    $_->{maxrsize} or $_->{maxrsize} = 0;
    $_->{maxvsize} or $_->{maxvsize} = 0;
    
    $_->{cost} = int ($_->{cost});
#    push @jobs, join ("###", $_->{queueId}, $_->{status}, $_->{name}, $_->{execHost}, $_->{submitHost},
#		      $_->{runtime}, $_->{cpu}, $_->{mem}, $_->{cputime}, $_->{rsize}, $_->{vsize},
#		      $_->{ncpu}, $_->{cpufamily}, $_->{cpuspeed}, $_->{cost}, $_->{maxrsize}, $_->{maxvsize}, $_->{site},$_->{node},1,$_->{split},$_->{procinfotime},$_->{received},$_->{started},$_->{finished});
     push @jobs, $_;
  }

  $self->info( "ps done with ".scalar(@jobs)." entries");

  return  \@jobs ;
}


sub f_ps_rc {
	my $self = shift;
	my $id   = shift;
	$id or $self->info("Error: missing the id of the process", 11) and return;
	$DEBUG and $self->debug(1, "Checking the return code of $id");
	  
  my $rc=$self->{TASK_DB}->queryValue("select error from QUEUE where queueId=$id");
  $self->info( "The return code of $id is $rc\n");
	return $rc;

}

sub f_ps_HELP {
	return "ps: Retrieves process information 
  Usage: ps [-adflrsxAIX] [-id <jobid>]

  Job Selection:
    -d  : all <done> jobs
    -f  : all <failed> jobs
    -a  : jobs of <all> users [default only my jobs]
    -r  : all not finished jobs
    -A  : all kind of job status
    -I  : all interactive daemon jobs
    -s  : add splitted jobs to the output
    -id=<id> : only job <id>

  Output Format:
    def : <user> <jobId> <status> <runtime> <jobname>
    -x  : <user> <jobId> <status> <cpu> <mem> <cost> <runtime> <jobname>
    -l  : <user> <jobId> <status> <maxrsize> <maxvsize> <cpu> <mem> <cost> <runtime> <jobname>
    -X  : <user> <jobId> <status> <maxrsize> <maxvsize> <cpu> <mem> <cost> <runtime> <ncpu> <cpufamily> <cpuspeed> <jobname>
    -T  : <user> <jobId> <status> <received> <started> <finished> <jobname>

  Job Status:
    R   : running
    W   : waiting for execution
    OW  : waiting until job quota is available
    A   : assigned to computing element - waiting for queueing
    Q   : queued at computing element
    ST  : started - processmonitor started
    I   : inserting - waiting for optimization
    RS  : running split job
    WS  : job is splitted at the moment
    IS  : inserting - waiting for splitting

Error Status:
    EX  : job expired (execution too long)
    K   : job killed by user
    EA  : error during assignment
    ES  : error during submission
    EE  : error during execution
    EV  : -

  Interactiv Job Status:
    Id : interactive job is idle
    Ia : interactive job is assigned (in use)

    -h : help text

  Examples:
    ps -XA             : show all jobs of me in extended format
    ps -XAs            : show all my jobs and the splitted subjobs in extended format
    ps -X              : show all active jobs of me in extended format
    ps -XAs -id 111038 : show the job 111038 and all it's splitted subjobs in extended format
    ps -rs             : show my running jobs and splitted subjobs";
}

sub f_ps_jdl {
  my $self=shift;
  my $id=shift;
 
  $id or $self->info("Usage: ps jdl <jobid> ") and return;
  $self->debug(1, "Asking for the jdl of $id");
 
  my $columns="ifnull(resultsjdl, origjdl) jdl";
  my $join="";
  my $method="queryValue"; 
  foreach my $o (@_) {
    $o=~ /-dir/ and $columns.=",path" and $method="queryRow";
    $o=~ /-status/ and $columns.=",statusId" and $method="queryRow" and $join="join QUEUE using (queueid)";
  }

  my $rc=$self->{TASK_DB}->$method("select $columns from QUEUEJDL $join where queueId=?", undef, {bind_values=>[$id]});
  $method =~ /queryRow/ and $rc->{statusId} and $rc->{status} = AliEn::Util::statusName($rc->{statusId});

  if (!grep (/-silent/, @_)) {
	my $message = "The jdl of $id is $rc";
	if (UNIVERSAL::isa($rc, "HASH")) {
		$message = "";
		foreach my $k (keys %$rc) {
			$message .= "The $k of $id is " . ($rc->{$k} || "not defined") . "\n";
		}
	}
	$self->info($message);
  }

  return $rc; 
}

sub f_ps {
	my $self    = shift;
	my @args    = @_;
	my $verbose = 1;
	my @outputarray;
    my $titles;	
	my $output;
	my $first=1;

  $self->info("At the beginning of ps, '@_'");
	my $subcommands = {
		trace => "f_ps_trace",
		traceAgent => "f_ps_traceAgent",
		rc    => "f_ps_rc",
		jdl   => "f_ps_jdl",
	};

	if ((defined $args[0]) && ($subcommands->{$args[0]})) {
		shift;
		my $method = $subcommands->{$args[0]};
		return $self->$method(@_);
	}

  my %options=();

  @ARGV = @_;
  
  Getopt::Long::Configure('bundling_override');
  
  my $done=Getopt::Long::GetOptions(\%options, "id=s\@", "user=s","site=s@", "status=s@",
                           "x", "q", "s", "S", 
                           "a", #all users
                           "d", "f", "r", "A", "z", # TYPE OF JOBS
                           "l", "j", "T", "W", "X" # FORMAT FLAGS)
                           );
  Getopt::Long::Configure("default");           
  $done or $self->info("Error parsing the arguments of ps\n" )
    and return 0;

  @_ = @ARGV;
  

	$options{'a'} or $options{'user'} or $options{'user'}=$self->{CATALOG}->{CATALOG}->{ROLE};
	
  $self->debug(1,Dumper(\%options));

	my $result; 
	($result) = $self->getPsFromDB(\%options) or return;
	my $error  = "";

	my @jobs = @$result;
	my $job;

	my $username;
	my $now = time;
	foreach $job (@jobs) {
	  foreach my $field  ('site', 'node','mem', 'cost','runtime' ){
          $job->{$field} or $job->{$field}='';
      }

		$job->{procinfotime} or $job->{procinfotime} = $now;


		my $indentor = "";
		my $exdentor = " ";
		if ($job->{split}) {
			$indentor = "-";
			$exdentor = "";
		}		
    	
    	my $titles;
    	    	
    	my $status=$job->{status};
		$status =~ s/RUNNING/R/;
		$status =~ s/SAVING/SV/;
		$status =~ s/OVER_WAITING/OW/;
		$status =~ s/WAITING/W/;
		$status =~ s/ASSIGNED/A/;
		$status =~ s/QUEUED/Q/;
		$status =~ s/STARTED/ST/;

		if ($job->{splitjob}) {
			$status =~ s/DONE/DS/;
			$status =~ s/INSERTING/IS/;
			$job->{site} = sprintf "-%-3s-subjobs--", $job->{splitjob};
		} else {
			$status =~ s/DONE/D/;
			$status =~ s/INSERTING/I/;
		}

		$status   =~ s/EXPIRED/EX/;
		$status   =~ s/KILLED/K/;
		$status   =~ s/ERROR_A/EA/;
		$status   =~ s/ERROR_S/ES/;
		$status   =~ s/ERROR_E/EE/;
		$status   =~ s/ERROR_IB/EIB/;
		$status   =~ s/ERROR_RE/ER/;
		$status   =~ s/ERROR_V/EV/;
		$status   =~ s/ERROR_VT/EVT/;
		$status   =~ s/ERROR_VN/EVN/;
		$status   =~ s/ERROR_SV/ESV/;
		$status   =~ s/ERROR_P/EP/;
		$status   =~ s/FAILED/FF/;
		$status   =~ s/IDLE/Id/;
		$status   =~ s/INTERACTIV/Ia/;
		$status   =~ s/SPLITTING/WS/;
		$status   =~ s/SPLIT/RS/;
		$status   =~ s/ZOMBIE/Z/;
		
		my $printCpu = $job->{cpu} || "-";
		if ($options{x}) {
			$titles = sprintf "%-10s %s%-9s%s %-8s    %-6s  %-5s %-6s  %-6s  %-10s", "User", $indentor, "queueId",
				$exdentor, "Status", $printCpu, "Mem", "Cost", "Runtime", "Command";
		    $output = sprintf "%-10s %s%-9s%s %-8s    %-6s  %-5s %-6s  %-6s  %-10s", $job->{user}, $indentor, $job->{queueId},
				$exdentor, $status, $printCpu, $job->{mem}, $job->{cost}, $job->{runtime}, $job->{name};
		} elsif ($options{l}) {
			$titles = sprintf "%-10s %s%-9s%s %-8s    %-8s %-8s %-6s  %-5s %-6s  %-6s  %-10s", "User", $indentor, "queueId",
				$exdentor, "Status", "Maxrsize", "Maxvsize", "CPU", "Mem", "Cost", "Runtime", "Command";
			$output = sprintf "%-10s %s%-9s%s %-8s    %-8s %-8s %-6s  %-5s %-6s  %-6s  %-10s", $job->{user}, $indentor, $job->{queueId},
				$exdentor, $status, $job->{maxrsize}, $job->{maxvsize}, $printCpu, $job->{mem}, $job->{cost}, $job->{runtime}, $job->{name};
		} elsif ($options{X}) {
			$titles = sprintf "%-10s %s%-9s%s %-26s %-8s    %-8s %-8s %-6s  %-5s  %-6s  %-8s %-1s %-2s %-4s %-10s", "User",
				$indentor, "queueId", $exdentor, "Site", "Status", "Maxrsize", "Maxvsize", "CPU", "Mem", "Cost", "Runtime", "NCPU",
				"Cpufamily", "Cpuspeed", "Command";
			$output = sprintf "%-10s %s%-9s%s %-26s %-8s    %-8s %-8s %-6s  %-5s  %-6s  %-8s %-1s %-2s %-4s %-10s", $job->{user},
				$indentor, $job->{queueId}, $exdentor, $job->{site}, $status, $job->{maxrsize}, $job->{maxvsize}, $printCpu, $job->{mem}, $job->{cost}, $job->{runtime}, $job->{ncpu},
				$job->{cpufamily}, $job->{cpuspeed}, $job->{name};
		} elsif ($options{W}) {
			$titles = sprintf "%s%-9s%s %-29s %-30s %-26s %-10s %-8s %-02s:%-02s:%-02s.%-02s", $indentor, "queueId", $exdentor,
				"Site", "Node", "execHost", "Command", "Status", "Procinfotime";
			$output = sprintf "%s%-9s%s %-29s %-30s %-26s %-10s %-8s %-02s:%-02s:%-02s.%-02s", $indentor, $job->{queueId}, $exdentor,
				$job->{site}, $job->{node}, $job->{execHost}, $job->{name}, $status, (gmtime($now - $job->{procinfotime}))[ 7, 2, 1, 0 ];
		} elsif ($options{T}) {
			my $rt = "....";
			my $st = "....";
			my $ft = "....";

			if ($job->{received}) {
				$rt = ctime($job->{received});
			}
			if ($job->{started}) {
				$st = ctime($job->{started});
			}
			if ($job->{finished}) {
				$ft = ctime($job->{finished});
			}
			chomp $rt;
			chomp $st;
			chomp $ft;

			$titles = sprintf "%-10s %s%-9s%s %-8s    %-24s  %-24s  %-24s  %-10s", "User", $indentor, "queueId", $exdentor, "Status", "rt", "st",
			"ft", "Command";
			$output = sprintf "%-10s %s%-9s%s %-8s    %-24s  %-24s  %-24s  %-10s", $job->{user}, $indentor, $job->{queueId}, $exdentor,
				$status, $rt, $st, $ft, $job->{name};

		} else {

			# no option given
			$titles = sprintf "%-10s %s%-9s%s %-8s  %-8s  %-10s", "User", $indentor, "queueId", $exdentor, "Status", "Command";
			$output = sprintf "%-10s %s%-9s%s %-8s  %-8s  %-10s", $job->{user}, $indentor, $job->{queueId}, $exdentor, $status, $job->{name};
		}
		push @outputarray, $output;
		$first and $self->info($titles) and $first=0;
		$verbose and $self->info($output);
	}

	if ($options{j}) {
		return @jobs;
	}
	return @outputarray;
}

sub f_kill {
	my $self = shift;

	(@_)
		or $self->{LOGGER}->warning("CE", "Error: No queueId specified in kill job!")
		and return;

	my $user = $self->{CATALOG}->{CATALOG}->{ROLE};
	foreach my $queueId (@_) {
		if (!$self->{TASK_DB}->killTask($queueId, $user)){
			$self->info("We can't kill the job $queueId");
			return;
		}
		$self->info("Process $queueId killed!!", undef, 0);
	}
	return 1;
}

sub pgroupmember {
	my $self    = shift;
	my $groupid = (shift or 0);
	my $user    = $self->{CATALOG}->{CATALOG}->{ROLE};
	return $self->{CATALOG}->execute("ls", "/proc/groups/$user/$groupid/", "-silent");
}

sub pgroups {
	my $self    = shift;
	my $groupid = (shift or 0);
	my $user    = $self->{CATALOG}->{CATALOG}->{ROLE};
	return $self->{CATALOG}->execute("ls", "/proc/groups/$user/", "-silent");
}

sub pgroupprocstatus {
	my $self = shift;
	my $pid  = (shift or 0);
	my @args = @_;
	return $self->f_ps("-q", "-A", "-a", "-id", "$pid", @args);
}

sub f_pgroup {
	my $self     = shift;
	my $command  = (shift or "");
	my $user     = $self->{CATALOG}->{CATALOG}->{ROLE};
	my $handeled = 0;

	if ($command =~ /^new/) {

		# new process group
		my @pgroups = $self->{CATALOG}->execute("ls", "-la", "/proc/groups/$user", "-silent");
		if (!@pgroups) {
			$self->{LOGGER}->error("CE",
"Error: You don't have process group support enabled.\nAsk the system administrator to create your /proc/groups/$user/ directory!"
			);
			return;
		} else {
			my $highestgroup = 0;
			if ($#pgroups == 1) {
				$self->info("Nothing, but a directory", undef, 0);

				# the new group index will become 1
			} else {

				# OK, let's see, which is the highest group index
				@pgroups = $self->{CATALOG}->execute("ls", "/proc/groups/$user", "-silent");
				foreach (@pgroups) {
					my $singlegroup = $_;
					if ($singlegroup =~ /\d+/) {
						if (($_) > $highestgroup) {
							$highestgroup = $_;
						}
					}
				}
			}

			my $newgroup = $highestgroup + 1;
			$self->{CATALOG}->execute("mkdir", "/proc/groups/$user/$newgroup");
			my @checkgroup = $self->{CATALOG}->execute("ls", "-la", "/proc/groups/$user/1", "-silent");
			if (!@checkgroup) {
				$self->{LOGGER}->error("CE", "Error: Cannot create new process group!");
				return;
			} else {
				$self->{WORKINGPGROUP} = $newgroup;
				$self->f_pgroup();
			}
		}
		$handeled = 1;
	}

	if ($command =~ /\d+/) {

		# set working group to existing process group
		my $groupid = $command;
		my @pgroups = $self->{CATALOG}->execute("ls", "-la", "/proc/groups/$user/$groupid/", "-silent");
		if (!@pgroups) {
			$self->{LOGGER}->error("CE", "Error: The group $groupid does not exist!");
			$self->f_pgroup();
			return;
		}
		$handeled = 1;
		$self->{WORKINGPGROUP} = $groupid;
		$self->f_pgroup();
		if (@_) {
			return $self->f_pgroup(@_);
		}
	}

	if ($command =~ /^add/) {

		# add process to group
		my $take1 = shift;
		my $take2 = shift;
		my $groupid;
		my $procid;

		if ((defined $take1) && (defined $take2)) {
			$groupid = $take1;
			$procid  = $take2;
		} else {
			if ((defined $take1) && (!defined $take2) && ($self->{WORKINGPGROUP} != 0)) {

				$groupid = $self->{WORKINGPGROUP};
				$procid  = $take1;
			} else {
				$self->{LOGGER}
					->error("CE", "Error: you have to pass correct arguments!\nUsage: pgroup add [process group] <process ID>");
				return;
			}
		}

		if ($groupid =~ /\d+/) {
			my @pgroups = $self->{CATALOG}->execute("ls", "-la", "/proc/groups/$user/$groupid/", "-silent");
			if (!@pgroups) {
				$self->{LOGGER}->error("CE", "Error: The group $groupid does not exist!");
				return;
			}
		} else {
			$self->{LOGGER}->error("CE", "Error: You have to give a valid process group ID!");
			return;
		}

		if ($procid =~ /\d+/) {
			my @pgroups = $self->{CATALOG}->execute("ls", "-la", "/proc/groups/$user/$groupid/$procid", "-silent");
			if (@pgroups) {
				$self->{LOGGER}->error("CE", "Error: The process $procid already existis in group $groupid!");
			} else {
				$self->{CATALOG}->execute("mkdir", "/proc/groups/$user/$groupid/$procid");
				@pgroups = $self->{CATALOG}->execute("ls", "-la", "/proc/groups/$user/$groupid/$procid", "-silent");
				if (!@pgroups) {
					$self->{LOGGER}->error("CE", "Error: Cannot create new process $procid in process group $groupid");
					return;
				}
			}
			$self->f_pgroup("dump");
		} else {
			$self->{LOGGER}->error("CE", "Error: You have to give a valid process ID!");
			return;
		}
		$handeled = 1;
	}

	if ($command =~ /^remove/) {

		# remove process from group
		my $procid = shift;
		if ($self->{WORKINGPGROUP} == 0) {
			$self->{LOGGER}->error("CE", "Error: No current working process group selected!");
			return;
		}

		my $groupid = $self->{WORKINGPGROUP};
		if ($procid =~ /\d*/) {

			my @pgroups = $self->{CATALOG}->execute("ls", "-la", "/proc/groups/$user/$groupid/$procid", "-silent");
			if (@pgroups) {

				# let's remove it
				$self->{CATALOG}->execute("rmdir", "-rf", "/proc/groups/$user/$groupid/$procid");
				@pgroups = $self->{CATALOG}->execute("ls", "-la", "/proc/groups/$user/$groupid/$procid", "-silent");
				if (@pgroups) {
					$self->{LOGGER}->error("CE", "Error: Could not remove the process $procid from group $groupid!");
					return;
				} else {
					$self->f_pgroup("ps");
				}
			} else {
				$self->{LOGGER}->error("CE", "Error: The process $procid does not exist in group $groupid!");
				return;
			}

		} else {
			$self->{LOGGER}->error("CE", "Error: You have to give a valid process ID!");
			return;
		}

		$handeled = 1;
	}

	if ($command =~ /^ls/) {

		# close existing process group
		my @allgroups = $self->pgroups();
		$self->info("Existing Groups: ", undef, 0);
		foreach (@allgroups) {
			$self->info("$_ ", undef, 0);
		}
		$self->info("", undef, 0);
		$handeled = 1;
	}

	if ($command =~ /^close/) {

		# close open working process group
		$self->{WORKINGPGROUP} = 0;
		$handeled = 1;
		$self->f_pgroup("");
	}

	if ($command =~ /^status/) {

		# print status for all group processes
		$handeled = 1;
	}

	if ($command =~ /^dump/) {

		# dumpall group processes
		my $pgroup;
		my @allgprocs;
		$pgroup    = $self->{WORKINGPGROUP};
		@allgprocs = $self->pgroupmember("$pgroup");
		my $procnt = 0;
		$self->info("===================================================================", undef, 0);
		$self->info("Process Group:          $pgroup",                                     undef, 0);
		$self->info("-------------------------------------------------------------------", undef, 0);
		foreach (@allgprocs) {
			$procnt++;
			$self->info(sprintf("  |-> PID %4d | \n", $_), undef, 0);
		}
		$self->info("-------------------------------------------------------------------", undef, 0);
		$handeled = 1;
	}

	if ($command =~ /^kill/) {

		# kill all processes in group
		my $pgroup    = $self->{WORKINGPGROUP};
		my @allgprocs = $self->pgroupmember("$pgroup");
		foreach (@allgprocs) {
			$self->f_kill($_);
		}
		$handeled = 1;
	}

	if ($command =~ /^ps/) {

		# dump all processes in group
		my $pgroup;
		my @allgprocs;
		$pgroup    = $self->{WORKINGPGROUP};
		@allgprocs = $self->pgroupmember("$pgroup");

		$self->info("===================================================================", undef, 0);
		$self->info("Process Group:          $pgroup",                                     undef, 0);
		$self->info("-------------------------------------------------------------------", undef, 0);
		my $procnt = 0;
		foreach (@allgprocs) {
			$procnt++;
			$self->info(sprintf("  |-> PID %4d |   %s\n", $_, $self->pgroupprocstatus("$_", @_)), undef, 0);
		}
		$self->info("-------------------------------------------------------------------", undef, 0);
		$handeled = 1;
	}

	if ($command eq "") {
		if ($self->{WORKINGPGROUP} != 0) {
			$self->info("Active Process Group: $self->{WORKINGPGROUP}", undef, 0);
		} else {
			$self->info("No active Process Group!", undef, 0);
		}
		$handeled = 1;
	}

	if (!$handeled) {
		$self->{LOGGER}->error("CE", "Error: Illegal Command $command");
	}
}

sub f_validate {
	my $self = shift;

	#    my $queueId = shift;

	(@_)
		or $self->{LOGGER}->warning("CE", "Error: No queueId specified in validate job!")
		and return;

	my $queueId;
	foreach $queueId (@_) {
		my ($done) = $self->{RPC}->CallRPC("Manager/Job", "validateProcess", $queueId) or return;
		$self->info("Job to validate  $queueId submitted!\n", undef, 0);
	}
	return 1;
}

sub f_queue_HELP {
	return "'queue': Displays information about all the batch queues defined in the system. Usage:\n
\tqueue info\t\t-\tshow queue information
\tqueue open <queue> \t-\topen a queue on a dedicated site
\tqueue lock <queue> \t-\tlock a queue on a dedicated site
\tqueue list\t\t-\tlist available sites
\tqueue add <queue> \t-\tadd an unknown site to the site queue
\tqueue remove <queue>\t-\tremoves a queue
\n";
}

sub f_queue {
	my $self    = shift;
	my $command = shift;

	my ($host, $driver, $db) =
		split("/", $self->{CONFIG}->{"JOB_DATABASE"});

	if (!defined $command) {
		$self->info("$self->f_queue_HELP()", undef, 0);
		return;
	}

	$self->{TASK_DB}
		or $self->info("In queue, we can't connect to the database directly")
		and return;

	$DEBUG and $self->debug(1, "Calling f_queue_$command");
	my @return;
	if ( ($self->{CATALOG}->{CATALOG}->{ROLE} !~ /^admin(ssl)?$/)
		&& ($command ne "list")
		&& ($command ne "info")) {
		$self->info("Error executing queue $command: you are not allowed to execute that!");
		return;
	}

	my $func = "f_queue_$command";
	eval { @return = $self->$func(@_); };
	if ($@) {

		#If the command is not defined, just print the error message
		if ($@ =~ /Can\'t locate object method \"$func\"/) {
			$self->info("queue doesn't understand '$command'", 111);

			#this is just to print the error message"
			return $self->f_queue();
		}
		$self->info("Error executing queue $command: $@");
		return;
	}
	return @return;
}

sub f_queue_info {
	my $self = shift;
	my $jdl = grep (/^-jdl$/, @_);
	@_ = grep (!/^-jdl$/, @_);
	my $site = (shift or '%');
	$jdl and $jdl = ",jdl";
	$jdl or $jdl = "";
	my $array =
		$self->{TASK_DB}
		->getFieldsFromSiteQueueEx("site,blocked, status, statustime$jdl, " . join(", ", @{AliEn::Util::JobStatus()}),
		" where site like '$site' ORDER by site");
	if ($array and @$array) {
		return $self->f_queueprint($array, $site);
	}
	return;
}


sub f_queue_ghost {
	my $self       = shift;
	my $subcommand = (shift or "list");

	if ($subcommand =~ /list/) {
		my $now = time;
		my $diff = (shift or "600");
		$self->info("----------------------------------------------------------------------------------------------------",
			undef, 0);
		my $array = $self->{TASK_DB}->getFieldsFromQueueEx(
			"queueId,SUBSTRING( submitHost, 1, POSITION('\@' in submitHost)-1 ) as user, procinfotime,node, site,statusId",
"where ( (statusId=-1 or statusId=-2 or statusId=-3 or statusId=-5 or statusId=-7 or statusId=-8 or statusId=-9 or statusId=-10",
"or statusId=-11 or statusId=-16 or statusId=-17 or statusId=-18 or statusId=-14 or statusId=-13 or statusId=-15 or statusId=4 or statusId=5) and (procinfotime not like 'NULL') and (procinfotime > 1) and ($now-procinfotime)<$diff) ORDER by site"
		);
#		my $array = $self->{TASK_DB}->getFieldsFromQueueEx(
#			"queueId,SUBSTRING( submitHost, 1, POSITION('\@' in submitHost)-1 ) as user, procinfotime,node, site,status",
#"where ( (status like 'ERROR_%' or status='KILLED' or status='FAILED' or status='ZOMBIE' or status='QUEUED' or status='WAITING') and (procinfotime not like 'NULL') and (procinfotime > 1) and ($now-procinfotime)<$diff) ORDER by site"
#		);
		my $cnt = 0;
		foreach (@$array) {
			$cnt++;
			$self->info(
				sprintf(
					" [%04d. ]      %10s %-24s %24s %12s %-12s %-10s\n",
					$cnt, $_->{'queueId'}, $_->{'site'}, $_->{'node'},
					$_->{'user'}, $_->{'statusId'}, ($now - $_->{'procinfotime'})
				),
				undef, 0
			);
		}

		$self->info("----------------------------------------------------------------------------------------------------",
			undef, 0);
		return;
	}

	if ($subcommand =~ /change/) {
		my $queueId = shift
			or $self->{LOGGER}->error("CE", "You have to specify a queueId for which you want to change the status!")
			and return;
		my $status = shift or $self->{LOGGER}->error("CE", "You have to specify the status you want to set!") and return;

		my $set = {};
		$set->{statusId} = $status;

		AliEn::Util::Confirm("Do you want to update $queueId to status $status?") or return;

		my $done = $self->{TASK_DB}->updateJob($queueId, $set);
		$done or $self->{LOGGER}->error("CE", "Could not change job $queueId to status $status!") and return;
		return;
	}
}

# This internal subroutine checks if a queue exists or not.
# By default, it will return true if the table exists.
# It can receive as a second argument a 0 if it has to check the opposite
sub f_queue_exists {
	my $self  = shift;
	my $queue = shift;

	#this is to check if the queue has to exist or not;
	my $expectedValue = shift;
	defined $expectedValue or $expectedValue = 1;
	my $command = (shift or "exists");
	$queue or $self->info("Not enough arguments in 'queue $command'\nUsage: \t queue $command <queue name>") and return;
	my $exists = $self->{TASK_DB}->getFieldsFromSiteQueueEx("site", "where site='$queue'");

	#If the queue does not exist, but i
	if (@$exists and !$expectedValue) {
		$self->info("Error: the queue $queue already exists!");
		return;
	}
	if (!@$exists and $expectedValue) {
		$self->info("Error: the queue $queue does not exist!");
		return;
	}
	return 1;
}

sub f_queue_remove {
	my $self  = shift;
	my $queue = shift;
	$self->f_queue_exists($queue, 1, "remove") or return;

	$DEBUG and $self->debug(1, "Let's try to remove the queue $queue");
	return $self->{TASK_DB}->deleteSiteQueue("site='$queue'");
}

sub f_queue_list {
	my $self  = shift;
	my $site  = (shift or '%');
	my $array = $self->{TASK_DB}->getFieldsFromSiteQueueEx(
"site,blocked,status,maxqueued,maxrunning,queueload,runload,(RUNNING + STARTED + INTERACTIV + SAVING) as ALLRUNNING",
		"where site like '$site' ORDER by blocked,status,site"
	);
	my $s1 = 0;
	my $s2 = 0;
	my $s3 = 0;
	my $s4 = 0;
	my $s5 = 0;
	my $s6 = 0;
	if (@$array) {
		$self->info("----------------------------------------------------------------------------------------------------\n",
			undef, 0);
		$self->info(
			sprintf(
				"%-32s %-12s %-20s %5s %5s %4s/%-4s",
				"site", "open", "status", "load", "runload", "run", "max"
			),
			undef, 0
		);
		foreach (@$array) {
			my $allrunning = ($_->{'ALLRUNNING'} || 0);
			my $maxrunning = ($_->{'maxrunning'} || 0);
			my $blocked    = ($_->{blocked}      || "undef");
			my $queueload  = ($_->{queueload}    || "undef");
			my $runload    = ($_->{runload}      || "undef");

			$self->info(
				sprintf(
					"%-32s %-12s %-20s %5s %5s %4s/%-4s",
					$_->{'site'}, $blocked,   $_->{'status'}, $queueload, $runload,
					$allrunning,    $maxrunning
				),
				undef, 0
			);
			$s3 += $allrunning;
			$s4 += $maxrunning;
		}
		$s4 and $s5 = sprintf "%3.02f", 100.0 * $s1 / $s4;
		$s4 and $s6 = sprintf "%3.02f", 100.0 * $s3 / $s4;
		my $empty   = "";
		my $sumsite = "All";
		$self->info("----------------------------------------------------------------------------------------------------\n",
			undef, 0);
		$self->info(
			sprintf("%-32s %-12s %-20s %5s %5s %4s/%-4s\n", $sumsite, $empty, $empty, $s5, $s6, $s3, $s4),
			undef, 0);
		$self->info("----------------------------------------------------------------------------------------------------\n",
			undef, 0);
	}

	return 1;
}

sub f_queue_update {
	my $self    = shift;
	my $command = shift;
	my $queue   = shift;

	$self->f_queue_exists($queue, 1, $command) or return;

	my $set = {};
	$set->{blocked} = "open";
	$command =~ /lock/ and $set->{blocked} = "locked";
	$self->info("=> going to $command the queue $queue ...");
	my $update = $self->{TASK_DB}->updateSiteQueue($set, "site='$queue'")
		or $self->{LOGGER}->error("CE", "Error opening the site $queue")
		and return;

	$self->f_queue("info", $queue);
	return 1;
}

sub f_queue_lock {
	my $self = shift;
	$self->f_queue_update("lock", @_);
}

sub f_queue_open {
	my $self = shift;
	$self->f_queue_update("add", @_);
}

sub f_queue_add {
	my $self  = shift;
	my $queue = shift;
	$self->f_queue_exists($queue, 0, "add") or return;

	#my $set = {site => $queue, blocked => "locked", status => "new", statustime => 0};

	my $insert = $self->{TASK_DB}->insertSiteQueue($queue) 
	or $self->info("Error adding the site $queue");
		
	return $insert;
}

sub f_queue_purge {
	my $self     = shift;
	my @killpids = ();
	my $topurge  = $self->{TASK_DB}->getFieldsFromQueueEx("queueId", "where statusId is null or statusId=0");
	foreach (@$topurge) {
		$self->info("Job $_->{queueId} has empty status field ... will be killed!\n", undef, 0);
		push @killpids, $_->{queueId};
	}
	$self->f_kill(@killpids);
	return;
}

sub f_queue_tokens {
	my $self = shift;
	my $subcommand = (shift or "list");
	if ($subcommand =~ /list/) {
		my $status = (shift or "%");
		
		my $where = "";
		$status ne "%" and $status=AliEn::Util::statusForML($status) and $where = "where statusId=$status";
		
		printf "Doing listing\n";
		my $tolist = $self->{TASK_DB}->getFieldsFromQueueEx("queueId", $where);

		foreach (@$tolist) {

			my $token = $self->{TASK_DB}->getFieldFromJobToken($_->{queueId}, "jobToken");
			printf "Job %04d Token %40s\n", $_->{queueId}, $token;
		}
	}
	return;
}

sub f_quit {
	my $self = shift;

	print("bye now!\n");
	exit;
}

#sub checkConnection {
#  my $self = shift;#
#
#  #Checking the CM
#  $DEBUG and $self->debug(1, "Checking the connection to the CM");#
#
#  if ($self->{SOAP}->checkService("ClusterMonitor")){
#    $self->{CONNECTION}="ClusterMonitor" ;
#    return 1;
#  }
#  $self->{CONNECTION}="Manager/Job" ;
#  return $self->{SOAP}->checkService("Manager/Job", "JOB_MANAGER", "-retry");
#}

############################################################
# Bank functions start
############################################################

sub checkBankConnection {
	my $self = shift;

	#Checking the CM
	$DEBUG and $self->debug(1, "Checking the connection to the CM");

	if ($self->{RPC}->checkService("ClusterMonitor")) {
		$self->{BANK_CONNECTION} = "ClusterMonitor";
		return 1;
	}
	$self->{BANK_CONNECTION} = "LBSG";
	return $self->{RPC}->checkService("LBSG", "LBSG", "-retry");
}

sub getBankHELP {
	return "gold - Executes AliEn bank commands
\t Usage:
\t\t gold command [options]
\t\t\t  Where 'command' is of the the commands of 'Gold Allocation Manager'\
\t\t\t  Please refer to \"Gold User's Guide\" for more details\n";
}

sub f_bank {
	my $self = shift;

	my $help = join("", @_);
	(($help eq "--help") or ($help eq "-h") or ($help eq "-help"))
		and $self->info($self->getBankHELP(), undef, 0)
		and return 1;

	($self->checkBankConnection()) or return;

	my ($done) = $self->{RPC}->CallRPC($self->{BANK_CONNECTION}, "bank", @_) or return;
	$done or (($self->info("Error: RPC call to $self->{BANK_CONNECTION} 'bank' failed\n", undef, 0)) and return);

	$self->info($done, undef, 0);
	return 1;
}

sub getUserBankAccount {
	my $self     = shift;
	my $username = shift;

	#connect to LDAP and search through users and roles
	my $config = AliEn::Config->new();

	my $ldap = Net::LDAP->new($config->{LDAPHOST}) or return;
	$ldap->bind();

	my $base = $config->{LDAPDN};
	my $entry;

	# perform search through all users' entries
	my $mesg = $ldap->search(
		base   => "ou=People,$base",
		filter => "(&(objectclass=pkiUser)(uid=$username))"
	);

	if (!$mesg->count) {

		# perform search through all roles' entries
		$mesg = $ldap->search(
			base   => "ou=Roles,$base",
			filter => "(&(objectClass=AliEnRole)(uid=$username))"
		);

		if (!$mesg->count) {

			# User not found in LDAP !!!
			return;
		}

		# found in roles
		$entry = $mesg->entry(0);
		return $entry->get_value('accountName');

	}

	#found in people
	$entry = $mesg->entry(0);
	return $entry->get_value('accountName');
}

sub getSiteBankAccount {
	my $self = shift;
	my $site = shift;

	#connect to LDAP and search through users and roles
	my $config = AliEn::Config->new();

	my $ldap = Net::LDAP->new($config->{LDAPHOST}) or return;
	$ldap->bind();

	my $base = $config->{LDAPDN};
	my $entry;

	my $mesg = $ldap->search(
		base   => "ou=Sites,$base",
		filter => "(&(objectclass=AliEnSite)(ou=$site))"
	);
	if (!$mesg->count) {

		#  Site not found in LDAP !!!
		return;
	}

	$entry = $mesg->entry(0);
	return $entry->get_value('accountName');
}
############################################################
# Bank functions end
############################################################

sub submitCommands {
	my $self = shift;
	$DEBUG and $self->debug(1, "In submitCommands @_");
	my $all_commands = shift;

	my @commands = split ";", $all_commands;

	my $command;

	foreach $command (@commands) {
		$DEBUG and $self->debug(1, "Submitting $command");
		my (@args) = split " ", $command;
		$self->submitCommand(@args);
	}
	$DEBUG and $self->debug(1, "Done  submitCommands!!");

	return 1;
}

sub resubmitCommand {
	my $self = shift;

	(@_)
		or print STDERR "Error: no queueId specified!\nUsage resubmitCommand <queueId>\n"
		and return;
	$DEBUG and $self->debug(1, "Resubmitting command @_");

	my $user = $self->{CATALOG}->{CATALOG}->{ROLE};

	my @result;
	foreach my $queueId (@_) {
		# Getting path and resubmission
		$self->info("Resubmitting $queueId");
		my $resubmission = $self->{TASK_DB}->getFieldFromQueue($queueId, "resubmission");

		my $path = $self->{TASK_DB}->queryValue("SELECT path from QUEUEJDL where queueid=?",
		                                        undef, {bind_values=>[$queueId]});

		my ($done, $error) = $self->resubmitCommandInternal($queueId, $user);
		$done!='-1' or  $self->info("Error resubmitting $queueId: $error")
			  and return ($done, $error);
		push @result, $done;
		$self->info("Process $queueId resubmitted!! (new jobid is " . $done . ")");
		
		if ($path) {
		  $self->info("Renaming previous saved files of the job in $path");
		  my @files = $self->{CATALOG}->execute("find", "-j", $queueId, $path, "*");
		
		# Renaming saved files so the process can save the new ones
		  foreach my $file (@files){
		  	 $file =~ /.resubmit\d+/ and $self->info("Ignoring file $file") and next;
		  	$self->info("Renaming file $file from job $queueId");
		  	$self->{CATALOG}->execute("mv", $file, $file.".resubmit".$resubmission);
		  }
		}
				
	}
  $self->info("The resubmission of '@_' finished correctly");
	return @result;
}

sub resubmitCommandInternal {
  my $self       = shift;
  my $queueId    = shift;
  my $user       = shift;
  my $masterId   = (shift or "");
  my $replacedId = (shift or "");

  $self->info("In Resubmit Command");
  ($user) and ($queueId)
    or $self->{LOGGER}->error("CE", "In resubmitCommand queueId not specified")
    and return (-1, "QueueId not specified");

  $self->info("Checking your job quota... For Resubmission");
  my ($ok, $jobUserId) = $self->{TASK_DB}->checkJobQuota($user, 1);
  ($ok > 0) or return (-1, $jobUserId);
  $self->info("OK. resubmit ($jobUserId)");

  my $userId=$self->{TASK_DB}->findUserId($user);
  ($userId == $jobUserId ) or ($user eq "admin")
    or $self->info("In resubmitCommand process $queueId does not belong to '$user'")
    and return (-1, "process $queueId does not belong to $user");

  $self->info("Removing the 'registeredoutput', 'registeredlog', 'joblogonclustermonitor', and 'successfullybookedpfns'  field");
  $self->{TASK_DB}->deleteJobToken($queueId)
   or $self->info("Error changing the token of $queueId") and return (-1, "Error changing the token of the job");
  $self->{TASK_DB}->resubmitJob($queueId)
    or $self->info("Error resubmitting the job $queueId") and return (-1, "Error resubmitting the job");
  $self->info("AND NOW PUTTING THE JOBLOG");
  $self->putJobLog($queueId, "state", "The job has been resubmited (back to WAITING)");
  
  return $queueId;
}

sub putJobLog {
  my $self = shift;

  $self->info(join(" ", "Putting in the log: ", @_));
  return $self->{TASK_DB}->insertJobMessage(@_);
}

sub DESTROY {
	my $self = shift;

	#    ( $self->{LOGGER} )
	#      and $DEBUG and $self->debug(1, "Destroying remotequeue" );
	$self->{TASK_DB}   and $self->{TASK_DB}->close();
	($self->{CATALOG}) and $self->{CATALOG}->close();
}

sub catch_zap {
	my $signame = shift;
	print STDERR "Somebody sent me a SIG$signame. Arhgggg......\n";
	die;
}

=item masterJob($queueId)

Displays information about a masterJob and all its subjobs

=cut

sub masterJob_HELP {
	return "masterJob: prints information about a job that has been split in several subjobs. Usage:
\tmasterJob <jobId> [-status <status>] [-site] [-printid] [-id <id>] [merge|kill|resubmit]

Options:
    -status <status>: display only the subjobs with that status
    -id <id>:   display only the subjobs with that id
    -site <id>: display only the subjobs on that site
    -printid:   print also the id of all the subjobs
    -printsite: split the number of jobs according to the execution site
    merge:      collect the output of all the subjobs that have already finished
    kill:       kill all the subjobs
    resubmit:   resubmit all the subjobs selected
    expunge:    delete completely the subjobs

You can combine kill and resubmit with '-status <status>' and '-id <id>'. For instance, if you do something like 'masterjob <jobId> -status ERROR_IB resubmit', all the subjobs with status ERROR_IB will be resubmitted
";
}

sub masterJob {
	my $self = shift;
	my $id   = shift;
	$id
		or $self->info("Not enough arguments in 'masterJob': missing queueId\n" . $self->masterJob_HELP())
		and return;
	$id =~ m{^\d+$} or $self->info(" $id doesn't look like a job id") and return;

	$self->info("Checking the masterjob of $id");
	my $user = $self->{CATALOG}->{CATALOG}->{ROLE};

	my $error;

	my $data = {command => "info"};

	while (@_) {
		my $argv = shift;
		my $type;
		$self->info("Checking $argv");
		($argv =~ /^-?-printid$/) and $data->{printId} = 1 and next;
		($argv =~ /^((kill)|(resubmit)|(expunge)|(merge)|(filebroker))$/)
			and $data->{command} = $1
			and next;
		($argv =~ /^-?-printsite?/i) and $data->{printsite} = 1 and next;
		if ($argv =~ /^-?-s(tatus)?$/i) {
			$type = "statusId";
			if ($_[0] =~ /^ERROR_ALL$/i) {
				shift;
				$data->{statusId} or $data->{statusId} = [];
				push @{$data->{statusId}}, "statusId=-12", "statusId=-4", "statusId=-10", "statusId=-3",
					"statusId=-13", "statusId=-9", "statusId=-1", "statusId=-11";
#				push @{$data->{status}}, "status='EXPIRED'", "status='ERROR_IB'", "status='ERROR_V'", "status='ERROR_E'",
#					"status='FAILED'", "status='ERROR_SV'", "status='ERROR_A'", "status='ERROR_VN'";
				next;
			}
		}
		($argv =~ /^-?-i(d)?$/i) and $type = "queueId";
		($argv =~ /^-?-site$/i)  and $type = "site";
		$type or $error = "argument '$argv' not understood" and last;

		my $value = shift or $error = "--$type requires a value" and last;
		$data->{$type} or $data->{$type} = [];

		$type eq "statusId" and $value = AliEn::Util::statusForML($value);
		push @{$data->{$type}}, "$type='$value'";

	}
	if ($error) {
		my $message = "Error in masterJob: $error\n";
		$self->{LOGGER}->error("JobManager", $message);
		return (-1, $message);
	}

	$data->{cond} = "where q.split=$id";
	$data->{statusId}  and $data->{cond} .= " and (" . join(" or ", @{$data->{statusId}}) . ")";
	$data->{queueId} and $data->{cond} .= " and (" . join(" or ", @{$data->{queueId}}) . ")";
	$data->{exechost} = "ifnull(substring(exechost,POSITION('\\\@' in exechost)+1),'')";
	if ($data->{site}) {
		map { s/^site='/$data->{exechost}='/ } @{$data->{site}};
		$data->{cond} .= " and (" . join(" or ", @{$data->{site}}) . ")";
	}

	if ($data->{command} eq 'info') {
		return $self->getMasterJobInfo($id, $data);
	} elsif ($data->{command} eq 'filebroker') {
		return $self->getMasterJobFileBroker($id, $data);
	}

	return $self->getMasterJob($user, $id, $data, @_);
}

sub getMasterJobFileBroker {
	my $self = shift;
	my $id   = shift;
	my $data = shift;

	$self->info("Checking the file broker for the masterjob $id");

	my $jdl = $self->{TASK_DB}->queryValue("select origjdl from QUEUEJDL where queueid=?", undef, {bind_values => [$id]});
	($jdl) or $self->info("Error getting the JDL of the job '$id'") and return;

	my $job_ca = AlienClassad::AlienClassad->new($jdl);
	$job_ca         or $self->info("Error creating the classad of the job from '$jdl'") and return;
	$job_ca->isOK() or $self->info("The syntax of the job '$jdl' is not correct")       and return;

	my ($ok, $split) = $job_ca->evaluateAttributeString("Split");
	($ok and ($split =~ /^se_advanced$/))
		or $self->info("The job $id does not use file brokering")
		and return;

	my $info = $self->masterJobInfoFileBroker($id, $data);
	$self->info(Dumper($info));

}

sub getMasterJobInfo {
	my $self  = shift;
	my $id    = shift;
	my $data  = shift;
	my $total = 0;

	my $group = "statusId";

	my $columns = "s.status,statusId, count(*) as count";
	if ($data->{printsite}){
	  $group .= ",site,siteid";
	  $columns.= ",site,siteid ";
	  $data->{cond} or $data->{cond}="";
	  $data->{cond}=" join SITEQUEUES using (siteid) $data->{cond}" 
	}
	
	my $query = "select $columns from QUEUE q join QUEUE_STATUS s using (statusid)  $data->{cond} group by $group";
	$self->debug(1, "Doing the query $query");
	my $info = $self->{TASK_DB}->query($query)
		or $self->info("In getMasterJobInfo error getting data from database")
		and return (-1, "error getting data from database");

	if ($data->{printId}) {
		$self->info("Getting the ids under each status");
		foreach my $job (@$info) {
			my $extra = "";
			$job->{siteid} and $extra = " and siteid='$job->{siteid}'";
			
			my $subId =	$self->{TASK_DB}->queryColumn("select queueId from QUEUE where split=$id and
			 statusId=$job->{statusId}$extra");

			$job->{ids} = $subId;
		}
	}
	my $jobInfo = $self->{TASK_DB}->queryRow("SELECT statusId,merging FROM QUEUE where queueid=$id");
	if (!$jobInfo) {
		$self->info("The job '$id' doesn't exist");
		return;
	}
	$self->info("The job $id is in status: ".AliEn::Util::statusName($jobInfo->{statusId})."\nIt has the following subjobs:");

	foreach my $subjob (@$info) {
		$subjob or next;
		my $ids = "";
		my $site = $subjob->{site} || "";
		$site and $site = " ($site)";
		($subjob->{ids})
			and $ids = "(ids: " . join(", ", @{$subjob->{ids}}) . ")";
		$self->info("\t\tSubjobs in ".AliEn::Util::statusName($subjob->{statusId})."$site: $subjob->{count} $ids\n", undef, 0);
		$total += $subjob->{count};
	}

	$self->info("In total, there are $total subjobs\n", undef, 0);

	if ($jobInfo->{merging}) {
		$self->info("The job is being merged. Let's see how is that going");
		my @ids = split(",", $jobInfo->{merging});
		map { $_ = "queueid=$_" } @ids;
		my $merging = $self->{TASK_DB}->query("SELECT statusId, queueId FROM QUEUE where " . join(" or ", @ids));
		foreach my $merge (@{$merging}) {
			$self->info("\n\tJob $merge->{queueId} : $merge->{statusId}", undef, 0);
		}

	}
	$data->{sites} = 1;
	$self->masterJobInfoFileBroker($id, $data);
	return $info;
}

=item C<getMasterJob>

Displaies information about a masterjob and all of its children

=cut

sub getMasterJob {
	my $self = shift;
	my $user = shift;
	my $id   = shift;
	my $data = shift;

	#	my $info = [];
	if ($data->{command} eq "expunge") {
		$self->info("Removing the jobs ");
		my $message = "Jobs expunged!!!";

		my $jobIds = $self->{TASK_DB}->queryColumn("SELECT queueId from QUEUE q $data->{cond}")
			or return (-1, "Error getting the subjobs with $data->{cond}");
		foreach my $subjob (@$jobIds) {
			my $message = "Removing job $subjob";
			$self->{TASK_DB}->delete("QUEUE", "queueId=$subjob");
		}
		$self->info($message, 0);

		$self->putJobLog($id, "state", "Removed all the subjobs ($data->{cond}): $message");

	} elsif ($data->{command} eq "merge") {

		$self->info("Merging the jobs that have finnished");
		my $subjobs = $self->{TASK_DB}->query("SELECT queueId,submitHost FROM QUEUE q $data->{cond} and statusId=15") #DONE
			or return (-1, "Error getting the subjobs of $id");
		if (!@$subjobs) {
			$self->info("The job $id doesn't have any subjobs that have finished");
			return (-1, "there are no finished subjobs of job $id");
		}
		$subjobs->[0]->{submitHost} =~ /^$user\@/
			or return (-1, "the job doesn't belong to $user");
#		$self->changeStatusCommand($id, 'token', "%", 'FORCEMERGE')
        $self->{RPC}->CallRPC("Manager/Job", "changeStatusCommand", $id, 'token', "%", 'FORCEMERGE')
			or return (-1, "error putting the status to 'FORCEMERGE'");
		$self->{TASK_DB}->update("ACTIONS", {todo => 1}, "action='MERGING'");

		$self->info("Forcing the merge of the output (this can take some time)", 0);
	} else {
		my $commands = {
			kill     => {sub => "f_kill"}, #killProcess
			resubmit => {sub => "resubmitCommand", extra => [$id]}
		};
		my $subroutine = $commands->{$data->{command}}->{sub};
		$self->info("We have to $data->{command} the subjobs of $id");
		my $ids = $self->{TASK_DB}->queryColumn("select queueid from QUEUE q $data->{cond}");
#		my @extra;
#		$commands->{$data->{command}}->{extra}
#			and push @extra, @{$commands->{$data->{command}}->{extra}};
		my $masterWaiting = 0;
		
		$self->{TASK_DB}->do("update QUEUE set mtime=now() where queueid=?", {bind_values => [$id]});
		foreach my $subjob (@$ids) { 
			my ($done) = $self->$subroutine($subjob); #, $user, @extra);
			$self->info("Despues del Kill @$ids $subjob");
			ref($done) eq 'ARRAY' and @$done[0]==-1 and $done=-1;
			if ($done==-1) {
				#shift @$done;				
				$self->putJobLog($id, "error", "Error $data->{command}ing  subjob $subjob: $done");
				return $done;
			}			
			$data->{command} =~ /resubmit/ and $masterWaiting = 1;
			$self->info("$data->{command}ing subjob $subjob ($done)");
			$self->putJobLog($id, "state", "$data->{command}ing subjob $subjob ($done)");

		}
		if ($masterWaiting) {
			$self->info("And we should put the masterJob to SPLIT");
			$self->{TASK_DB}->updateStatus($id, 'DONE', 'SPLIT');
		}
	}
	$self->info("getMasterJob done for $id");
	return 1;

}

sub masterJobInfoFileBroker {
	my $self = shift;
	my $id   = shift;
	my $data = shift || {};

	$self->info("Checking if we are doing File Brokering");
	my $group = "2";
	my $site  = "";
	if ($data->{site}) {
		$site  = ", sites ";
		$group = "2,3"

	}
	my $files = $self->{TASK_DB}->query(
	"select count(1)c ,ifnull(status, 0) status $site from 
	FILES_BROKER f left join QUEUE using (queueid) 
                      join QUEUE_STATUS using (statusid)
where f.split=? group by $group", undef,
		{bind_values => [$id]}
	);
	$files and @$files or return 1;
	$self->info("This job uses file brokering!");
	foreach my $entry (@$files) {
		$self->info("There are $entry->{c} files in status $entry->{status}")

	}
	return $files;
}

sub checkJobAgents {
	my $self = shift;
	my $inDB = $self->{DB}->queryColumn("SELECT batchId from JOBAGENT")
		or $self->info("Error getting the list of jobagents")
		and return;
	my @inDB    = @$inDB;
	my @inBatch = $self->{BATCH}->getAllBatchIds();

	$self->info("According to the db: @inDB. According to the batch system: @inBatch");
	foreach my $job (@inDB) {
		$self->info("Looking for $job");
		grep (/^$job$/, @inBatch) or $self->info("Agent $job is dead!!\n", undef, 0);
		@inBatch = grep (!/^$job$/, @inBatch);
	}
	if (@inBatch) {
		$self->info("Jobs @inBatch are in the batch system, but not in the DB");
	}
	$self->info("Finally, let's check also in the LQ");
	eval { $self->{BATCH}->checkJobAgents(); };
	if ($@) {
		$self->info("Error checking the jobagents in the LQ: $@");
	}
	return 1;
}

sub requirementsFromPackages {
	my $self   = shift;
	my $job_ca = shift;

	$DEBUG and $self->debug(1, "Checking Packages required by the job");
	my ($ok, @packages) = $job_ca->evaluateAttributeVectorString("Packages");
	($ok) or return "";

	my $installed = "";

	#Checking if the packages have to be installed
	($ok, my $value) = $job_ca->evaluateExpression("PackagesPreInstalled");
	if ($ok and $value) {
		$self->debug(1, "The packages have to be installed");
		$installed = "Installed";
	}

	#$self->debug(1,"Checking if the packages @packages are defined in the system");
	my ($status, @definedPack) = $self->getAllPackages();

	$status
		or $self->info("Error getting the list of packages")
		and return;
	my $requirements = "";

	foreach my $package (@packages) {
		$package =~ /@/  or $package = ".*\@$package";
		$package =~ /::/ or $package = "${package}::.*";

		#$self->debug(1,"checking if $package is in @definedPack");
		my @name = grep (/^$package$/, @definedPack);
		if (@name) {
			$requirements .= " && (member(other.${installed}Packages, \"$name[0]\"))";
			next;
		}
		$self->info("The package $package is not defined!!");
		return;
	}

	return $requirements;
}

sub getAllPackages {
	my $self = shift;

	my ($info) = AliEn::Util::returnCacheValue($self, "all_packages");
	if ($info and ${$info}[0]) {
		return @$info;
	}

	my ($status, @definedPack) = $self->{PACKMAN}->f_packman("list", "-silent", "-all");
	AliEn::Util::setCacheValue($self, "all_packages", [ $status, @definedPack ]);
	return $status, @definedPack;
}

sub requirementsFromMemory {
	my $self         = shift;
	my $job_ca       = shift;
	my $requirements = "";
	my ($ok, $memory) = $job_ca->evaluateExpression("Memory");
	if ($memory) {
		($memory =~ s/\s+mb?\s*//i) and $memory *= 1024;
		($memory =~ s/\s+gb?\s*//i) and $memory *= 1024 * 1024;
		($memory =~ s/\s+kb?\s*//i);
		$memory =~ /^\s*\d+\s*$/
			or $self->info(
"Sorry, I don't understand '$memory' as a memory unit. Memory is supposed to be the number of KB that you want in the worker node. You can specify MB or GB if you prefer to specify in those units"
			)
			and return;
		$requirements = " && (other.FreeMemory>$memory) ";
	}
	($ok, $memory) = $job_ca->evaluateExpression("Swap");
	if ($memory) {
		($memory =~ s/\s+mb?\s*//i) and $memory *= 1024;
		($memory =~ s/\s+gb?\s*//i) and $memory *= 1024 * 1024;
		($memory =~ s/\s+kb?\s*//i);
		$memory =~ /^\s*\d+\s*$/
			or $self->info(
"Sorry, I don't understand $memory as a swap unit. Memory is supposed to be the number of KB that you want in the worker node. You can specify MB or GB if you prefer to specify in those units"
			)
			and return;
		$requirements = " && (other.FreeSwap>$memory) ";
	}

	return $requirements;
}

sub f_jobListMatch_HELP {
	return "jobListMatch: Checks if the jdl of the job matches any of the current CE. 

Usage:
\t\tjobListMatch [-v] <jobid> [<ce_name> ]

If the site is specified, it will only compare the jdl with that CE

Options:
  -v: verbose mode: display also the name of the sites that do not match
  -n: number: print the number of jobs with a higher priority per site that matches
";

}

sub f_jobListMatch {
	my $self    = shift;
	my $options = shift;
	my $jobid   = shift
		or $self->info("Error: no jobid in jobListMatch\n" . $self->f_jobListMatch_HELP())
		and return;
	my $ceName = shift || "%";

	my $jdl = $self->f_ps_jdl($jobid, "-silent")
		or return;
	my $job_ca = AlienClassad::AlienClassad->new($jdl);
	$job_ca         or $self->info("Error creating the classad of the job")    and return;
	$job_ca->isOK() or $self->info("The syntax of the job jdl is not correct") and return;
	my @result = $self->f_queueinfo($ceName, "-jdl", "-silent");
	my $done=shift @result;
	$done or return;

	my $anyMatch = 0;
	my $jobPriority;

		$jobPriority =
			$self->{TASK_DB}->queryValue(
			"select  if(j.priority, j.priority, 0) from QUEUE join JOBAGENT j on (entryid=agentid) where queueid=?",
			undef, {bind_values => [$jobid]});
		$self->info("The priority of the job is $jobPriority", undef, 0);


	foreach my $site (@$done) {
		if (!$site->{jdl}) {
			$options =~ /v/ and $self->info("\t Ignoring $site->{site} (the jdl is not right)", undef, 0);
			next;
		}

		#$self->debug(2,"Comparing with $site->{site} (and $site->{jdl})");
		my $ce_ca = AlienClassad::AlienClassad->new($site->{jdl});
		if (!$ce_ca->isOK()) {
			$options =~ /v/ and $self->info("The syntax of the CE jdl is not correct");
			next;
		}
		my ($match, $rank) = AlienClassad::Match($job_ca, $ce_ca);
		my $status = "no match :(";
		$match and $status = "MATCHED!!! :)" and $anyMatch++;
		if ($options =~ /v/ or $status =~ /MATCHED!!! :\)/) {
			$self->info("\tComparing the jdl of the job with $site->{site}... $status", undef, 0);
		}

		if ($options =~ /n/) {
			
				$site->{site} =~ /::(.*)::/;
				my $sitePattern = "\%,$1,\%";
				my $number      = $self->{TASK_DB}->queryValue(
					"select sum(counter) from JOBAGENT
         where priority>= ? and (site is null or site like ? or site = '')", undef,
					{bind_values => [ $jobPriority, $sitePattern ]}
				);
				$number or $number = 0;
				$self->info("\t\tJobs for $site->{site} with higher priority: $number", undef, 0);
		}

#     ($options=~ /v/ or  $status=~ /MATCHED!!! :\)/  ) and $self->info("\tComparing the jdl of the job with $site->{site}... $status",undef,0);
	}
	$self->info("In total, there are $anyMatch sites that match");
	if ($anyMatch <= 0 && $options =~ /v/) {
		$self->jobFindReqMiss($done, $job_ca);
	}
	return $anyMatch;
}

sub jobFindReqMissHelp {
	return "jobFindReason: Looks in the job's jdl to find the reason why it didn't match any CE. 

Usage:
\t\tjobFindReason <jobid> ";
}

sub jobFindReqMiss {
	my $self   = shift;
	my $sites  = shift;
	my $job_ca = shift;

	my $initReq;

	my $reason = "Non";

	my $count2       = 0;
	my @requirements = ("other\.CE", "SE", "Packages", "TTL", "Price", "LocalDiskSpace");
	my @explanation  = (
		"This is not the CE that was requested",
		"One or more input files are located in a SE not close to the CE requested",
		"Package doesn't exit or deleted from the Cataluge",
		"TTL is not matched by this CE",
		"Price is not matched by this CE",
		"Not enough disk space"
	);
	my $siteErros;

	#  my $numOfCorrectSites=0;
	my $ce_ca;

	my $req = $job_ca->evaluateExpression("Requirements");
	$initReq = $req;

	$req or $self->{LOGGER}->error("CE", "In jobFindReqMiss, failed to requirements") and return -1;

	my $all_sites_reasons = {};

	#get rid of impossible requirements
	# 	my $unKnownCE="";
	# 	foreach my $site (@$sites){
	# 		my $ceName= $site->{site};#$ce_ca->evaluateAttributeString("CE");
	# 		$self->debug(1, "Checkning if $ceName is in the job jdl" );
	# 		#assuming there can be only one CE
	# 		if($req =~ /other.CE ==(.*?)\&&/i && $req !~ m/$ceName/i ){
	# 			$unKnownCE = $1;
	# 			$siteErros++;
	# 		}
	# 	}
	# 	$self->debug(1, "$unKnownCE does not match any known CE" );
	$self->info("Starting with $req");
	foreach my $site (@$sites) {
		$self->info("Checking $site->{site}");
		my @allReasons;
		if (!$site->{jdl}) {
			next;
		}
		$ce_ca = AlienClassad::AlienClassad->new($site->{jdl});
		if (!$ce_ca->isOK()) {
			next;
		}

		#Re-set the jdl
		my $tmpReq = $req = $initReq;
		$job_ca->set_expression("Requirements", $initReq);
		$ce_ca = AlienClassad::AlienClassad->new($site->{jdl});

		#    $numOfCorrectSites++;
		#now start removing reqirements till something matcches
		my ($match, $rank) = AlienClassad::Match($job_ca, $ce_ca);
		while (!$match) {
			my @reqs = split(/&&/, $req, 2);
			$reason = $reqs[0];
			if ($reqs[1]) {
				$req = $reqs[1];
				$req = $self->repairReq($req);
				$job_ca->set_expression("Requirements", $req)
					or $self->{LOGGER}->error("CE", "In jobFindReqMiss, failed to set new requirements")
					and return -1;
				($match, $rank) = AlienClassad::Match($job_ca, $ce_ca);
			} else {
				$match = 1;
			}
			$self->debug(1, "did we match?? $match ($req)");
			if ($match) {

				# 				#dont add duplicate reasons
				my $exists = 0;
				foreach my $elem (@allReasons) {
					if ($elem eq $reason) {
						$exists = 1;
						last;
					}
				}
				if (!$exists) {
					$self->debug(1, "one problem is $reason");
					push(@allReasons, $reason);
				}
				if ($tmpReq =~ s/\Q$reason//) {
					$tmpReq =~ s/&&&&/&&/;
					$tmpReq =~ s/&&\s*$//;
				}
				$req = $tmpReq;
				$self->debug(1, "Let's see if now it works... ($req)");
				$job_ca->set_expression("Requirements", $req);
				($match, $rank) = AlienClassad::Match($job_ca, $ce_ca);
			}
		}
		$all_sites_reasons->{$site->{site}} = \@allReasons;
		if (@allReasons) {
			$self->info("Unmet Requirements for $site->{site}:");
			my $count = 0;
			my $found;
			foreach $reason (@allReasons) {
				$count++;
				foreach $req (@requirements) {
					$found = 0;
					if ($reason =~ m/$req/) {
						$self->info("\t$count)$reason: $explanation[$count2]");
						$found  = 1;
						$count2 = 0;
						last;
					}
					$count2++;
				}
				if (!$found) {
					$self->info("\tunknown problem : $reason");
				}
			}
		}

	}

	my $min_unmet = 10000000;
	my @min_sites = ();
	foreach my $s (keys %$all_sites_reasons) {
		$self->debug(1, "$s  has $#{$all_sites_reasons->{$s}}");
		if ($#{$all_sites_reasons->{$s}} < $min_unmet) {
			@min_sites = $s;
			$min_unmet = $#{$all_sites_reasons->{$s}};
		} elsif ($#{$all_sites_reasons->{$s}} eq $min_unmet) {
			push @min_sites, $s;
		}
	}
	$min_unmet++;
	$self->info("The best sites are @min_sites (with only $min_unmet)");

	#put the init back
	# 	$job_ca->set_expression("Requirements",$initReq);

	# 	 my ($match, $rank ) = AlienClassad::Match( $job_ca, $ce_ca );
	# 	my $size = @$sites;
	# 	print "site $siteErros numOfSite $numOfCorrectSites\n";
	# 	if ( $initReq =~ /other.CE ==(.*?)\&&/ && $siteErros >= $numOfCorrectSites ){
	# 		$reason = " other.CE ==" . $1;
	# 		#my $sdf=$ce_ca->evaluateAttributeString("CE");
	# 		$self->debug(1,"adding $reason to unmet requirements" );
	# 		push(@allReasons, $reason);
	# 	}

	return $all_sites_reasons;
}

sub fixReq {
	my $self        = shift;
	my $problemCode = shift;
	my $job_ca      = shift;
	my $reason      = shift;
	my @allCeCa     = shift;
	my $req         = $job_ca->evaluateExpression("Requirements");

	switch ($problemCode) {

		#propose solution. List available CE, and/or ask user what to do
		case 0 { $self->info("Not imlemented") and return; }

		#mirror files to the CE
		case 1 {
			my ($ok, @files) = $job_ca->evaluateAttributeVectorString("inputdata");

			#see if there is a choise
			my $dest;
			if ($req =~ /other.CE ==(.*?)\&&/) {
				$self->debug(1, "Files have to be copied to a CE close to $1");
				$dest = $1;
				$dest =~ s/"//g;
				$dest =~ s/ //g;
				foreach my $se (@allCeCa) {
					my $ceName = $se->evaluateAttributeString("CE");
					if ($ceName eq $dest) {
						my @closeSE = $se->evaluateAttributeVectorString("CloseSE");
						foreach my $elem (@closeSE) {
							$dest = $elem;
							if ($dest =~ m/::/g) {
								last;
							}
						}
						last;
					}
				}
			} else {
				$reason =~ /"(.*?)\"/;
				$dest = $1;
			}
			$dest =~ s/\Q)// or $dest =~ s/\Q(//;
			$dest or $self->{LOGGER}->error("CE", "In fixReq failed to find a destination to mirror files") and return -1;

			foreach my $file (@files) {
				my @fName = split(/:/, $file, 2);

#find out location and SE
# 				my @fileInfo = $self->{CATALOG}->execute("whereis","-s", $fName[1]) or $self->{LOGGER}->error("CE","In fixReq Error getting the info from '$fName[1]'") and return;
				$self->info("Mirroring $fName[1] to $dest");
				if ($self->{CATALOG}->execute("mirror", $fName[1], $dest)) {

					# 					$req =~ m/CloseSE,"(.*?)\"/;
					# 					$req =~ s/$1/$dest/;
					my @reqs = split(/&&/, $req);
					foreach my $elem (@reqs) {
						if ($elem =~ m/CloseSE,"(.*?)\"/g) {
							if ($1 ne $dest) {
								$req = $self->repairReq($req);
								my $len = length($req);
								my $fragment = substr $req, ($len - 4);
								if ($fragment =~ m/&&/) {
									$req = substr $req, 0, ($len - 4);
								}
							}
						}
					}

					#inf loop
					# 					while ($req =~ m/CloseSE,"(.*?)\"/g) {
					# 						if($1 ne $dest){
					# 							$req = $self->repairReq($req);
					# 							my $len = length ($req);
					# 							my $fragment =  substr $req, ($len-4);
					# 							if($fragment =~ m/&&/){
					# 								$req = substr $req, 0,($len-4);
					# 							}
					# 						}
					# 					}
				}

				# 				my @info = $self->{CATALOG}->execute( "ls", "$fName[1]", "-sl" );
				# 				my @lsTokens = split(/###/,$info[0]);
			}
		}
		else { $self->info("Could not resolve problem"); }
	}
	return $req;
}

sub repairReq {
	my $self = shift;
	my $req  = shift;
	$req =~ s/&&&&/&&/   or $req =~ s/ &&  &&/ &&/;
	$req =~ s/\Q||||/&&/ or $req =~ s/\Q ||  ||/ ||/;
	$req =~ s/&&$//      or $req =~ s/\|\|$//;
	$req =~ s/\Q)&&/) &&/;

	# 	$req =~ s/\Q( m/m/ and $req =~ s/\) \)$/)/;
	return $req;
}

sub resyncJobAgent {
	my $self = shift;
	$self->info("Ready to resync the number of jobs waiting in the system");

	my ($host, $driver, $db) =
		split("/", $self->{CONFIG}->{"JOB_DATABASE"});

	$self->{TASK_DB}
		or $self->{TASK_DB} =
		AliEn::Database::TaskQueue->new(
		{DB => $db, HOST => $host, DRIVER => $driver, ROLE => 'admin', SKIP_CHECK_TABLES => 1});
	$self->{TASK_DB}
		or $self->info("In initialize creating TaskQueue instance failed")
		and return;
	$self->info("First, let's take a look at the missing jobagents");

	my $jobs = $self->{TASK_DB}->query(
		"select userid, origjdl jdl, agentid from QUEUE q join 
(select min(queueid) as q from QUEUE left join JOBAGENT on agentid=entryid where entryid is null  and statusId=5 group by agentid) t  on queueid=q
join QUEUEJDL using (queueid)"
		)
		or $self->info("Error getting the jobs without jobagents")
		and return; #5=WAITING

	foreach my $job (@$jobs) {
		$self->info("We have to insert a jobagent for $job->{agentid}");

		$job->{jdl} =~ /[\s;](requirements[^;]*).*\]/ims
			or $self->info("Error getting the requirements from $job->{jdl}")
			and next;
		my $req = $1;
		$job->{jdl} =~ /(\suser\s*=\s*"([^"]*)")/si or $self->info("Error getting the user from '$job->{jdl}'") and next;
		$req .= "; $1 ";
		my $params = $self->{TASK_DB}->extractFieldsFromReq($req);
		$params->{entryId} = $job->{agentid};
		$self->{TASK_DB}->insert("JOBAGENT", $params);
	}

	$self->info("Now, update the jobagent numbers");

	$self->{TASK_DB}
		->do("update JOBAGENT j set counter=(select count(*) from QUEUE where statusId=5 and agentid=entryid)"); #5=WAITING
	$self->{TASK_DB}->do("delete from JOBAGENT where counter<1");
	$self->info("Resync done");
	$self->f_resyncPriorities();
	return 1;
}

sub f_resyncPriorities {
	my $self = shift;

	$self->{TASK_DB} or return 0;
	$self->info("Updating the priorities");

	$self->{TASK_DB}->insertNewPriorityUsers();
	$self->info("Now, compute the number of jobs waiting and priority per user");
  $self->{TASK_DB}->getPriorityUpdate();

	$self->info("Finally, let's update the JOBAGENT table");
	
	$self->{TASK_DB}->do("UPDATE JOBAGENT j set priority=(select computedPriority from PRIORITY p where p.userid=j.userid)");

	return 1;
}

sub f_killAllAgents {
	my $self = shift;
	$self->info("Ready to kill all the jobagents that are on this site");
	my @inBatch = $self->{BATCH}->getAllBatchIds();
	foreach my $job (@inBatch) {
		$self->info("Ready to kill $job");
	}
	return 1;
}

#____________________________________________________________________________________
# Quota
#____________________________________________________________________________________

sub f_jquota_HELP {
	my $self   = shift;
	my $whoami = $self->{CATALOG}->{CATALOG}->{ROLE};
	if (($whoami !~ /^admin(ssl)?$/)) {
		return "jquota: Displays information about Job Quotas.
Usage:
  jquota list                       - show the user quota for job\n";
	}

	return "jquota: Displays information about Job Quotas.
Usage:
  jquota list <user>                - list the user quota for job
                                     use just 'jquota list' for all users

  jquota set <user> <field> <value> - set the user quota for job
                                      (maxUnfinishedJobs, maxTotalCpuCost, maxTotalRunningTime)
                                      use <user>=% for all users\n";
}

sub f_jquota {
	my $self    = shift;
	my $command = shift or $self->info($self->f_jquota_HELP()) and return;

	$DEBUG and $self->debug(1, "Calling f_jquota_$command");
	if (($self->{CATALOG}->{CATALOG}->{ROLE} !~ /^admin(ssl)?$/) && ($command eq "set")) {
		$self->{LOGGER}->error("CE", "You are not allowed to execute this command!");
		return;
	}

	my @return;
	my $func = "f_jquota_$command";
	eval { @return = $self->$func(@_); };
	if ($@) {

		#If the command is not defined, just print the error message
		if ($@ =~ /Can\'t locate object method \"$func\"/) {
			$self->info("jquota doesn't understand '$command'", 111);

			#this is just to print the error message"
			return $self->f_jquota();
		}
		$self->info("Error executing jquota $command: $@");
		return;
	}
	return @return;
}

sub f_jquota_list {
	my $self   = shift;
	my $user   = shift || "%";
	my $whoami = $self->{CATALOG}->{CATALOG}->{ROLE};

	# normal users can see their own information
	if (($whoami !~ /^admin(ssl)?$/) and ($user eq "%")) {
		$user = $whoami;
	}

	if (($whoami !~ /^admin(ssl)?$/) and ($user ne $whoami)) {
		$self->info("Not allowed to see other users' quota information");
		return;
	}
	my @bind;
	my $where="";
  if($user ne '%'){
		$where="where user=?";
		push @bind, $user;
  }

 

	my $result = $self->{TASK_DB}->query("select 
	user, priority, computedpriority, running, maxparallelJobs, unfinishedJobsLast24h, maxUnfinishedJobs, totalRunningTimeLast24h, maxTotalRunningTime, totalCpuCostLast24h, maxTotalCpuCost
from PRIORITY join QUEUE_USER using (userid) $where ", undef, {bind_values=>\@bind}
		)
		or $self->info("Failed to getting data from PRIORITY table", 1)
		and return;
	$result->[0]
		or $self->info("User $user not exist", 1)
		and return;

	my $cnt = 0;


	$self->info(
"-------------------------------------------------------------------------------------------
                user        priority         runningJobs        unfinishedJobs        totalCpuCost                    totalRunningTime
------------------------------------------------------------------------------------------\n",

		undef, 0
	);

	foreach (@$result) {
		$cnt++;
		$self->info(
			sprintf(
				" [%04d. ]   %12s   %5s (%5s)   %5s/%5s        %5s/%5s         %5s/%5s             %5s/%5s",
				$cnt,                            $_->{'user'},  $_->{priority}, $_->{computedpriority},
                                $_->{running}, $_->{maxparallelJobs},
                                $_->{'unfinishedJobsLast24h'},
				$_->{'maxUnfinishedJobs'},       $_->{'totalCpuCostLast24h'}, $_->{'maxTotalCpuCost'},
				$_->{'totalRunningTimeLast24h'}, $_->{'maxTotalRunningTime'}
			),
			undef, 0
		);
	}
	$self->info("-------------------------------------------------------------------------------------------\n", undef,
		0);
}

sub f_jquota_set_HELP {
	return "Usage:
  jquota set <user> <field> <value> - set the user quota for job
                                      (maxUnfinishedJobs, maxTotalCpuCost, maxTotalRunningTime, priority, maxParallelJobs
                                      use <user>=% for all users\n";
}

sub f_jquota_set {
	my $self  = shift;
	my $user  = shift or $self->info($self->f_jquota_set_HELP()) and return;
	
	my $set={};
	my $error=1;
	while (@_){
	 
	  my $field = shift or $self->info($self->f_jquota_set_HELP()) and return;
	  my $value = shift;
	  (defined $value) or $self->info($self->f_jquota_set_HELP()) and return;
	  if ($field !~ /(maxUnfinishedJobs)|(maxTotalRunningTime)|(maxTotalCpuCost)|(priority)|(maxParallelJobs)/) {
		  $self->info("Wrong field name! Choose one of them: maxUnfinishedJobs, maxTotalRunningTime, maxTotalCpuCost, priority, maxParallelJobs\n");
	  	return;
	  }
		$set->{$field}=$value;
		$error=0;
	}
  $error and $self->info($self->f_jquota_set_HELP()) and return;
	
	my $done = $self->{TASK_DB}->updatePrioritySet($user, $set);
	$done or $self->info("Failed to set the value in PRIORITY table") and return;

	if ($done eq '0E0') {
		($user ne "%") and $self->info("User '$user' not exist.") and return;
	}

	$done and $self->f_jquota_list($user);
}

sub calculateJobQuota {
	my $self   = shift;
	my $silent = shift;

	my $method = "info";
	my @data;
	$silent and $method = "debug" and push @data, 1;

	my $user = $self->{CATALOG}->{CATALOG}->{ROLE};
  
	($user =~ /^admin(ssl)?$/)
		or $self->info("Error: only the administrator can check the databse")
		and return;

	$self->$method(@data, "Calculate Job Quota");

	$self->$method(@data, "Compute the number of unfinished jobs in last 24 hours per user");

#$self->{TASK_DB}->do("update PRIORITY pr left join (select SUBSTRING( submitHost, 1, POSITION('\@' in submitHost)-1 ) as user, count(1) as unfinishedJobsLast24h from QUEUE q where (status='INSERTING' or status='WAITING' or status='STARTED' or status='RUNNING' or status='SAVING' or status='OVER_WAITING') and (unix_timestamp()>=q.received and unix_timestamp()-q.received<60*60*24) group by submithost) as C on pr.user=C.user set pr.unfinishedJobsLast24h=IFNULL(C.unfinishedJobsLast24h, 0)") or $self->$method(@data, "Failed");
	$self->{TASK_DB}->unfinishedJobs24PerUser or $self->$method(@data, "Failed");
	$self->$method(@data, "Compute the total runnning time of jobs and cpu in last 24 hours per user");

	$self->$method(@data, "Compute the total cpu cost of jobs in last 24 hours per user");

#$self->{TASK_DB}->do("update PRIORITY pr left join (select SUBSTRING( submitHost, 1, POSITION('\@' in submitHost)-1 ) as user, sum(p.cost) as totalCpuCostLast24h , sum(p.runtimes) as totalRunningTimeLast24h from QUEUE q join QUEUEPROC p using(queueId) where (unix_timestamp()>=q.received and unix_timestamp()-q.received<60*60*24) and status='DONE' group by submithost) as C on pr.user=C.user set pr.totalRunningTimeLast24h=IFNULL(C.totalRunningTimeLast24h, 0), pr.totalCpuCostLast24h=IFNULL(C.totalCpuCostLast24h, 0)");
	$self->{TASK_DB}->cpuCost24PerUser or $self->$method(@data, "Failed");
	$self->$method(@data, "Change job status from OVER_WAITING to WAITING");

#$self->{TASK_DB}->do("update QUEUE q join PRIORITY pr on pr.user=SUBSTRING( q.submitHost, 1, POSITION('\@' in q.submitHost)-1 ) set q.status='WAITING' where (pr.totalRunningTimeLast24h<pr.maxTotalRunningTime and pr.totalCpuCostLast24h<pr.maxTotalCpuCost) and q.status='OVER_WAITING'") or $self->$method(@data, "Failed");
	$self->{TASK_DB}->changeOWtoW or $self->$method(@data, "Failed");
	$self->$method(@data, "Change job status from WAITING to OVER_WAITING");

#$self->{TASK_DB}->do("update QUEUE q join PRIORITY pr on pr.user=SUBSTRING( q.submitHost, 1, POSITION('\@' in q.submitHost)-1 ) set q.status='OVER_WAITING' where (pr.totalRunningTimeLast24h>=pr.maxTotalRunningTime or pr.totalCpuCostLast24h>=pr.maxTotalCpuCost) and q.status='WAITING'") or $self->$method(@data, "Failed");
	$self->{TASK_DB}->changeWtoOW or $self->$method(@data, "Failed");
	$self->$method(@data, "Synchronize with SITEQUEUES");
	foreach (qw(OVER_WAITING WAITING)) {
		$self->{TASK_DB}
			->do("update SITEQUEUES s set $_=(select count(1) from QUEUE q where statusId='$_' and s.siteid=q.siteid)")
			or $self->$method(@data, "$_ Failed");
	}

	return;
}
sub f_verifyToken {
  my $self  = shift;
  my @arg   = grep ( !/-z/, @_ );
  my $jobId = shift @arg
    or print STDERR "You have to provide a job identifier" and return;
  my $token = shift @arg
    or print STDERR "You have to provide a job token" and return;
  my @results;

  my $user =
    $self->{TASK_DB}->queryValue("SELECT username from JOBTOKEN where jobid=? and jobtoken=?", undef, {bind_values=>[$jobId, $token]} );
  if ( $user ) {
    push @results, $user;
  }
  $self->info("WE GOT THE USER $user\n");
  return @results;
}


return 1;
