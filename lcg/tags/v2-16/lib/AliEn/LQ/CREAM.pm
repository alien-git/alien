package AliEn::LQ::CREAM;

use AliEn::LQ::LCG;
use vars qw (@ISA);
@ISA = qw( AliEn::LQ::LCG);

use strict;
use AliEn::Database::CE;
use File::Basename;
use Net::LDAP;
use AliEn::TMPFile;
use POSIX ":sys_wait_h";
use Sys::Hostname;

sub initialize {
   my $self=shift;
   $self->{DB}=AliEn::Database::CE->new();
   $ENV{X509_CERT_DIR} and $self->{LOGGER}->debug("LCG","X509: $ENV{X509_CERT_DIR}");
   my $host= `/bin/hostname` || $self->{CONFIG}->{HOST};
   chomp $host;
   $self->{CONFIG}->{VOBOX} = $host.':8084';
   $ENV{ALIEN_CM_AS_LDAP_PROXY} and $self->{CONFIG}->{VOBOX} = $ENV{ALIEN_CM_AS_LDAP_PROXY};
   $self->info("This VO-Box is $self->{CONFIG}->{VOBOX}, site is \'$ENV{SITE_NAME}\'");
   $self->{CONFIG}->{VOBOXDIR} = "/opt/vobox/\L$self->{CONFIG}->{ORG_NAME}";
   $self->{UPDATECLASSAD} = 0;
   
   my $cmds = {  SUBMIT_CMD  => 'glite-ce-job-submit',
                 STATUS_CMD  => 'glite-ce-job-status',
		 KILL_CMD    => 'glite-ce-job-cancel',
		 CLEANUP_CMD => 'glite-ce-job-output'};
			 
   $self->{$_} = $cmds->{$_} || $self->{CONFIG}->{$_} || '' foreach (keys %$cmds);
   
   unless ( $ENV{LCG_GFAL_INFOSYS} ) {
     $self->{LOGGER}->error("\$LCG_GFAL_INFOSYS not defined in environment");
     return;
   }    
   $self->{CONFIG}->{CE_SITE_BDII} = '';
   if ($ENV{CE_SITE_BDII}) {
     $self->{CONFIG}->{CE_SITE_BDII} = $ENV{CE_SITE_BDII};
   } else {
      $self->info("No site BDII defined in \$ENV, querying $ENV{LCG_GFAL_INFOSYS}");
      my $IS = "ldap://$ENV{LCG_GFAL_INFOSYS}";
      my $DN = "mds-vo-name=$ENV{SITE_NAME},mds-vo-name=local,o=grid";
      $self->debug(1,"Querying $IS/$DN");
      if (my $ldap =  Net::LDAP->new($IS)) {
        if ($ldap->bind()) {
	  my $result = $ldap->search( base   => $DN,
  		                      filter => "GlueServiceType=bdii_site");
          my $code = $result->code;
	  unless ($code) {
	    my $entry  = $result->entry(0);
	    my $thisDN = $entry->dn;
	    $self->debug(1,"Found $thisDN");
            my $found = $entry->get_value("GlueServiceEndpoint");
  	    $self->{CONFIG}->{CE_SITE_BDII} = $found;
	  } else {
	    my $msg = $result->error();
	    $self->{LOGGER}->error("LCG","Error querying: $code ($msg)");
	  }			 
	  $ldap->unbind();	
        } else {
	  $self->{LOGGER}->error("LCG","Could not bind to $IS");
	}
     } else {
       $self->{LOGGER}->error("LCG","Could not contact $IS");
     }
   }    
   if ($self->{CONFIG}->{CE_SITE_BDII}) {
     $self->info("Site BDII is $self->{CONFIG}->{CE_SITE_BDII}"); 
   } else {
     $self->{LOGGER}->warning("LCG","No site BDII defined and could not find one in IS");
   }  
   
   if ( $ENV{CE_LCGCE} ) {
     $self->info("Taking the list of CEs from \$ENV: $ENV{CE_LCGCE}");
     my $string = $ENV{CE_LCGCE};
     my @sublist = ($string =~ /\(.+?\)/g);
     $string =~ s/\($_\)\,?// foreach (@sublist);
     push  @sublist, split(/,/, $string);
     $self->{CONFIG}->{CE_LCGCE_LIST} = \@sublist;
   }   
   # Flat-out sublist in CE list
   my @flatlist = ();
   foreach my $CE ( @{$self->{CONFIG}->{CE_LCGCE_LIST}} ) {
     $CE =~ s/\s*//g;
     if (  $CE =~ m/\(.*\)/ ) {
       $CE =~ s/\(//; $CE =~ s/\)//;
       push @flatlist, split (/,/,$CE);
     } else {
       push @flatlist, $CE;
     }
   }
   $self->{CONFIG}->{CE_LCGCE_LIST_FLAT} = \@flatlist;
      

   $self->{CONFIG}->{CE_MINWAIT} = 180; #Seconds
   defined $ENV{CE_MINWAIT} and $self->{CONFIG}->{CE_MINWAIT} = $ENV{CE_MINWAIT};
   $self->info("Will wait at least $self->{CONFIG}->{CE_MINWAIT}s between submission loops.");
   $self->{LASTCHECKED} = time-$self->{CONFIG}->{CE_MINWAIT};
   
   $self->renewProxy(100000);
   return 1;
}

sub submit {
  my $self = shift;
  my $jdl = shift;
  my $command = shift;

  my $startTime = time;
  my @args=();
  $self->{CONFIG}->{CE_SUBMITARG_LIST} and @args = @{$self->{CONFIG}->{CE_SUBMITARG_LIST}};
  my $jdlfile = $self->generateJDL($jdl, $command);
  $jdlfile or return;

  $self->renewProxy(100000);

  $self->info("Submitting to LCG with \'@args\'.");
  my $now = time;
  my $logFile = AliEn::TMPFile->new({filename=>"job-submit.$now.log"}) or return;

  my $contact = '';
  $contact = $self->wrapSubmit($logFile, $jdlfile, @args);

  $self->info("LCG JobID is $contact");
  $self->{LAST_JOB_ID} = $contact;
  open JOBIDS, ">>$self->{CONFIG}->{LOG_DIR}/CE.db/JOBIDS";
  print JOBIDS "$now,$contact\n";
  close JOBIDS;

  my $submissionTime = time - $startTime;
  $self->info("Submission took $submissionTime sec.");
  return 0;
}

sub wrapSubmit {
  my $self = shift;
  my $logFile = shift;
  my $jdlfile = shift;
  my @args = @_ ;  
  my @command = ( $self->{SUBMIT_CMD}, "--noint", "--nomsg");
  @command = ( @command, "--logfile", $logFile, @args, "$jdlfile");
  my @output = $self->_system(@command);
  my $error = $?;
  (my $jobId) = grep { /https:/ } @output;
  return if ( $error || !$jobId);
  $jobId =~ m/(https:\/\/[A-Za-z0-9.-]*:8443\/CREAM\d+)/;
  $jobId = $1; chomp $jobId;
  return $jobId;
}

sub generateJDL {
  my $self = shift;
  my $ca = shift;
  my $command=shift;
  my $bdiiReq=shift;
  my $requirements = $self->translateRequirements($ca, $bdiiReq);

  my $exeFile = AliEn::TMPFile->new({filename=>"dg-submit.$$.sh"})
    or return;

  open( BATCH, ">$exeFile" )
    or print STDERR "Can't open file '$exeFile': $!"
      and return;
  print BATCH "\#!/bin/sh
\# Script to run AliEn on LCG
\# Automatically generated by AliEn running on $ENV{HOSTNAME}

export OLDHOME=\$HOME
export HOME=`pwd`
export ALIEN_LOG=$ENV{ALIEN_LOG}
echo --- hostname, uname, whoami, pwd --------------
hostname
uname -a
whoami
pwd
echo --- ls -la ------------------------------------
ls -lart
echo --- df ----------------------------------------
df -h
echo --- free --------------------------------------
free

";

  my $exec="alien";
  if ( $self->{CONFIG}->{CE_INSTALLMETHOD}) {
    $exec= "\$HOME/bootsh /opt/alien/bin/alien";
    my $method="installWith".$self->{CONFIG}->{CE_INSTALLMETHOD};
    eval {
      ($exec, my $print)=$self->$method();
      print BATCH $print;
    };
    if ($@){
      $self->info("Error calling $method: $@");
      return;

    };
  } else {
    print BATCH "export PATH=\$PATH:\$VO_ALICE_SW_DIR/alien/bin\n";
  }

  print BATCH "
cd \${TMPDIR:-.}
echo --- env ---------------------------------------
echo \$PATH
echo \$LD_LIBRARY_PATH

echo --- alien --printenv --------------------------
$exec -printenv
echo --- alien proxy-info ---------------------------
$exec proxy-info
echo --- Run ---------------------------------------
$exec RunAgent
rm -f dg-submit.*.sh
ls -lart
";

  close BATCH or return;
  
  my $jdlFile = AliEn::TMPFile->new({filename=>"dg-submit.$$.jdl"})
    or return;
  open( BATCH, ">$jdlFile" )
    or print STDERR "Can't open file '$jdlFile': $!"
      and return;
  my $voName=$ENV{ALIEN_VOBOX_ORG}|| $self->{CONFIG}->{ORG_NAME};

  my $now = gmtime()." "."$$"; 
  $_ = "$now";
  s/\s+/\_/g;
  $now = $_;
  mkdir "/tmp/$now", 0755 or warn "the directory for the CREAM outputs cannot be made\n";
  my $host_name = hostname;
  print BATCH "\# JDL automatically generated by AliEn
  Executable = \"/bin/sh\";
  Arguments = \"-x dg-submit.$$.sh\";
  StdOutput = \"std.out\";
  StdError = \"std.err\";
  InputSandbox = {\"$exeFile\"};
  #OutputSandbox = { \"std.err\" , \"std.out\" };
  #Outputsandboxbasedesturi = \"gsiftp://$host_name:2811/tmp/$now\";
  Environment = {\"ALIEN_CM_AS_LDAP_PROXY=$self->{CONFIG}->{VOBOX}\",\"ALIEN_JOBAGENT_ID=$ENV{ALIEN_JOBAGENT_ID}\", \"ALIEN_USER=$ENV{ALIEN_USER}\"};
  ";

  print BATCH "Requirements = $requirements;\n" if $requirements;
  close BATCH;
  return $jdlFile;
}

return 1;

