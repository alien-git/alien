#!/bin/env alien-perl

use strict;
use Test;

use File::Basename;
use AliEn::UI::Catalogue::LCM::Computer;
use AliEn::SOAP;

    my $ALIEN_ROOT=$ENV{ALIEN_ROOT};
       $ALIEN_ROOT or die "ALIEN_ROOT not set !";

    my $HOME=$ENV{HOME};
    my $ALIEN_HOME=$ENV{HOME}."/.alien";
    my $ALIEN_TESTDIR=$ENV{ALIEN_TESTDIR};
    my $USER=$ENV{USER};

    my $ALIEN_ROOT_TMP = $ALIEN_ROOT;
       $ALIEN_ROOT_TMP =~ s/\//\\\//g;

    my $hostname = `hostname -s`;
    chomp $hostname;

    my $domain = `hostname -d`;
    chomp $domain;
    $domain or $domain = "cern.ch";


BEGIN { plan tests => 1}
{
    configureHTTPD();
    configureGold();

 
my @commands=(# make auth_key for gold
              {  command=>"echo goldkey > $ALIEN_ROOT/etc/auth_key && chmod 400 $ALIEN_ROOT/etc/auth_key", ignore=>1 },
              # start gold
              { command=>"env GOLD_HOME=$ALIEN_ROOT $ALIEN_ROOT/bin/alien-perl -T $ALIEN_ROOT/sbin/goldd", ignore=>1, sleep=>5},
              # init gold
              { command=>"env GOLD_HOME=$ALIEN_ROOT $ALIEN_ROOT/bin/alien-perl -T $ALIEN_ROOT/bin/goldsh < bank.gold", ignore=>1, sleep=>5},
              #start apache
              { command=>"env LD_LIBRARY_PATH=$ALIEN_ROOT/httpd/lib:$ENV{LD_LIBRARY_PATH} $ALIEN_ROOT/httpd/bin/httpd -f $ALIEN_ROOT/httpd/conf/httpd.conf -k restart", ignore=>1, sleep=>15},
              #see if apache is running
	          {command=>"ps -ef|grep httpd"}
             );

    foreach (@commands)
    {
        print "\nDoing $_->{command}\n";
        if (system($_->{command}) and not $_->{ignore})
        {
            print "Error doing $_->{command}\n";
            exit(-1);
        }

        $_->{sleep} and sleep($_->{sleep}) and print "Going to sleep for ", $_->{sleep}, " seconds...\n";
    }

ok(1);
}

sub configureGold
{
    # create database 'gold' and add the user 'golduser 'in mysql 
    my $goldSQL = "CREATE database gold;
    GRANT ALL PRIVILEGES ON gold.* TO \'golduser\'@\'$hostname.$domain\';
    FLUSH PRIVILEGES;
    GRANT ALL PRIVILEGES ON gold.* TO \'golduser\'@\'$hostname\';
    FLUSH PRIVILEGES;
    ";

  open (DB, "| mysql -h $hostname -P 3307 -u admin --password=pass");
  print DB $goldSQL;
  close DB;

#prepare base.sql
system ("sed -i -e s/hartem/$ENV{USER}/ base.sql");


# create gold tables
  my $mysqlCMD = "env LD_LIBRARY_PATH=$ALIEN_ROOT/lib/:$ALIEN_ROOT/lib/mysql $ALIEN_ROOT/bin/mysql" ;
  system ("$mysqlCMD -h $hostname -P 3307 -u golduser gold < base.sql");

# prepare goldd.conf 
    my $goldServerConf = " 
    super.user = $ENV{USER}
    server.host = $hostname.$domain
    database.datasource = DBI:mysql:database=gold;host=$hostname.$domain;port=3307;user=golduser
    response.chunksize=10000
    log4perl.logger = OFF
    ";

    open FH, " > $ENV{ALIEN_ROOT}/etc/goldd.conf";
    print FH $goldServerConf;
    close FH;

# prepare gold.conf
    my $goldClientConf = "
	server.host = $hostname.$domain
	log4perl.logger = OFF	
	";	
    
    open FH, "> $ENV{ALIEN_ROOT}/etc/gold.conf";
    print FH $goldClientConf;
    close FH;			

#prepare bank.gold
#substitute 'hartem' with current user	
system ("sed -i -e s/hartem/$ENV{USER}/ bank.gold");
}

sub configureHTTPD
{
    my @seed=(
      # Substitute "/opt/alien" with $ALIEN_ROOT in httpd.conf
      "'s/\\\/opt\\\/alien/$ALIEN_ROOT_TMP/' $ALIEN_TESTDIR/httpd.conf",
	  # Substitute "/opt/alien" with $ALIEN_ROOT in ssl.conf
	  "'s/\\\/opt\\\/alien/$ALIEN_ROOT_TMP/' $ALIEN_ROOT/httpd/conf/extra/httpd-ssl.conf",
	  # Substitute "/opt/alien" with $ALIEN_ROOT in highperformance.conf
	#  "'s/\\\/opt\\\/alien/$ALIEN_ROOT_TMP/' $ALIEN_ROOT/httpd/conf/highperformance.conf",
	  # Substitute "Listen 80" with Listen "11983" in highperformance.conf
	 # "'s/Listen 80/Listen 11983/' $ALIEN_ROOT/httpd/conf/highperformance.conf",
	  # Substitute "User alienmaster" with the username of current user in httpd.conf
      "'s/User alienmaster/User $USER/' $ALIEN_TESTDIR/httpd.conf",
       );

    foreach (@seed)
    {
        print  ("sed -i -e $_ \n");
        if (system ("sed -i -e $_ ")) 
        {
            print "Error checking the status of: sed -i -e $_ \n";
            exit(-1);
        }
    }

#
# prepare SSL and mod_perl specific stuff in httpd.conf


    my $dispatch_to = `find $ALIEN_ROOT -follow -name LBSG.pm 2>/dev/null`;
    chomp ($dispatch_to);
    $dispatch_to = dirname(dirname(dirname ( $dispatch_to)));

   
    my $addHTTPD="

ServerName $hostname.$domain

SSLengine on 
SSLSessionCache dbm:$ALIEN_ROOT/httpd/logs/ssl_gcache_data 
SSLCertificateFile     $HOME/.alien/globus/usercert.pem 
SSLCertificateKeyFile  $HOME/.alien/globus/userkey.pem 
SSLVerifyClient require 
SSLVerifyDepth  10 
SSLOptions +StdEnvVars 
SSLCACertificatePath $ALIEN_ROOT/globus/share/certificates/ 
\<Location \/\> 
    SSLRequireSSL
    SetHandler perl-script 
    PerlHandler AliEn::Service 
    PerlSetVar dispatch_to \"$dispatch_to AliEn::Service::LBSG \" 
    PerlSetVar options \"compress_threshold => 10000\" 
    PerlOptions +SetupEnv 
    Allow from all 
    GridSiteGSIProxyLimit 2 
\<\/Location\> 
PerlSwitches -I$ALIEN_ROOT/lib/perl5 -I$ALIEN_ROOT/lib/perl5/site_perl
PerlModule Apache2::compat  
PerlConfigRequire $ALIEN_ROOT/httpd/conf/startup.pl ";

    if (open (INFD, "$ALIEN_TESTDIR/httpd.conf") )
    {
        if (open (OUTFD, ">/tmp/httpd.conf_$$"))
        {
	        print OUTFD <INFD>;	
        	close INFD;
	
        	print OUTFD $addHTTPD;
        	close OUTFD;
        }
        else 
        {
	        print "Error! Failed to open /tmp/httpd.conf_$$ \n";	
	        close INFD;
	        exit (-1);
        }
    }
    else 
    {
        print "Error ! Failed to open httpd.conf \n";
        exit (-1);
    }


    if (system ("mv /tmp/httpd.conf_$$ $ALIEN_ROOT/httpd/conf/httpd.conf"))
    {
        print "Error checking the status of: mv /tmp/httpd.conf_$$ $ALIEN_ROOT/httpd/conf/httpd.conf \n";
        exit (-1);
    }
    
    if (system ("mkdir -p $ALIEN_ROOT/httpd/logs"))
    {
        print "Error checking the status of: mkdir -p $ALIEN_ROOT/httpd/logs \n";
        exit (-1);
    }

#
# startup.pl creation section              

    #get ALIEN ORGANISATION
    my $alienOrg = `grep -m 1 ALIEN_ORGANISATION $ALIEN_HOME/Environment`;
    chop $alienOrg;
    (undef, $alienOrg) = split (/=\s*/, $alienOrg);

    #get LDAP DN
    my $alienLdapDn = `grep -m 1 ALIEN_LDAP_DN $ALIEN_HOME/Environment`;
    chop $alienLdapDn;
    (undef, $alienLdapDn) = split (/ALIEN_LDAP_DN\s*=\s*/, $alienLdapDn);



    if (!open (INFD, "$ALIEN_TESTDIR/startup.pl"))
    { 
        print "Error ! Failed to open startup.pl \n";
        exit (-1);
    }

    if (!open (OUTFD, ">/tmp/startup.pl_$$"))
    {
        print "Failed to open /tmp/startup.pl_$$";
        close INFD;
        exit (-1);
    }
	
    while ( <INFD> )
    {
        print OUTFD;
        if ($_ =~ m/\[TEST_MARK\]/)
        {
	        print OUTFD "\# Here come essential env vars \n\n";
	
        	print OUTFD "\$ENV{ALIEN_ROOT}=\"$ALIEN_ROOT\"\; \n";
	        print OUTFD "\$ENV{ALIEN_HOME}=\"$ALIEN_HOME\"\; \n";
        	print OUTFD "\$ENV{ALIEN_ORGANISATION}=\"$alienOrg\"\; \n";
	        print OUTFD "\$ENV{ALIEN_LDAP_DN}=\"$alienLdapDn\"\; \n";		
      }
    }

    close INFD;
    close OUTFD;

    if (system ("mv /tmp/startup.pl_$$ $ALIEN_ROOT/httpd/conf/startup.pl"))
    {
        print "Error checking the status of: mv /tmp/startup.pl_$$ $ALIEN_ROOT/httpd/conf/startup.pl \n";
        exit (-1);
    }
}
