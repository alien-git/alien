#!/bin/env alien-perl

use strict;
use Test;

use File::Basename;
use AliEn::UI::Catalogue::LCM::Computer;
use AliEn::SOAP;

BEGIN { plan tests => 1}
{
my $ALIEN_ROOT=$ENV{ALIEN_ROOT};
my $HOME=$ENV{HOME};
my $ALIEN_HOME=$ENV{HOME}."/.alien";

$ALIEN_ROOT or die "ALIEN_ROOT not set !";


#
# Substitute "/opt/alien" with $ALIEN_ROOT in httpd.conf
#
my $ALIEN_ROOT_TMP = $ALIEN_ROOT;
   $ALIEN_ROOT_TMP =~ s/\//\\\//g;

print  ("sed -i -e 's/\\\/opt\\\/alien/$ALIEN_ROOT_TMP/' httpd.conf \n");
system ("sed -i -e 's/\\\/opt\\\/alien/$ALIEN_ROOT_TMP/' httpd.conf ");

# 
# Substitute "/opt/alien" with $ALIEN_ROOT in ssl.conf
#

print  ("sed -i -e 's/\\\/opt\\\/alien/$ALIEN_ROOT_TMP/' $ALIEN_ROOT/httpd/conf/ssl.conf \n");
system ("sed -i -e 's/\\\/opt\\\/alien/$ALIEN_ROOT_TMP/' $ALIEN_ROOT/httpd/conf/ssl.conf ");

#
# Substitute "/opt/alien" with $ALIEN_ROOT in highperformance.conf
#                                             

print  ("sed -i -e 's/\\\/opt\\\/alien/$ALIEN_ROOT_TMP/' $ALIEN_ROOT/httpd/conf/highperformance.conf \n");
system ("sed -i -e 's/\\\/opt\\\/alien/$ALIEN_ROOT_TMP/' $ALIEN_ROOT/httpd/conf/highperformance.conf ");

#
# Substitute "Listen 80" with Listen "11983" in highperformance.conf
#                                             

print  ("sed -i -e 's/Listen 80/Listen 11983/' $ALIEN_ROOT/httpd/conf/highperformance.conf \n");
system ("sed -i -e 's/Listen 80/Listen 11983/' $ALIEN_ROOT/httpd/conf/highperformance.conf ");

#
# prepare SSL specific stuff in httpd.conf
#

my $dispatch_to = `find $ALIEN_ROOT -name LBSG.pm`;
   chomp ($dispatch_to);
   $dispatch_to = dirname(dirname(dirname ( $dispatch_to)));

my $addHTTPD="
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
PerlModule Apache2::compat  
PerlConfigRequire $ALIEN_ROOT/httpd/conf/startup.pl ";

if (open (INFD, "httpd.conf") )
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
else  {
  print "Error ! Failed to open httpd.conf \n";
  exit (-1);
}


system ("mv /tmp/httpd.conf_$$ $ALIEN_ROOT/httpd/conf/httpd.conf");
system ("mkdir -p $ALIEN_ROOT/httpd/logs");

#
# put SSL stuff to my.cfg 
#

 if (open (INFD, "my.cnf")) 
 {
        if (open (OUTFD, ">/tmp/my.cnf_$$") )
	{
                while ( <INFD>)
                {
                print OUTFD;
		if ($_ =~ m/server_id/){
			 print OUTFD "\n";
                         print OUTFD "ssl-capath=$ALIEN_ROOT/gloubs/share/certificates \n"; 
                         print OUTFD "ssl-cert=$ALIEN_HOME/globus/usercert.pem \n";
                         print OUTFD "ssl-key=$ALIEN_HOME/globus/userkey.pem \n";                       
                 	}
                }
		close INFD;
	        close OUTFD;
	}
	else 
	{
		print "Error ! Failed to open /tmp/my.cnf_$$ \n";
		close INFD;
		exit (-1);
	}
  }
  else 
  {
  	print "Error ! Failed to open my.cnf \n";
	exit (-1);
  }
system("mv /tmp/my.cnf_$$ $ALIEN_ROOT/etc/my.cnf");

#
# recreate startup.pl
#

#get LDAP DN
my $alienLdapDn = `grep -m 1 ALIEN_LDAP_DN $ALIEN_HOME/Environment`;
chop $alienLdapDn;
(undef, $alienLdapDn) = split (/ALIEN_LDAP_DN\s*=\s*/, $alienLdapDn);

print "\n\n $alienLdapDn\n";

#get ALIEN ORGANISATION
my $alienOrg = `grep -m 1 ALIEN_ORGANISATION $ALIEN_HOME/Environment`;
chop $alienOrg;
(undef, $alienOrg) = split (/=\s*/, $alienOrg);

if (open (INFD, "startup.pl"))
  { 
    if (open (OUTFD, ">/tmp/startup.pl_$$"))
      {
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
      }
    else 
      {
	print "Failed to open /tmp/startup.pl_$$";
	close INFD;
	exit (-1);
      }
  }
else 
  {
    print "Error ! Failed to open startup.pl \n";
    exit (-1);
  }


system("mv /tmp/startup.pl_$$ $ALIEN_ROOT/httpd/conf/startup.pl");

# restart mysql
system(" $ALIEN_ROOT/etc/rc.d/init.d/alien-mysqld stop ");
sleep(5);
system(" $ALIEN_ROOT/etc/rc.d/init.d/alien-mysqld start ");
sleep(5);
system(" $ALIEN_ROOT/etc/rc.d/init.d/alien-mysqld status ");

#restart LBSG (httpd)
system("$ALIEN_ROOT/httpd/bin/httpd -k stop");
sleep(5);
system("$ALIEN_ROOT/httpd/bin/httpd");
sleep(2);
system("ps -ef|grep httpd");



ok(1);

}
