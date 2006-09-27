#!/bin/env alien-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;
use AliEn::SOAP;

BEGIN { plan tests => 1}
{
my $ALIEN_ROOT=$ENV{ALIEN_ROOT};
my $HOME=$ENV{HOME};
my $ALIEN_HOME=$ENV{HOME}."/.alien";

$ALIEN_ROOT or die "ALIEN_ROOT not set !";

#
# prepare SSL specific stuff for httpd.conf
#

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
    PerlSetVar dispatch_to \"$ALIEN_ROOT/lib/perl5/site_perl/5.8.7/ AliEn::Service::LBSG \" 
    PerlSetVar options \"compress_threshold => 10000\" 
    PerlOptions +SetupEnv 
    Allow from all 
    GridSiteGSIProxyLimit 2 
\<\/Location\> 
PerlModule Apache2::compat  
PerlConfigRequire $ALIEN_ROOT/httpd/conf/startup.pl ";

	open (INFD, "httpd.conf");
	open (OUTFD, ">/tmp/httpd.conf_$$");
        my $conf;
		while ( $conf = <INFD>)
		{
  		print OUTFD $conf;
		}
	
	close (INFD);

	print OUTFD $addHTTPD;
	close OUTFD;

`mv /tmp/httpd.conf_$$ $ALIEN_ROOT/httpd/conf/httpd.conf`;
`mkdir -p /opt/alien/httpd/logs`;

#
# put SSL stuff to my.cfg 
#

 open (INFD, "my.cnf");
        open (OUTFD, ">/tmp/my.cnf_$$");
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

`mv /tmp/my.cnf_$$ $ALIEN_ROOT/etc/my.cnf`;

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

 open (INFD, "startup.pl");
	open (OUTFD, ">/tmp/startup.pl_$$");
        while ( <INFD> )
	{
	print OUTFD;
	if ($_ =~ m/\[TEST_MARK\]/){
		print OUTFD "\# Here come essential env vars \n\n";

		print OUTFD "\$ENV{ALIEN_ROOT}=\"$ALIEN_ROOT\"\; \n";
                print OUTFD "\$ENV{ALIEN_HOME}=\"$ALIEN_HOME\"\; \n";
		print OUTFD "\$ENV{ALIEN_ORGANISATION}=\"$alienOrg\"\; \n";
                print OUTFD "\$ENV{ALIEN_LDAP_DN}=\"$alienLdapDn\"\; \n";		
		
		}
	}
       
       close INFD;
       close OUTFD;	

`mv /tmp/startup.pl_$$ $ALIEN_ROOT/httpd/conf/startup.pl`;

          # restart mysql
          system(" $ALIEN_ROOT/etc/rc.d/init.d/alien-mysqld stop ");
          sleep(5);
          system(" $ALIEN_ROOT/etc/rc.d/init.d/alien-mysqld start ");
          sleep(5);
          system(" $ALIEN_ROOT/etc/rc.d/init.d/alien-mysqld status ");

          #restart LBSG (httpd)
          system("pkill httpd");
          sleep(5);
          system("$ALIEN_ROOT/httpd/bin/httpd");
          sleep(2);
          system("ps -ef|grep httpd");
	  


ok(1);

}
