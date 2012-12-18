#!/opt/alien/bin/perl
use strict;
use warnings;
use Test;
use Net::LDAP;
use AliEn::Config;
use Net::Domain;

my $ALIEN_ROOT=$ENV{ALIEN_ROOT};
my $ALIEN_HOME=$ENV{HOME}."/.alien";

BEGIN { plan tests => 2}
{
   # get rootdn
   my $rootdn = `grep -m 1 \"^rootdn\" $ALIEN_ROOT/etc/openldap/slapd.conf`;
   chomp $rootdn; 
   $rootdn =~ s/^\s*rootdn\s*//;
   $rootdn =~ s/\"//g;

   #get LDAP password
   my $hostname =Net::Domain::hostname();
   chomp $hostname;
   my $pass = `cat $ALIEN_HOME/.startup/.ldap.secret.$hostname`;

   #get LDAP dn 
   my $alienLdapDn = `grep -m 1 ALIEN_LDAP_DN $ALIEN_HOME/Environment`;
   chomp $alienLdapDn;
   (undef, $alienLdapDn) = split (/ALIEN_LDAP_DN\s*=\s*/, $alienLdapDn);

   my $alienOrg = `grep -m 1 ALIEN_ORGANISATION $ALIEN_HOME/Environment`;
   chomp $alienOrg;
   (undef, $alienOrg) = split (/=\s*/, $alienOrg);

   $ENV{ALIEN_ORGANISATION}=$alienOrg;
   $ENV{ALIEN_LDAP_DN}=$alienLdapDn;

   my $config = AliEn::Config->new();
   my $base = $config->{LDAPDN};

   #connect to LDAP
   my $ldap = Net::LDAP->new("$hostname:8389", "onerror" => "warn") or 
         print "failed\nError conecting to the ldap server\n $?and $! and  $@\n" and exit(-1);
 

   my $result=$ldap->bind($rootdn, "password" => "$pass");
   $result->code && print "failed\nCould not bind to LDAP-Server (\n DN: $rootdn )\n: ",$result->error and exit(-1);

   my @user_acc;  
   my @role_acc;
   my @site_acc;
 

   #find all users and put them to the list  
	   my $filter = "(objectclass=AliEnUser)";
	   my $mesg = $ldap->search ( 
                      		   	 base   => "ou=People,"."$base",
   		                      	 filter => "$filter"
	 			                );
	   my $entry;
	   my $uid;
	   my $i;

	   for ( $i = 0; $i < $mesg->count; $i++)
	   {
	    	$entry = $mesg->entry($i);
		    $uid =  $entry->get_value('uid');
		    push (@user_acc, "uid=$uid".","."ou=People,"."$base");
       }

   #find all roles and put them in the list  
	    $filter = "(objectclass=AliEnRole)";
        $mesg = $ldap->search ( 
	                    	   	base   => "ou=Roles,"."$base",
   			                    filter => "$filter"
	 			              );

	   for ( $i = 0; $i < $mesg->count; $i++)
	   {
	       $entry = $mesg->entry($i);	
		   $uid =  $entry->get_value('uid');		
		   push (@role_acc, "uid=$uid".","."ou=Roles,"."$base");
       }

   #find all sites and put them in the list  
	    $filter = "(objectclass=AliEnSite)";
	    $mesg = $ldap->search ( 
                    		   	base   => "ou=Sites,"."$base",
   			                    filter => "$filter"
	 			              );

	    for ( $i = 0; $i < $mesg->count; $i++)
	    {
		   $entry = $mesg->entry($i);	
		   $uid =  $entry->get_value('ou');		
		   push (@site_acc, "ou=$uid".","."ou=Sites,"."$base");
        }
  
	  my $dn;

   # Put accounts to LDAP 
   #

	  # fill in bank accounts for user 
	  foreach $dn (@user_acc)
      {
	      print $dn, "\n";
	      $ldap->modify($dn, replace => {'accountName' => 'user_acc'});
	  }

	  # fill in bank accounts for roles 
	  foreach $dn (@role_acc)
      {
	      print $dn, "\n";
	      $ldap->modify($dn, replace => {'accountName' => 'role_a1cc'});
	  }
	 
      # fill in bank accounts for sites 
	  foreach $dn (@site_acc){
	      print $dn, "\n";
	      $ldap->modify($dn, replace => {'accountName' => 'site_acc'});
	  }

	  my $subject="";
	  my $file="$ALIEN_HOME"."/globus/usercert.pem";

   # put alienmaster as bank admin 
      $ldap->modify("ou=Config,"."$base", replace => {'bankAdmin' => $ENV{USER} });

      # get the subject of alienmaster certificate  
      if (-f $file) 
      {
	    if (open( TEMP, "openssl x509 -noout -in $file -subject|"))
        {
		      $subject=<TEMP>;
              $subject=~ s/^subject=\s+//;
		      chomp $subject;
			  close(TEMP);
	    }
	  }	
       
       # put the subject of alienmaster's CERT to LDAP
       $ldap->modify("uid=alienmaster,ou=People,"."$base", replace => {'subject' => $subject});           

#restart apache        
system ("env LD_LIBRARY_PATH=$ALIEN_ROOT/httpd/lib:$ENV{LD_LIBRARY_PATH} $ALIEN_ROOT/httpd/bin/httpd -f $ALIEN_ROOT/httpd/conf/httpd.conf -k restart");
ok(1);

}
