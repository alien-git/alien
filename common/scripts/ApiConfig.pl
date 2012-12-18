use AliEn::Config;


my $config = AliEn::Config->new();
$config or print "error [ApiConfig]: cannot get the configuration from LDAP\n" and exit -1;

system("mkdir -p $ENV{'ALIEN_ROOT'}/api/etc/config 2>&1 > /dev/null");

if ( -e "$ENV{'ALIEN_ROOT'}/api/etc/config/gapiservice.cfg.xml" ) { 
    # save the old configuration file
    print "ApiConfig: Saving old configuration file to $ENV{'ALIEN_ROOT'}/api/etc/config/gapiservice.cfg.xml~\n";
    system("mv $ENV{'ALIEN_ROOT'}/api/etc/config/gapiservice.cfg.xml $ENV{'ALIEN_ROOT'}/api/etc/config/gapiservice.cfg.xml~");
}

if ( defined $config->{APISERVICE_USELOCALCONFIG} && $config->{APISERVICE_USELOCALCONFIG} eq "1") {
    print "ApiConfig: Using local configuration file $ENV{'ALIEN_ROOT'}/api/etc/config/gapiservice.localcfg.xml\n";
    if (!( -e "$ENV{'ALIEN_ROOT'}/api/etc/config/gapiservice.localcfg.xml")) {
	print "error [ApiConfig]: local configuration file $ENV{'ALIEN_ROOT'}/api/etc/config/gapiservice.localcfg.xml is not existing\n";
	exit (-2);
    }
    system("cp  $ENV{'ALIEN_ROOT'}/api/etc/config/gapiservice.localcfg.xml $ENV{'ALIEN_ROOT'}/api/etc/config/gapiservice.cfg.xml");
} else {
    print "ApiConfig: Writing configuration file $ENV{'ALIEN_ROOT'}/api/etc/config/gapiservice.cfg.xml\n";
    # write a new XML configuration file
    open APC, "> $ENV{'ALIEN_ROOT'}/api/etc/config/gapiservice.cfg.xml";

    my $conf_authfile = "noauth";
    if ( defined $config->{APISERVICE_AUTHORIZATIONFILE} && ($config->{APISERVICE_AUTHORIZATIONFILE} ne "" ) ) {
	$conf_authfile = $config->{APISERVICE_AUTHORIZATIONFILE};
    }

    my $conf_user = "admin";
    if ( defined $config->{APISERVICE_USER} && ($config->{APISERVICE_USER} ne "" ) ) {
	$conf_user = $config->{APISERVICE_USER};
    }

    my $conf_role = "admin";
    if ( defined $config->{APISERVICE_ROLE} && ($config->{APISERVICE_ROLE} ne "" ) ) {
	$conf_role = $config->{APISERVICE_ROLE};
    }
    
    my $conf_debug = "0";
    if ( defined $config->{APISERVICE_DEBUG} && ($config->{APISERVICE_DEBUG} ne "" ) ) {
	$conf_debug = $config->{APISERVICE_DEBUG};
    }

    my $conf_commandlogging = "";
    if ( defined $config->{APISERVICE_COMMANDLOGGING} && ($config->{APISERVICE_COMMANDLOGGING} ne "" ) ) {
	$conf_commandlogging = $config->{APISERVICE_COMMANDLOGGING};
    }

    my $conf_performancelogging = "0";
    if ( defined $config->{APISERVICE_PERFORMANCELOGGING} && ($config->{APISERVICE_PERFORMANCELOGGING} ne "" ) ) {
	$conf_performancelogging = $config->{APISERVICE_PERFORMANCELOGGING};
    }
    print APC <<EOF
<?xml version="1.0" encoding="UTF-8" ?>
<service name="gapiservice">
  <components>
    <component name="gapiservice">
      <config>
        <param name="port">                 <value>$config->{APISERVICE_PORT}</value></param>
        <param name="prefork">              <value>$config->{APISERVICE_PREFORK}</value></param>
        <param name="perlmodule">           <value>"$ENV{'ALIEN_ROOT'}/api/scripts/gapiserver/$config->{APISERVICE_PERLMODULE}"</value></param>
        <param name="sessionlifetime">      <value>$config->{APISERVICE_SESSIONLIFETIME}</value></param>
        <param name="nodaemon">             <value>1</value></param>
        <param name="ssl_port">             <value>$config->{APISERVICE_SSLPORT}</value></param>
        <param name="ssl_keydir">           <value>""</value></param>
EOF
;

    if (defined $config->{'APISERVICE_SSLKEYFILE'}) {
	print APC "<param name=\"ssl_keyfile\">>          <value>\"$config->{'APISERVICE_SSLKEYFILE'}\"</value></param\n";
    }

    if (defined $config->{'APISERVICE_SSLDHFILE'}) {
	print APC "<param name=\"ssl_dhfile\">>          <value>\"$config->{'APISERVICE_SSLDHFILE'}\"</value></param\n";
    }

    if (defined $config->{'APISERVICE_SSLCERTDIR'}) {
	print APC "<param name=\"ssl_certdir\">>          <value>\"$config->{'APISERVICE_SSLCERTDIR'}\"</value></param\n";
    }

    print APC <<EOF
        <param name="authorization_file">       <value>"$conf_authfile"</value></param>
        <param name="user">                 <value>"$conf_user"</value></param>
        <param name="role">                 <value>"$conf_role"</value></param>
        <param name="debug">                <value>$conf_debug</value></param>
EOF
;
    if ($conf_commandlogging ne "") {
	print APC "        <param name=\"commandlogging\">       <value>1</value></param>\n";
	print APC "        <param name=\"commandlogfile\">       <value>$config->{'LOG_DIR'}/ApiCommands.log</value></param>\n";
    } else {
	print APC "        <param name=\"commandlogging\">       <value>0</value></param>\n";
    }
    print APC <<EOF
        <param name="performancelogging">   <value>$conf_performancelogging</value></param>
        <param name="filteredcommands">
           <arrayvalue>"aioget"</arrayvalue>
           <arrayvalue>"aioput"</arrayvalue>
           <arrayvalue>"aioless"</arrayvalue>
           <arrayvalue>"aioedit"</arrayvalue>
           <arrayvalue>"get"</arrayvalue>
           <arrayvalue>"add"</arrayvalue>
           <arrayvalue>"quit"</arrayvalue>
           <arrayvalue>"exit"</arrayvalue>
           <arrayvalue>"user"</arrayvalue>
EOF
; 
    if (defined $config->{'APISERVICE_FILTEREDCOMMANDS_LIST'} ){
	foreach (@{$config->{'APISERVICE_FILTEREDCOMMANDS_LIST'}}) {
	    print APC "           <arrayvalue>\"".$_."\"</arrayvalue>\n";
	}
    }

    print APC<<EOF
        </param>
        <param name="environment">
           <arrayvalue>SEALED_ENVELOPE_LOCAL_PRIVATE_KEY="$config->{'APISERVICE_LOCALPRIVKEYFILE'}"</arrayvalue>
           <arrayvalue>SEALED_ENVELOPE_LOCAL_PUBLIC_KEY="$config->{'APISERVICE_LOCALPUBKEYFILE'}"</arrayvalue>
           <arrayvalue>SEALED_ENVELOPE_REMOTE_PRIVATE_KEY="$config->{'APISERVICE_REMOTEPRIVKEYFILE'}"</arrayvalue>
           <arrayvalue>SEALED_ENVELOPE_REMOTE_PUBLIC_KEY="$config->{'APISERVICE_REMOTEPUBKEYFILE'}"</arrayvalue>
EOF
;
    if (defined $config->{'APISERVICE_ENVIRONMENT_LIST'} ){
	foreach (@{$config->{'APISERVICE_ENVIRONMENT_LIST'}}) {
	    print APC "           <arrayvalue>\"".$_."\"</arrayvalue>\n";
	}
    }
    print APC <<EOF
        </param>
      </config>
    </component>
  </components>
</service>
EOF
;
}
exit 0;



