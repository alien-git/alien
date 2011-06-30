package AliEn::UI::Catalogue::LCM::Computer::Proof;

use AliEn::UI::Catalogue::LCM::Computer;

@ISA = qw( AliEn::UI::Catalogue::LCM::Computer );

use strict;

my (%command_list);

%command_list = (
  'proofSetMasterPwd' => [ '$self->proofSetMasterPwd', 0 ],
  'proofSetClientPwd' => [ '$self->proofSetClientPwd', 0 ],
  'proofStartDaemon'  => [ '$self->proofStartDaemon',  0 ],
);

my %help_list = (
  'masterpwd' => "\tProof Service configuration and communicatione",

);

sub initialize {
  my $self    = shift;
  my $options = shift;

  $self->{SOAP} = new AliEn::SOAP;

  $self->SUPER::initialize($options) or return;

  $options->{CATALOG} = $self;

  $self->AddCommands(%command_list);
  $self->AddHelp(%help_list);
}

# check, that core commands are executed only on the VO core service machine for Proof
sub proofCheckCoreHost {
  my $self = shift;
  my $response =
    SOAP::Lite->uri("AliEn/Service/IS")->proxy("http://$self->{CONFIG}->{IS_HOST}:$self->{CONFIG}->{IS_PORT}")
    ->getAllServices("Services");

  if (!($response)) {
    printf STDERR "Error asking IS ...\n";
    return;
  }

  $response = $response->result;
  my @hosts;
  my @ports;
  my @names;

  @hosts = split ":",   $response->{HOSTS};
  @names = split "###", $response->{NAMES};

  my $cnt = 0;
  foreach (@hosts) {
    if ($names[$cnt] eq 'Proof') {
      my $thishost = $ENV{'ALIEN_HOSTNAME'} . "." . $ENV{'ALIEN_DOMAIN'};
      chomp $thishost;
      if ($thishost eq $_) {
        return 1;
      }
    }
    $cnt++;
  }
  print STDERR "This command can only be run on the Proof Service node!\n";
  return;
}

# set's the proof Master password, which allows Proof Masters running under
# the Proof Service to connect to the proodf, accessed via the TcpRouter service

sub proofSetMasterPwd {
  my $self = shift;
  my $word;

  $self->proofCheckCoreHost() or return;

  print "Creating Master Password for Proof Master Server ..\n";

  system "stty -echo";
  print "Password: ";
  chomp($word = <STDIN>);
  print "\n";
  system "stty echo";
  my $pw = crypt("$word", "salt");
  print "Crypted Password is: $pw\n";
  print "Please update the LDAP Server Entry!\n";

  my $response =
    SOAP::Lite->uri("AliEn/Service/IS")->proxy("http://$self->{CONFIG}->{IS_HOST}:$self->{CONFIG}->{IS_PORT}")
    ->getAllServices("TcpRouter");

  if (!($response)) {
    printf STDERR "Error asking IS ...\n";
    return;
  }

  $response = $response->result;

  open(PNETRC, "> $ENV{'HOME'}/.rootnetrc");
  chmod 0600, "$ENV{'HOME'}/.rootnetrc";
  my @hosts;
  my @names;
  my %i;
  my @uniq;
  my $cnt = -1;

  @hosts = split ":",   $response->{HOSTS};
  @names = split "###", $response->{NAMES};
  for (@hosts) {
    $cnt++;
    my $site = $names[$cnt];
    $site =~ s/::SUBSYS//g;
    unless ($i{$_}++) {

      # machine node1.cern.ch login rdm password secretpasswd
      my $response = $self->{SOAP}->CallSOAP("IS", "getServiceUser", "TcpRouter", "$site");

      $self->{SOAP}->checkSOAPreturn($response, "IS")
        or $self->{LOGGER}->error("Proof", "IS could not find the Service User of the TcpRouter for $site\n")
        and next;
      $response = $response->result;
      $response
        or $self->{LOGGER}->error("Proof", "IS could not find the Service User of the TcpRouter for $site\n")
        and next;
      push(@uniq, "machine $_ login $response->{'USER'} password $word\n");
    }
  }

  print PNETRC @uniq;
  close PNETRC;

  if (!(-d "$ENV{'HOME'}/.alien/proofd")) {
    if (!(mkdir "$self->{HOME}/.alien/proofd/", 0700)) {
      print "Cannot create $self->{HOME}/.alien/proofd directory!\n";
      return;
    }
  }

  if (-d "$ENV{'HOME'}/.alien/proofd") {
    open(PAPCP, "> $ENV{'HOME'}/.alien/proofd/master.plain.pwd");
    open(PAPCC, "> $ENV{'HOME'}/.alien/proofd/master.crypt.pwd");
    chmod 0600, "$ENV{'HOME'}/.alien/proofd/master.plain.pwd";
    chmod 0600, "$ENV{'HOME'}/.alien/proofd/master.crypt.pwd";
    print PAPCP "$word";
    print PAPCC "$pw";
    close PAPCP;
    close PAPCC;
    print "Proof Master Password successfully updated!\n";
  }
}

# sets the password, which is used by alien clients to connect to the proof master
# server

sub proofSetClientPwd {
  my $self = shift;
  my $word;

  $self->proofCheckCoreHost() or return;

  print "Creating Clients Password for the Proof Master Server ..\n";

  system "stty -echo";
  print "Password: ";
  chomp($word = <STDIN>);
  print "\n";
  system "stty echo";
  my $pw = crypt("$word", "salt");

  open(RDPWD, "> $ENV{'HOME'}/.rootdpass");
  chmod 0600, "$ENV{'HOME'}/.rootdpass";
  print RDPWD "$pw";
  close RDPWD;

  if (!(-d "$ENV{'HOME'}/.alien/proofd")) {
    if (!(mkdir "$self->{HOME}/.alien/proofd/", 0700)) {
      print "Cannot create $self->{HOME}/.alien/proofd directory!\n";
      return;
    }
  }

  if (-d "$ENV{'HOME'}/.alien/proofd") {
    open(PAPCP, "> $ENV{'HOME'}/.alien/proofd/client.plain.pwd");
    open(PAPCC, "> $ENV{'HOME'}/.alien/proofd/client.crypt.pwd");
    chmod 0600, "$ENV{'HOME'}/.alien/proofd/client.plain.pwd";
    chmod 0600, "$ENV{'HOME'}/.alien/proofd/client.crypt.pwd";
    print PAPCP "$word";
    print PAPCC "$pw";
    close PAPCP;
    close PAPCC;
    print "Proof Clients Password successfully updated!\n";
  }
}

sub proofStartDaemon {
  my $self    = shift;
  my $site    = shift or print STDERR "You have to provide a site, where you want to start the daemon\n";
  my $howmany = (shift or 1);
  my $cnt     = 1;
  for $cnt (1 .. $howmany) {
    my $randomnumber = rand;
    my $thistime     = time;
    my $localjdlfile = "/tmp/proofd.jdl.$randomnumber.$thistime";
    open(TMPJDL, ">$localjdlfile");
    print TMPJDL "Executable = \"proofd\";\n";
    print TMPJDL "Packages = \"ROOT::3.10.01\";\n";
    print TMPJDL "Interactive = \"1\";\n";
    print TMPJDL "Arguments = \"Command::PROOFD\";\n";
    print TMPJDL "Requirements = ( other.CE == \"$site\" );\n";
    close(TMPJDL);

    if (!(-e "$localjdlfile")) {
      print STDERR "Cannot create temporary jdl file $localjdlfile!\n" and return;
    }

    my $oldsilent = $self->{CATALOG}->{SILENT};
    $self->{CATALOG}->{SILENT} = 1;
    my $newjobid = $self->{QUEUE}->submitCommand("< $localjdlfile");
    $self->{CATALOG}->{SILENT} = $oldsilent;
    if ($newjobid) {
      print "New Proofd request submitted:\n";
      printf "Site: %25s JobId: %d\n", $site, $newjobid;
    } else {
      print STDERR "New Proofd request failed!\n";
    }
  }
}

return 1;

