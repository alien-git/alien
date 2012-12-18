#!/usr/bin/perl -w

#use diagnostics;
use Getopt::Long ();
use strict;


my $name=(shift or "");
if (( not $name)||  ( $name=~/^::/)){ 
  $name="AliEn::UI::Catalogue$name";
} elsif($name eq 'virtual') {
  $name="AliEn::UI::Catalogue";
}


my $options = {
	       'user'       => $ENV{ALIEN_USER},    
	       'debug'      => 0,
	       'exec'       => "",
	       'token'      => "",
	       'password'   => "",
	       'silent'     => "",
	       'role'       => "",
	       'AuthMethod' => "",
	       'domain'     => "",
	       'organisation'=>"",
	       'gasModules' => $ENV{ALIEN_GAS_MODULES},
	       'no_catalog' => "",
	       'packman_method'=> "",
	       'file'=>"",
	      };


Getopt::Long::GetOptions(
			 $options,  "help",           "silent",     "user=s",
			 "exec=s",  "token=s",        "password=s", "role=s",
			 "debug=s", "ForcedMethod=s", "domain=s", "organisation=s","file=s",
       "gasModules=s", "no_catalog", "packman_method=s","queue=s",
			)

or exit(-3);

#print "ARGUMENTS @ARGV\n";
if ($options->{exec}){
  $options->{exec} = join (" ", ($options->{exec}, @ARGV));
}else {
  if (@ARGV) {
    print STDERR "Error: \'@ARGV\' not understood \n";
    exit(-2);
  }
}


#print "Got $options->{exec}\n";
#print "Got $options->{no_catalog}\n";

eval "require $name"
  or print STDERR "Error requiring $name\n$@\nDoes the service $name exist?\n"
  and exit(-2);

my $base = new $name($options);
if (!($base)){
  $options->{exec} or exit (-1);
  #if there was a problem during the exec, we have to see it:
  use AliEn::Logger;

  my $error=$AliEn::Logger::ERROR_NO;
  exit $error;
}

if ($options->{file}){
  print "Let's execute the commands in the file $options->{file}\n";
  open (FILE, "<$options->{file}") or
    print "Error opening the file $options->{file}\n" and exit(-2);
  foreach my $line (<FILE>){
    chomp $line;
    $line =~ /^\s*$/ and next;
    $line =~ /^\s*#/ and next;
    print "Executing '$line' ...\n";
    $base->execute(split(/ /, $line));
  }
  close FILE;
  exit;
}
$base->startPrompt;


