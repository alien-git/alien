use strict;

use AliEn::UI::Catalogue;

print "First, let's create a proxy certificate\n";


system("alien proxy-init") and print "Error creating the proxy\n" and exit(-2);


my $vo=`hostname -s`;
chomp $vo;
my $dir="$ENV{HOME}/.alien/var/log/AliEn/$vo/";
my $file="/tmp/proxylog.$$";

my ($status, @content)=checkNoShared();
system("alien proxy-destroy");
$status and exit(0);
print "We have a problem: @content\n";

if (grep (/no shared cipher/, @content)){
  print "We have the no_shared_cipher problem :(\n";
  print "Let's try restarting the Proxy\n";
  system("alien StartProxy < $ENV{HOME}/.alien/.startup/.passwd.$vo");
  my ($status, @content)=checkNoShared();
  $status and print "At least, after restarting it seems to work...\n";
}

exit(-2);

sub checkNoShared {
  my $pid=open(FILE, "tail -n 0 -F $dir/ProxyServer.log  > $file |") 
  or print "Error checking the output of the ProxyServer\n" and return (0,undef);

  my $c=AliEn::UI::Catalogue->new({role=>"newuser"});
  sleep 2;
  open (PID, "ps -ef |grep ' $pid '|grep -v grep|");
  my @pid=<PID>;
  map { s/^\s*\S+\s*(\S+)\s.*$/$1/} @pid;

  close PID;
  kill 9, $pid, @pid;
  close FILE;

  if ($c) {
    print "We got the catalogue! :)\n";
    unlink $file;
    return 1;
  }
  open (FILE, "<$file") or print "Error opening the file $file\n";
  my @content=<FILE>;
  close FILE;
  unlink $file;
  return (0, @content);
}
