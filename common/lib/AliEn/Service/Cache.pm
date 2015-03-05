package AliEn::Service::Cache;


use strict;
use AliEn::Util;


$ENV{SEALED_ENVELOPE_REMOTE_PUBLIC_KEY}="$ENV{ALIEN_HOME}/authen/rpub.pem";
$ENV{SEALED_ENVELOPE_REMOTE_PRIVATE_KEY}="$ENV{ALIEN_HOME}/authen/rpriv.pem";
$ENV{SEALED_ENVELOPE_LOCAL_PUBLIC_KEY}="$ENV{ALIEN_HOME}/authen/lpub.pem";
$ENV{SEALED_ENVELOPE_LOCAL_PRIVATE_KEY}="$ENV{ALIEN_HOME}/authen/lpriv.pem";
#$ENV{ALIEN_DATABASE_PASSWORD}='pass';
$ENV{ALIEN_DATABASE_ROLE}="admin";
$ENV{ALIEN_DATABASE_PASSWORD}='XI)B^nF7Ft';

use AliEn::UI::Catalogue::LCM;

  use warnings;
  use Data::Dumper;
  use Apache2::RequestRec ();
  use Apache2::RequestIO ();

# my $l=AliEn::Logger->new();

#$l->infoToSTDERR();
my $cat;
use Apache2::Const -compile => qw(OK);
  
  sub handler {
      my $r = shift; 

      if (!$cat) {
        $cat=AliEn::UI::Catalogue::LCM->new({silent=>1});
        $cat->execute("silent");
      }
      my $args=$r->args();

      my ($h)= $args =~ m/&?args=([^&]*)/;
      $h =~ s/%3C/</g;
      $h =~ s/%3E/>/g;
      my @args2= split ('%20', $h);

      my ($op) = $args =~ m/&?op=([^&]*)/;

      if ($op !~ /(whereis)|(find)/ ){
        print "We can only do the operations whereis and find\n";
         return Apache2::Const::OK;   
      }

      $r->content_type('text/plain');
      my $cacheName=join("_", $op, @args2);
      my $info=AliEn::Util::returnCacheValue( $cat, $cacheName);
      if ($info){
        print STDERR "$$ RETURNING FROM THE CACHE\n";
      } else{
        print STDERR "$$ ASKING THE CATALOGUE '@args2'\n";
        $cat->{LOGGER}->keepAllMessages();
        my @files= $cat->execute($op, @args2);
        my @loglist = @{$cat->{LOGGER}->getMessages()};
        $info=Dumper([join("", @loglist), @files]);
        AliEn::Util::setCacheValue($cat, $cacheName,$info);
      }
      print $info;
      return Apache2::Const::OK;
  }



return 1;
