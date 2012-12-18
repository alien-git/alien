use strict;
use Authen::PAM;
use POSIX;

my   $VFpasswd = "testPass";
my   $VFusername = "newuser";
# ***************************************************************
# Conversation function for PAM
# ***************************************************************
my $my_conv_func = sub {
    my @res;

    while (@_) {
        my $code = shift;
        my $msg  = shift;
        my $ans  = "";

        $ans = $VFusername if ( $code == PAM_PROMPT_ECHO_ON() );
        $ans = $VFpasswd   if ( $code == PAM_PROMPT_ECHO_OFF() );

        push @res, ( PAM_SUCCESS(), $ans );
    }
    push @res, PAM_SUCCESS();
    return @res;
};
my $tty_name = ttyname( fileno(STDIN) );

print  "Checking password of $VFusername\n" ;


print "Before PAM init\n";

my   $pamh = new Authen::PAM( "login", $VFusername, \&$my_conv_func );
print "After PAM init\n" ;
my   $res = $pamh->pam_set_item( PAM_TTY(), $tty_name );
  $res = $pamh->pam_authenticate();

if ($res) {
  print "User passwd is not correct!\n";
  exit(-1);
  }
print "User passwd is correct\n" ;
exit(0);
