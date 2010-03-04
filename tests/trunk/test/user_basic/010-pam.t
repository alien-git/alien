use strict;
use POSIX qw(ttyname);
use Authen::PAM;

# *******************************
# Needed for PAM authentification
my $VFusername = "";
my $VFpasswd   = "";
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

my $tty_name = (ttyname( fileno(STDIN) ) || "");

$VFpasswd = "testPass";
print "Before PAM init and $tty_name \n" ;
$VFusername = "newuser";
my  $pamh = new Authen::PAM( "login", $VFusername, \&$my_conv_func );
print  "After PAM init and $pamh\n" ;
my $res="";
 $res = $pamh->pam_set_item( PAM_TTY(), $tty_name );
print "AFTER SET ITEM\n";
$res = $pamh->pam_authenticate();
$pamh=0;
print "DONE AND $res\n";
$res and print "Error: the user was not authenticated\n" and exit -2;

print "Let's try now with the wrong password\n";
$VFpasswd = "testPass2";
$pamh = new Authen::PAM( "login", $VFusername, \&$my_conv_func );
print  "After PAM init and $pamh\n" ;
 $res = $pamh->pam_set_item( PAM_TTY(), $tty_name );
print "AFTER SET ITEM\n";
$res = $pamh->pam_authenticate();
$pamh=0;
print "DONE AND $res\n";
$res or print "Error: the user was authenticated\n" and exit -2;
print "ok!!\n";
