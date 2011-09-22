#!/bin/env alien-perl

use strict;
use Test;

BEGIN { plan tests => 1 }

use AliEn::UI::Catalogue;

{
    print "Getting an instance of the catalogue";
    my $cat =AliEn::UI::Catalogue->new({"role", "admin"});
    $cat or exit(-2);

    ##########################################################################################
		print "1. Doing the populate directory \n ";
    my ($done)=$cat->execute("silent", 1);
    ($done)=$cat->execute("populate", "diTest","1","2","3","4","5","6");
    $done or print "Error populating the directory diTest\n" and exit(-2);
    ($done)=$cat->execute("silent", 0);
    print "\n1. Done";
    ##########################################################################################
    print "\n2. Now, lets check the status of the tables using the di <l> command\n ";
    $done=$cat->execute("di","l");
    $done or print "Error doing the di command\n" and exit(-2);
    print "\n2. Done";
    #############################################################################################
    ($done)=$cat->execute("silent", 1);
    my $max_lim=100;
    my $min_lim=0;
    print "\n3. Now, its time to optimize the tables for a good distributed ones for the directory diTest.";
    print "\nUsing the di <optimize_dir> <max_lim> <min_lim> <dir_name> command\n ";
    print "\n\tdi optimize_dir $max_lim $min_lim diTest\n ";
    $done=$cat->execute("di","optimize_dir",$max_lim,$min_lim,"diTest");
#here the code for checking the tables will appear
    $done or print "Error trying to optimize using the given parameters... :( :( \n" and exit(-2);
    ($done)=$cat->execute("silent", 0);
    print "\n3. Done";
    #############################################################################################
    print "\n4. Now, lets recheck the status of the tables using the di <l> command\n ";
    $done=$cat->execute("di","l");
    $done or print "Error doing the di command\n" and exit(-2);
    print "\n4. Done";
    ##############################################################################################
    ($done)=$cat->execute("silent", 1);
    $max_lim=10000;
    $min_lim=10000;
    print "\n5. Now, optimize the tables with different <max_lim> <min_lim> \n ";
    print "\n\tdi optimize_dir $max_lim $min_lim diTest\n ";
    $done=$cat->execute("di","optimize_dir",$max_lim,$min_lim,"diTest");
#here the code for checking the tables will appear
    $done or print "Error trying to optimize using the given parameters... :( :( \n" and exit(-2);
    ($done)=$cat->execute("silent", 0);
    print "\n5. Done";
    ##########################################################################################
    print "\n6. Now, lets recheck the status of the tables using the di <l> command\n ";
    $done=$cat->execute("di","l");
    $done or print "Error doing the di command\n" and exit(-2);
    print "\n6. Done";
    #############################################################################################
    print "\n7. Finally, lets remove the directory we populated i.e. diTest\n ";
    $done=$cat->execute("rmdir","diTest");
    $done or print "Error doing the rmdir command\n" and exit(-2);
    print "\n7. Done";
    #############################################################################################
		print "\nok\n";
		$cat->close();
    ok(1);
}

=cut1
sub checkINDEX {
  my $d     = shift;
  my $user  = shift;
  my $field = shift;
  my $value = shift;

  my $result = 0;
  $result = $d->queryValue("SELECT $field FROM FQUOTAS WHERE user='$user'");
  (defined $result) or print "Error checking the $field of the user\n" and exit(-2);
  ($result eq $value) or print "FAILED: $field expected:<$value> but was: $result\n";
  return ($result eq $value);
}
=cut
