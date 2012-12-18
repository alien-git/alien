#!/bin/env alien-perl

use strict;
use Test;

BEGIN { plan tests => 1 }

use AliEn::UI::Catalogue;
use AliEn::UI::Catalogue::LCM::Computer;

{
    print "Getting an instance of the catalogue";
    my $cat_ad =AliEn::UI::Catalogue->new({"role", "admin"});
    $cat_ad or exit(-2);

    my $user = "DUser";
    $cat_ad->execute("addUser", $user);
  
    my $cat = AliEn::UI::Catalogue::LCM::Computer->new({"user", $user});
    $cat or exit(-1);
    
    my ($pwd) = $cat->execute("pwd") or exit(-2);
    $cat->execute("cd") or exit(-2);

    #############################################################################################
    print "\n0. Lets start with checking the status of the tables using the di <l> command\n ";
    my @done1=$cat->execute("di","l");
    my @expected = ('69','1','1','736','3','1');
    my $done = checkINDEX($cat,\@done1,\@expected );
    $done or print "Error in the file catalogue tables (Unexpected number of entries)  ..\n" and exit(-2);
    print "\n0. Done\n";
    ##########################################################################################
		print "1. Doing the populate directory \n ";
    ($done)=$cat->execute("silent", 1);
    ($done)=$cat->execute("populate", "diTest","1","2","3","4","5","6");
    $done or print "Error populating the directory diTest\n" and exit(-2);
    ($done)=$cat->execute("silent", 0);
    print "\n1. Done";
    ##########################################################################################
    print "\n2. Now, lets check the status of the tables using the di <l> command\n ";
    @done1=$cat->execute("di","l");
    @expected = ('69','1','1','736','3','875');
    $done = checkINDEX($cat,\@done1,\@expected );
    $done or print "Error populating the file catalogues .. entries have not been made in the L#L tables ..\n" and exit(-2);
    print "\n2. Done";
    #############################################################################################
    ($done)=$cat->execute("silent", 1);
    my $max_lim=100;
    my $min_lim=0;
    print "\n3. Now, its time to optimize the tables for a good distributed ones for the directory diTest.";
    print "\nUsing the di <optimize_dir> <max_lim> <min_lim> <dir_name> command\n ";
    print "\n\tdi optimize_dir $max_lim $min_lim diTest\n ";
    $done=$cat_ad->execute("cd","../../D/DUser/");
    $done=$cat_ad->execute("di","optimize_dir",$max_lim,$min_lim,"diTest");
#here the code for checking the tables will appear
    @done1=$cat->execute("di","l");
    @expected = ('69','1','1','736','3','11','145','145','145','145','145','145');
    $done = checkINDEX($cat,\@done1,\@expected );
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
    $min_lim=5000;
    print "\n5. Now, optimize the tables with different <max_lim> <min_lim> \n ";
    print "\n\tdi optimize_dir $max_lim $min_lim diTest\n ";
    $done=$cat_ad->execute("di","optimize_dir",$max_lim,$min_lim,"diTest");
#here the code for checking the tables will appear
    @done1=$cat->execute("di","l");
    @expected = ('942','1','1','736','3');
    $done = checkINDEX($cat,\@done1,\@expected );
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

sub checkINDEX {
  my $d     = shift;
  my $values1 = shift;
  my @values = @$values1;
  my $expected1 = shift;
  my @expected = @$expected1;

  for(my $i=0;$i<@values/2; $i++)
  {
    ($expected[$i] eq $values[$i+(@values/2)]) or print "FAILED\n" and return;
  }
  return 1;
}
