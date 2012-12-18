#!/bin/env alien-perl

use strict;
use Test;

BEGIN { plan tests => 1 }

use AliEn::UI::Catalogue;

{
    print "Getting an instance of the catalogue";
    my $cat =AliEn::UI::Catalogue->new({"role", "admin"});
    $cat or exit(-2);
    my $done;


    print "\n1. First things first .. Remove all expired entries from the G#L and G#L_PFN tables using removeExpiredFiles command\n ";
    $done=$cat->execute("removeExpiredFiles");
    $done or print "Error doing removeExpiredFiles\n" and exit(-2);
    print "\n1. Done";
    ##########################################################################################
    print "\n2. Now, lets first check the status of the G#L tables using the di <g> command\n ";
    $done=$cat->execute("di","g");
    $done or print "Error doing the di command\n" and exit(-2);
    print "\n2. Done";
    #############################################################################################
    ($done)=$cat->execute("silent", 1);
    my $max_lim=200;
    my $min_lim=0;
    print "\n3. Now, its time to optimize the tables for a good distributed ones";
    print "\nUsing the di <optimize_guid> <max_lim> <min_lim> command\n ";
    print "\n\tdi optimize_guid $max_lim $min_lim \n ";
    $done=$cat->execute("di","optimize_guid",$max_lim,$min_lim);
#here the code for checking the tables will appear
    my @done1=$cat->execute("di","g");
    my @expected = ('22','196','200','200');
    $done = checkINDEX($cat,\@done1,\@expected );
    $done or print "Error trying to optimize using the given parameters... :( :( \n" and exit(-2);
    ($done)=$cat->execute("silent", 0);
    print "\n3. Done";
    #############################################################################################
    print "\n4. Now, lets recheck the status of the tables using the di <g> command\n ";
    $done=$cat->execute("di","g");
    $done or print "Error doing the di command\n" and exit(-2);
    print "\n4. Done";
    ##############################################################################################
    ($done)=$cat->execute("silent", 1);
    $max_lim=10000;
    $min_lim=5000;
    print "\n5. Now, optimize the tables with different <max_lim> <min_lim> \n ";
    print "\n\tdi optimize_guid $max_lim $min_lim \n ";
    $done=$cat->execute("di","optimize_guid",$max_lim,$min_lim);
#here the code for checking the tables will appear
    @done1=$cat->execute("di","g");
    @expected = ('22','596');
    $done = checkINDEX($cat,\@done1,\@expected );
    $done or print "Error trying to optimize using the given parameters... :( :( \n" and exit(-2);
    ($done)=$cat->execute("silent", 0);
    print "\n5. Done";
    ##########################################################################################
    print "\n6. Now, lets recheck the status of the tables using the di <g> command\n ";
    $done=$cat->execute("di","g");
    $done or print "Error doing the di command\n" and exit(-2);
    print "\n6. Done";
    ##############################################################################################
    ($done)=$cat->execute("silent", 1);
    $max_lim=300;
    $min_lim=100;
    print "\n7. Now, optimize the tables with different <max_lim> <min_lim> \n ";
    print "\n\tdi optimize_guid $max_lim $min_lim \n ";
    $done=$cat->execute("di","optimize_guid",$max_lim,$min_lim);
#here the code for checking the tables will appear
    @done1=$cat->execute("di","g");
    @expected = {22,296,300};
    $done = checkINDEX($cat,\@done1,\@expected );
    $done or print "Error trying to optimize using the given parameters... :( :( \n" and exit(-2);
    ($done)=$cat->execute("silent", 0);
    print "\n7. Done";
    ##########################################################################################
    print "\n8. Now, lets recheck the status of the tables using the di <g> command\n ";
    $done=$cat->execute("di","g");
    $done or print "Error doing the di command\n" and exit(-2);
    print "\n8. Done";
    #############################################################################################
    print "\n9. Now, lets recheck the status of the tables using the di <g> command\n ";
    $done=$cat->execute("di","g");
    $done or print "Error doing the di command\n" and exit(-2);
    print "\n9. Done";
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
