

use strict;
use Test;


sub fillTestTableWithTests{

  my $run_test;
  $run_test->{noJDL}  =1;
  $run_test->{noarchivec2s2}   =1;
  $run_test->{a2f1}   =1;
  $run_test->{nolink} =1;
  $run_test->{fnolink} =1;
  $run_test->{std} =1;





  my $testTable;

  my $archivename1="";
  my @archivecontent1=();
  my @ases1 = ();
  my $asec1 = "";
  my @aoptions1 = ();
  my @filetag1=();
  my @fses1 = ();
  my $fsec1 = "";
  my @foptions1 = ();

  $testTable->{noJDL}={archivename=>$archivename1,archivecontent=>\@archivecontent1,ases=>\@ases1,asec=>$asec1,aopt=>\@aoptions1,
                         filetag=>\@filetag1,fses=>\@fses1,fsec=>$fsec1,fopt=>\@foptions1,status=>0,id=>0,seres=>0,secres=>0};



  my $archivename2="SomeThing.zip";
  my @archivecontent2=("stderr","resources");
  my @ases2 = ("pcepalice10::CERN::TESTSE","pcepalice10::CERN::TESTSE2","pcepalice10::CERN::TESTSE3");
  my $asec2 = 2;
  my @aoptions2 = ();
  my @filetag2=("stdout");
  my @fses2 = ();
  my $fsec2 = 3;
  my @foptions2 = ("no_archive");

  $testTable->{noarchivec2s2}={archivename=>$archivename2,archivecontent=>\@archivecontent2,ases=>\@ases2,asec=>$asec2,aopt=>\@aoptions2,
                         filetag=>\@filetag2,fses=>\@fses2,fsec=>$fsec2,fopt=>\@foptions2,status=>0,id=>0,seres=>0,secres=>0};



  my $archivename3="";
  my @archivecontent3=();
  my @ases3 = ();
  my $asec3 = "";
  my @aoptions3 = ();
  my @filetag3=("stderr","resources","stdout");
  my @fses3 = ("pcepalice10::CERN::TESTSE","pcepalice10::CERN::TESTSE2");
  my $fsec3 = 3;
  my @foptions3 = ();

  $testTable->{a2f1}={archivename=>$archivename3,archivecontent=>\@archivecontent3,ases=>\@ases3,asec=>$asec3,aopt=>\@aoptions3,
                         filetag=>\@filetag3,fses=>\@fses3,fsec=>$fsec3,fopt=>\@foptions3,status=>0,id=>0,seres=>0,secres=>0};


  my $archivename4="AZipArchive.zip";
  my @archivecontent4=("stderr","stdout","resources");
  my @ases4 = ("pcepalice10::CERN::TESTSE","pcepalice10::CERN::TESTSE2","pcepalice10::CERN::TESTSE3");
  my $asec4 = 2;
  my @aoptions4 = ("no_link_registration");
  my @filetag4=();
  my @fses4 = ();
  my $fsec4 = "";
  my @foptions4 = ();

  $testTable->{nolink}={archivename=>$archivename4,archivecontent=>\@archivecontent4,ases=>\@ases4,asec=>$asec4,aopt=>\@aoptions4,
                         filetag=>\@filetag4,fses=>\@fses4,fsec=>$fsec4,fopt=>\@foptions4,status=>0,id=>0,seres=>0,secres=>0};


  my $archivename5="someName.zip";
  my @archivecontent5=("stderr","stdout");
  my @ases5 = ("pcepalice10::CERN::TESTSE","pcepalice10::CERN::TESTSE2","pcepalice10::CERN::TESTSE3");
  my $asec5 = 2;
  my @aoptions5 = ();
  my @filetag5=("resources");
  my @fses5 = ();
  my $fsec5 = 3;
  my @foptions5 = ("no_link_registration");

  $testTable->{fnolink}={archivename=>$archivename5,archivecontent=>\@archivecontent5,ases=>\@ases5,asec=>$asec5,aopt=>\@aoptions5,
                         filetag=>\@filetag5,fses=>\@fses5,fsec=>$fsec5,fopt=>\@foptions5,status=>0,id=>0,seres=>0,secres=>0};


  my $archivename6="someOtherName.zip";
  my @archivecontent6=("stderr","resources");
  my @ases6 = ("pcepalice10::CERN::TESTSE","pcepalice10::CERN::TESTSE2","pcepalice10::CERN::TESTSE3");
  my $asec6 = 3;
  my @aoptions6 = ("custodial=1");
  my @filetag6=("stdout");
  my @fses6 = ("pcepalice10::CERN::TESTSE2");
  my $fsec6 = 2;
  my @foptions6 = ();

  $testTable->{std}={archivename=>$archivename6,archivecontent=>\@archivecontent6,ases=>\@ases6,asec=>$asec6,aopt=>\@aoptions6,
                         filetag=>\@filetag6,fses=>\@fses6,fsec=>$fsec6,fopt=>\@foptions6,status=>0,id=>0,seres=>0,secres=>0};









return ($testTable, $run_test);

}


return 1;
