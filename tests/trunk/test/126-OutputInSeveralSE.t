#!/bin/env useren-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;
use Net::Domain;

BEGIN { plan tests => 1 }

{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/userenmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("16-add") or exit(-2);

  eval `cat $ENV{ALIEN_TESTDIR}/FillTableFor126-OutputInSeveralSE.pl`;



  my ($testTable, $run_test)=fillTestTableWithTests();

  my @archivename=();
  my @archivecontent=();
  my @filetag=();
  my @ses = ();
  my $secount;
  my @options = ();
  my @ids = ();
  my $statustemp;
  my $archivetag = "";
  my $filetag = "";



  print "Tests loaded into table.\n";

  my $testrun = "Tests ";

  for my $tcase (keys(%$testTable)) {

        if($run_test->{$tcase}) {
                $testrun .= $tcase.", ";
  

                print "Test $tcase is about to be started.\n";
                ($testTable->{$tcase}->{status}, $testTable->{$tcase}->{id}) 
                       = submitCopiesOnMultipleSes($testTable->{$tcase}->{archivename},$testTable->{$tcase}->{archivecontent},
                       $testTable->{$tcase}->{ases},$testTable->{$tcase}->{asec}, $testTable->{$tcase}->{aopt},
                       $testTable->{$tcase}->{filetag},
                       $testTable->{$tcase}->{fses},$testTable->{$tcase}->{fsec}, $testTable->{$tcase}->{fopt});
          
          
       }
  }

print " were run and are finished now.";

my $outputstring="";

for my $tcase (keys(%$testTable)) {
    ($run_test->{$tcase}) and
    $outputstring.=$tcase."::".$testTable->{$tcase}->{id}.",";
}

$outputstring=~ s/,$//;


print "Job submitted!! 
\#ALIEN_OUTPUT $outputstring\n"

}


sub buildSEOptionString{

  my $ses=shift;
  my $secount=shift;
  my $options=shift;
    
  my $sestring = '@';
  foreach (@$ses){
     if( $_ ne "" ){
         $sestring .= "$_,";
     }
  }
  
  if ($secount ne "") {
      $sestring .= "copies=$secount,";
  }
  
  
  foreach (@$options){
     if($_ ne ""){
        $sestring .= "$_,";
     }
  }
  $sestring =~ s/,$//;
  $sestring =~ s/\@$//;
  
  return $sestring;

}

sub submitCopiesOnMultipleSes{
  
  my $archivename=shift;
  my $archivecontent=shift;
  my $ases=shift;
  my $asecount=shift;
  my $aoptions=shift;
  my $filetag=shift;
  my $fses=shift;
  my $fsecount=shift;
  my $foptions=shift;
  
  my $asestring=buildSEOptionString($ases,$asecount,$aoptions);
  my $fsestring=buildSEOptionString($fses,$fsecount,$foptions);
  
  
  
  my $jdlstring = "Executable=\"date\";\n";


  if ($archivename ne "" ) {
     $jdlstring .= "OutputArchive = (\"";
     $jdlstring .= $archivename.":";
     for my $file (@$archivecontent) {
	    $jdlstring .= $file.",";
     }
     $jdlstring =~ s/,$//;
     $jdlstring .= $asestring;
     $jdlstring .= "\");\n";
  }

  if (scalar(@$filetag) > 0) {
     $jdlstring .= "OutputFile = (\"";
     for my $file (@$filetag) {
            $jdlstring .= $file.",";
     }
     $jdlstring =~ s/,$//;
     $jdlstring .= $fsestring;
     $jdlstring .= "\");\n";
  }



  print "JDL STRING IS:".$jdlstring."\n";
  


  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  #my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});


  $cat or exit (-1);

  $cat->execute("cd");
  $cat->execute("cd","jdl");
  $cat->execute("rm","date.jdl");

  $cat->execute("cd") or exit (-2);
  #my ($list)= $cat->execute("ls","-la") or exit(-2);
  #print "ls says: $list";  
  $cat->execute("pwd") or exit (-2);
  $cat->execute("mkdir", "-p", "jdl") or exit(-2);

  addFile($cat, "jdl/date.jdl",$jdlstring) or exit(-2);
  my ($id)=$cat->execute("submit", "jdl/date.jdl");
  $id or exit(-2); ;
  $cat->close();
  #my $id=$out->{id}; 
  print "job id is: $id";


   return (1, $id);
}


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
  
  my $vo=Net::Domain::hostname();
  $testTable->{noJDL}={archivename=>$archivename1,archivecontent=>\@archivecontent1,ases=>\@ases1,asec=>$asec1,aopt=>\@aoptions1,
                         filetag=>\@filetag1,fses=>\@fses1,fsec=>$fsec1,fopt=>\@foptions1,status=>0,id=>0,seres=>0,secres=>0};



  my $archivename2="SomeThing.zip";
  my @archivecontent2=("stderr","resources");
  
  my @ases2 = ("${vo}::CERN::TESTSE","${vo}::CERN::TESTSE2","${vo}::CERN::TESTSE3");
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
  my @fses3 = ("${vo}::CERN::TESTSE","${vo}::CERN::TESTSE2");
  my $fsec3 = 3;
  my @foptions3 = ();

  $testTable->{a2f1}={archivename=>$archivename3,archivecontent=>\@archivecontent3,ases=>\@ases3,asec=>$asec3,aopt=>\@aoptions3,
                         filetag=>\@filetag3,fses=>\@fses3,fsec=>$fsec3,fopt=>\@foptions3,status=>0,id=>0,seres=>0,secres=>0};


  my $archivename4="AZipArchive.zip";
  my @archivecontent4=("stderr","stdout","resources");
  my @ases4 = ("${vo}::CERN::TESTSE","${vo}::CERN::TESTSE2","${vo}::CERN::TESTSE3");
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
  my @ases5 = ("${vo}::CERN::TESTSE","${vo}::CERN::TESTSE2","${vo}::CERN::TESTSE3");
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
  my @ases6 = ("${vo}::CERN::TESTSE","${vo}::CERN::TESTSE2","${vo}::CERN::TESTSE3");
  my $asec6 = 3;
  my @aoptions6 = ("custodial=1");
  my @filetag6=("stdout");
  my @fses6 = ("${vo}::CERN::TESTSE2");
  my $fsec6 = 2;
  my @foptions6 = ();

  $testTable->{std}={archivename=>$archivename6,archivecontent=>\@archivecontent6,ases=>\@ases6,asec=>$asec6,aopt=>\@aoptions6,
                         filetag=>\@filetag6,fses=>\@fses6,fsec=>$fsec6,fopt=>\@foptions6,status=>0,id=>0,seres=>0,secres=>0};









return ($testTable, $run_test);

}


