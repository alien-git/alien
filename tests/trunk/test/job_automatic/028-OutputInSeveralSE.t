#!/bin/env useren-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;
use Net::Domain;

BEGIN { plan tests => 1 }

{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/userenmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("catalogue/003-add") or exit(-2);

  my ($testTable, $run_test)=fillTestTableWithTests();

#  my @archivename=();
#  my @archivecontent=();
#  my @filetag=();
#  my @ses = ();
#  my $selcount;
#  my $diskcount;
#  my $tapecount;
#  my @options = ();
#  my @ids = ();
#  my $statustemp;
#  my $archivetag = "";
#  my $filetag = "";



  print "Tests loaded into table.\n";

  my $testrun = "Tests ";


  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});

  $cat or exit (-1);


  $cat->execute("cd") or exit (-2);
  $cat->execute("pwd") or exit (-2);
  $cat->execute("mkdir", "-p", "jdl") or exit(-2);

  for my $tcase (keys(%$testTable)) {

    if($run_test->{$tcase}) {
      $testrun .= $tcase.", ";
      print "Test $tcase is about to be started.\n";
      ($testTable->{$tcase}->{status}, $testTable->{$tcase}->{id}) 
	= submitCopiesOnMultipleSes($cat,$tcase,  $testTable->{$tcase});
    }
  }

  $cat->close();

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
  my $selcount=shift;
  my $diskcount=shift;
  my $tapecount=shift;
  my $options=shift;

  my $sestring = '@';
  foreach (@$ses){
     if( $_ ne "" ){
         $sestring .= "$_,";
     }
  }

  ($selcount ne 0) and $sestring .= "select=$selcount,";
  ($diskcount ne 0) and $sestring .= "disk=$diskcount,";
  ($tapecount ne 0) and $sestring .= "tape=$tapecount,";
  
  
  
  
  
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
  my $cat=shift;
  my $case_name=shift;
  my $entry=shift;

  my $archivename=  $entry->{archivename};
  my $archivecontent=$entry->{archivecontent};

  my $ases=$entry->{ases};
  my $aselcount=$entry->{asel};
  my $adiskcount=$entry->{adisk};
  my $atapecount=$entry->{atape};
  my $aoptions=$entry->{aopt};
  my $filetag=$entry->{filetag};
  my $fses=$entry->{fses};
  my $fselcount=$entry->{fsel};
  my $fdiskcount=$entry->{fdisk};
  my $ftapecount=$entry->{ftape};
  my $foptions=$entry->{fopt};

  my $asestring=buildSEOptionString($ases,$aselcount,$adiskcount,$atapecount,$aoptions);
  my $fsestring=buildSEOptionString($fses,$fselcount,$fdiskcount,$ftapecount,$foptions);



  my $jdlstring = "Executable=\"date\";\n";

  $jdlstring .= "Output={";

  if ($archivename ne "" ) {
     $jdlstring .= "\"";
     $jdlstring .= $archivename.":";
     for my $file (@$archivecontent) {
	    $jdlstring .= $file.",";
     }
     $jdlstring =~ s/,$//;
     $jdlstring .= $asestring;
     $jdlstring .= "\",";
  }

  if (scalar(@$filetag) > 0) {
     $jdlstring .= "\"";
     for my $file (@$filetag) {
            $jdlstring .= $file.",";
     }
     $jdlstring =~ s/,$//;
     $jdlstring .= $fsestring;
     $jdlstring .= "\",";
  }
  $jdlstring =~ s/,$//;
  $jdlstring .= "};\n";




  print "JDL STRING IS:".$jdlstring."\n";

  addFile($cat, "jdl/date_${case_name}.jdl",$jdlstring) or exit(-2);
  my ($id)=$cat->execute("submit", "jdl/date_${case_name}.jdl");
  $id or exit(-2); ;
  #my $id=$out->{id}; 
  print "job id is: $id";


   return (1, $id);
}


sub fillTestTableWithTests{

  my $run_test;

  $run_test->{noarchivec2s2}   =1;
  $run_test->{a2f1}   =1;
  $run_test->{nolink} =1;
  $run_test->{fnolink} =1;
  $run_test->{std} =1;
  $run_test->{simple} =1;






  my $testTable;

  $run_test->{noJDL}  =0;
  my $archivename1="";
  my @archivecontent1=("");
  my @ases1 = ();
  my $asel1 = 0;
  my $adisk1 = 0;
  my $atape1 = 0;
  my @aoptions1 = ();
  my @filetag1=("");
  my @fses1 = ();
  my $fsel1 = 0;
  my @foptions1 = ("");
  my $fdisk1 = 0;
  my $ftape1 = 0;
  my $vo=Net::Domain::hostname();
  $testTable->{noJDL}={archivename=>$archivename1,archivecontent=>\@archivecontent1,ases=>\@ases1,asel=>$asel1,adisk=>$adisk1,atape=>$atape1,aopt=>\@aoptions1,
                         filetag=>\@filetag1,fses=>\@fses1,fsel=>$fsel1,fdisk=>$fdisk1,ftape=>$ftape1,fopt=>\@foptions1,status=>0,id=>0,seres=>0,secres=>0};



  my $archivename2="SomeThing.zip";
  my @archivecontent2=("stderr","resources");
  
  my @ases2 = ("${vo}::CERN::TESTSE","${vo}::CERN::TESTSE2");
  my $asel2 = 2;
  my $adisk2 = 0;
  my $atape2 = 1;
  my @aoptions2 = ();
  my @filetag2=("stdout");
  my @fses2 = ();
  my $fsel2 = 0;
  my $fdisk2 = 2;
  my $ftape2 = 1;
  my @foptions2 = ();

  $testTable->{noarchivec2s2}={archivename=>$archivename2,archivecontent=>\@archivecontent2,ases=>\@ases2,asel=>$asel2,adisk=>$adisk2,atape=>$atape2,aopt=>\@aoptions2,
                         filetag=>\@filetag2,fses=>\@fses2,fsel=>$fsel2,fdisk=>$fdisk2,ftape=>$ftape2,fopt=>\@foptions2,status=>0,id=>0,seres=>0,secres=>0};



  my $archivename3="";
  my @archivecontent3=();
  my @ases3 = ();
  my $asel3 = 0;
  my $adisk3 = 2;
  my $atape3 = 1;
  my @aoptions3 = ();
  my @filetag3=("stderr","resources","stdout");
  my @fses3 = ("${vo}::CERN::TESTSE","${vo}::CERN::TESTSE2");
  my $fsel3 = 2;
  my $fdisk3 = 0;
  my $ftape3 = 0;
  my @foptions3 = ();

  $testTable->{a2f1}={archivename=>$archivename3,archivecontent=>\@archivecontent3,ases=>\@ases3,asel=>$asel3,adisk=>$adisk3,atape=>$atape3,aopt=>\@aoptions3,
                         filetag=>\@filetag3,fses=>\@fses3,fsel=>$fsel3,fdisk=>$fdisk3,ftape=>$ftape3,fopt=>\@foptions3,status=>0,id=>0,seres=>0,secres=>0};


  my $archivename4="AZipArchive.zip";
  my @archivecontent4=("stderr","stdout","resources");
  my @ases4 = ("${vo}::CERN::TESTSE","${vo}::CERN::TESTSE2","${vo}::CERN::TESTSE3");
  my $asel4 = 2;
  my $adisk4 = 0;
  my $atape4 = 0;
  my @aoptions4 = ("no_links_registration");
  my @filetag4=();
  my @fses4 = ();
  my $fsel4 = 0;
  my $fdisk4 = 0;
  my $ftape4 = 0;
  my @foptions4 = ();

  $testTable->{nolink}={archivename=>$archivename4,archivecontent=>\@archivecontent4,ases=>\@ases4,asel=>$asel4,adisk=>$adisk4,atape=>$atape4,aopt=>\@aoptions4,
                         filetag=>\@filetag4,fses=>\@fses4,fsel=>$fsel4,fdisk=>$fdisk4,ftape=>$ftape4,fopt=>\@foptions4,status=>0,id=>0,seres=>0,secres=>0};


  my $archivename5="someName.zip";
  my @archivecontent5=("stderr","stdout");
  my @ases5 = ("${vo}::CERN::TESTSE","${vo}::CERN::TESTSE2","${vo}::CERN::TESTSE3");
  my $asel5 = 3;
  my $adisk5 = 0;
  my $atape5 = 0;
  my @aoptions5 = ();
  my @filetag5=("resources");
  my @fses5 = ();
  my $fsel5 = 0;
  my $fdisk5 = 0;
  my $ftape5 = 1;
  my @foptions5 = ("no_links_registration");

  $testTable->{fnolink}={archivename=>$archivename5,archivecontent=>\@archivecontent5,ases=>\@ases5,asel=>$asel5,adisk=>$adisk5,atape=>$atape5,aopt=>\@aoptions5,
                         filetag=>\@filetag5,fses=>\@fses5,fsel=>$fsel5,fdisk=>$fdisk5,ftape=>$ftape5,fopt=>\@foptions5,status=>0,id=>0,seres=>0,secres=>0};


  my $archivename6="someOtherName.zip";
  my @archivecontent6=("stderr","resources");
  my @ases6 = ();
  my $asel6 = 0;
  my $adisk6 = 2;
  my $atape6 = 1;
  my @aoptions6 = ();
  my @filetag6=("stdout");
  my @fses6 = ("${vo}::CERN::TESTSE2");
  my $fsel6 = 1;
  my $fdisk6 = 0;
  my $ftape6 = 1;
  my @foptions6 = ();

  $testTable->{std}={archivename=>$archivename6,archivecontent=>\@archivecontent6,ases=>\@ases6,asel=>$asel6,adisk=>$adisk6,atape=>$atape6,aopt=>\@aoptions6,
                         filetag=>\@filetag6,fses=>\@fses6,fsel=>$fsel6,fdisk=>$fdisk6,ftape=>$ftape6,fopt=>\@foptions6,status=>0,id=>0,seres=>0,secres=>0};



  my $archivename7="myArchive";
  my @archivecontent7=("stdout");
  my @ases7 = ();
  my $asel7 = 0;
  my $adisk7 = 0;
  my $atape7 = 0;
  my @aoptions7 = ();
  my @filetag7=("stderr");
  my @fses7 = ();
  my $fsel7 = 0;
  my @foptions7 = ();
  my $fdisk7 = 0;
  my $ftape7 = 0;
  $testTable->{simple}={archivename=>$archivename7,archivecontent=>\@archivecontent7,ases=>\@ases7,asel=>$asel7,adisk=>$adisk7,atape=>$atape7,aopt=>\@aoptions7,
                         filetag=>\@filetag7,fses=>\@fses7,fsel=>$fsel7,fdisk=>$fdisk7,ftape=>$ftape7,fopt=>\@foptions7,status=>0,id=>0,seres=>0,secres=>0};








return ($testTable, $run_test);

}


