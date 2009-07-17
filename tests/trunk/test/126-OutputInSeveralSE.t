#!/bin/env useren-perl

use strict;
use Test;

use AliEn::UI::Catalogue::LCM::Computer;

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




