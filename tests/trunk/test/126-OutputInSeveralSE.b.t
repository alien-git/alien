#!/bin/env useren-perl

use strict;
use Test;
use POSIX;


use AliEn::UI::Catalogue::LCM::Computer;
use AliEn::Config;



BEGIN { plan tests => 1 }

{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/userenmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("16-add") or exit(-2);
  includeTest("126-OutputInSeveralSE") or exit(-2);
  #  eval `cat $ENV{ALIEN_TESTDIR}/FillTableFor126-OutputInSeveralSE.pl`;

  my $outputstring=(shift || exit(-2));

  print "OutputString is $outputstring.\n";
  ((!$outputstring) or ($outputstring eq "")) and exit(-2);

  my ($testTable, $run_test)=fillTestTableWithTests();

  my @results=split(/,/, $outputstring);

  
  my @testcases=();
 
  for my $testline (@results) {
       my ($tcase, $tid) = split(/::/, $testline);
       ((!$tcase) or (!$tcase ne "") or (!$testTable->{$tcase})) and exit(-2);
       (!(isdigit $tid)) and exit(-2);
       
       $testTable->{$tcase}->{id} = $tid;
       push @testcases, $tcase;
       
       print "Test $tcase has id: $testTable->{$tcase}->{id}\n";
  }
 

  print "Tests finished, starting to verify output.\n";
  my $fileTable;

  for my $tcase (@testcases) {
        if($run_test->{$tcase}) {
           
            cleanUpTestJDL($tcase);

            print "Test $tcase has id: $testTable->{$tcase}->{id}\n";

                $fileTable->{$tcase}->{listing} = doLsOnJobsOutputDir($testTable->{$tcase}->{id});
                for my $filename (@{$fileTable->{$tcase}->{listing}}) {

                   print "checking file $filename";
                   ($fileTable->{$tcase}->{$filename}->{copies}, $fileTable->{$tcase}->{$filename}->{ses},
                                  $fileTable->{$tcase}->{$filename}->{guid})
                                  =getSEsAndGuidForAJobsOutputFile($testTable->{$tcase}->{id},$filename);

                   (!grep(/^no_se$/, @{$fileTable->{$tcase}->{$filename}->{ses}})) and
                   $fileTable->{$tcase}->{$fileTable->{$tcase}->{$filename}->{guid}}=$filename;

                   $fileTable->{$tcase}->{$filename}->{filename}=$filename;
                }    
        }
  }




  print "#########################################\n";
  print "ALL TESTS FINISHED. RESULTS BELOW:\n";
  print "#########################################\n";
  
  my $TotalTestStatus=1;
  
  my $defaultCopies=2;

  for my $tcase (@testcases) {
    
       my $archiveStatus=0;
       my $archiveContentStatus=0;
       my $fileStatus=0;
       my $defaultFileStatus=0;

       $testTable->{$tcase}->{status}=0;
       ($run_test->{$tcase} and ($testTable->{$tcase}->{id} ne 0))or next;

       #print "Test $tcase is active, getting verification info.\n";
       #print "OK, Test $tcase, has id: $testTable->{$tcase}->{id}\n";
       my @archivefiles=();
       (scalar(@{$testTable->{$tcase}->{archivecontent}}) > 0) and
              @archivefiles=@{$testTable->{$tcase}->{archivecontent}}; 


       my @filefiles=();
       (scalar(@{$testTable->{$tcase}->{filetag}}) > 0) and
              @filefiles=@{$testTable->{$tcase}->{filetag}}; 
       my $filePointer = \@filefiles;

       my @archives=();
       doEqualsOnStrings($testTable->{$tcase}->{archivename},"") and
              push @archives, $testTable->{$tcase}->{archivename};


       for my $filename (@{$fileTable->{$tcase}->{listing}}) {
         my $copyCount = 0;
         if(doEqualsOnArray($filename,\@archivefiles)){
             (isdigit $testTable->{$tcase}->{asel}) and $copyCount += $testTable->{$tcase}->{asel};
             (isdigit $testTable->{$tcase}->{adisk}) and $copyCount += $testTable->{$tcase}->{adisk};
             (isdigit $testTable->{$tcase}->{atape}) and $copyCount += $testTable->{$tcase}->{atape};
             ($testTable->{$tcase}->{asel} eq 0) and $copyCount += scalar(@{$testTable->{$tcase}->{ases}});
         }

         if(doEqualsOnArray($filename,\@filefiles)){
             (isdigit $testTable->{$tcase}->{fsel}) and $copyCount += $testTable->{$tcase}->{fsel};
             (isdigit $testTable->{$tcase}->{fdisk}) and $copyCount += $testTable->{$tcase}->{fdisk};
             (isdigit $testTable->{$tcase}->{ftape}) and $copyCount += $testTable->{$tcase}->{ftape};
             ($testTable->{$tcase}->{fsel} eq 0) and $copyCount += scalar(@{$testTable->{$tcase}->{fses}});
         } 


         ($copyCount < 1) and  $copyCount = setUnspecifiedCopyCount(); #When there wasn't specified anything



         if(doEqualsOnArray($filename,\@archivefiles)) {
           if(doEqualsOnArray("no_se",\@{$fileTable->{$tcase}->{$filename}->{ses}})) {
               if(doEqualsOnStrings($fileTable->{$tcase}->{$fileTable->{$tcase}->{$filename}->{guid}}, $testTable->{$tcase}->{archivename})) {
                    if(doEqualsOnStrings($copyCount, $fileTable->{$tcase}->{$testTable->{$tcase}->{archivename}}->{copies})) {
                       for my $j(0..$#archivefiles) {
                               doEqualsOnStrings($archivefiles[$j], $filename) and splice(@archivefiles,$j,1);
                       }   
                    } 
               }
           }
         }


         if(doEqualsOnArray($filename,\@filefiles)) {
             if(doEqualsOnArray("no_archive",\@{$testTable->{$tcase}->{fopt}})){
                 if(!doEqualsOnArray("no_se",\@{$fileTable->{$tcase}->{$filename}->{ses}})){
                     if(doEqualsOnStrings($copyCount, $fileTable->{$tcase}->{$filename}->{copies})) {
                         for my $j(0..$#filefiles) {
                                doEqualsOnStrings($filefiles[$j], $filename) and splice(@filefiles,$j,1);
                         } 
                     }
                 }
             } else {
                 if(doEqualsOnArray("no_se",\@{$fileTable->{$tcase}->{$filename}->{ses}})) {
                     my $archivename = $fileTable->{$tcase}->{$fileTable->{$tcase}->{$filename}->{guid}};
                     if(doEqualsOnArray($archivename,\@archives)) {
                           if(doEqualsOnStrings($copyCount, $fileTable->{$tcase}->{$testTable->{$tcase}->{archivename}}->{copies})) {
                              for my $j(0..$#filefiles) {
                                   doEqualsOnStrings($filefiles[$j], $filename) and splice(@filefiles,$j,1);
                              } 
                           } 
                     } else {
                           if(doEqualsOnStrings($copyCount, $fileTable->{$tcase}->{$fileTable->{$tcase}->{$fileTable->{$tcase}->{$filename}->{guid}}}->{copies})) {
                             for my $j(0..$#filefiles) {
                                     doEqualsOnStrings($filefiles[$j], $filename) and splice(@filefiles,$j,1);
                             } 
                           } 
                     }
                 }
             }
         }

         if(doEqualsOnArray($filename,\@archives)) {
             if(doEqualsOnStrings($copyCount, $fileTable->{$tcase}->{$filename}->{copies})) {
                      for my $j(0..$#archives) {
                                 doEqualsOnStrings($archives[$j], $filename) and splice(@archives,$j,1);;
                      } 
             } 
         }
 
       
       }


       #### Testing the no_link_registration case
       my $archoffset  = 0;
       my $fileoffset  = 0;

       if(doEqualsOnArray("no_links_registration",\@{$testTable->{$tcase}->{aopt}} )) {
          my @archivefilesCopy = @archivefiles;
          for my $j(0..$#archivefilesCopy) {
             if(doEqualsOnArray($archivefilesCopy[$j],\@{$testTable->{$tcase}->{archivecontent}} )) {
                        splice(@archivefiles,$j-$archoffset,1);
                        $archoffset++;
             }
          }
       } 
       if(doEqualsOnArray("no_links_registration",\@{$testTable->{$tcase}->{fopt}} )) {
          my @filefilesCopy = @filefiles;
          for my $j(0..$#filefilesCopy) {
             if(doEqualsOnArray($filefilesCopy[$j],\@{$testTable->{$tcase}->{filetag}} )) {
                        splice(@filefiles,$j-$fileoffset,1);
                        $fileoffset++;
             } 
          }
       }

  
       ($#archivefiles < 0) and $archiveContentStatus=1;
       ($#archives < 0) and $archiveStatus=1;
       ($#filefiles < 0) and $fileStatus=1;


       if(scalar(@{$fileTable->{$tcase}->{listing}}) < 1){
          print "#########################################\n";
          print "FATAL ERROR: ls on $testTable->{$tcase}->{id}/job-out was empty, job didn't store any files.\n";
          print "#########################################\n";

          $archiveContentStatus=0;
          $archiveStatus=0;
          $fileStatus=0;
       }
  

       $testTable->{$tcase}->{status} = $archiveStatus && $archiveContentStatus && $fileStatus;
       $TotalTestStatus = $TotalTestStatus && $testTable->{$tcase}->{status}; 

  print "#########################################\n";
  print "Test result of $tcase with id: $testTable->{$tcase}->{id}\n";
  statusPrint("   ArchiveOutput",$archiveStatus);   
  $archiveStatus or print "Failed with archives: @archives\n";

  statusPrint("   ArchiveContentOutput",$archiveContentStatus);   
  $archiveContentStatus or print "Failed with archivefiles: @archivefiles\n";

  statusPrint("   FileOutput",$fileStatus);   
  $fileStatus or print "Failed with filefiles: @filefiles\n";

  statusPrint("   TEST FINAL STATUS",$testTable->{$tcase}->{status});
  

  } 

  print "#########################################\n";
  print "#########################################\n";
  



  print "#########################################\n";
  print "#########################################\n";
  print "#########################################\n";
  statusPrint("ALL TESTS FINAL STATUS",$TotalTestStatus); 


  $TotalTestStatus or exit(-2);

  print "ok\n";

}




sub statusPrint{
my $testname=shift;
my $status=shift;

print $testname."... ";



   if($status){
      print "OK\n";
   }else{
      print "FAILED\n"; 
#   return $status;
   }
}






sub getSEsAndGuidForAJobsOutputFile{
  my $id=shift;
  my $filename=shift;
  my $table;

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  my(@outputarch)=$cat->execute("whereis","/proc/newuser/$id/job-output/$filename") or exit(-2);

  my @senames=();
  my $guid=0;

  if(grep(/no_se/, @outputarch)) {
     @senames=("no_se");
     for my $entry (@outputarch) {
         my @entries=();
         if ($entry =~ /\//) {
            print "entry is: $entry\n";
            @entries =(split (/\//, $entry));
            $entry=$entries[$#entries];
            print "entry is: $entry\n";
            @entries=split (/\?ZIP=/, $entry);
            $guid=uc($entries[0]);
            
         }
      }
   } else {
       for my $entry (@outputarch) {
         if(! ($entry =~ /\//)) {
             push @senames, $entry;
         }
       }
       my(@outguid)=$cat->execute("lfn2guid","/proc/newuser/$id/job-output/$filename") or exit(-2);
       $guid=$outguid[0];
   }

  $cat->close();
  return (scalar(@senames),\@senames,$guid);
}

sub doEqualsOnArray{
  my $pattern=shift;
  my $anArray=shift;

  $anArray or return 0;
  $pattern or return 0;

  ($pattern ne "") or return 0;
  ($#$anArray >= 0) or return 0;


  for my $el (@$anArray) {
     $el or next;
     ($el ne "") or next;
     ($el =~ $pattern) and return 1;
  }
  return 0;
}

sub doEqualsOnStrings{
  my $stringOne=shift;
  my $stringTwo=shift;

  ($stringOne and $stringTwo) or return 0;

  ($stringOne eq $stringTwo) and return 1;
  return 0;

}


sub spliceCertainArrayElement{
  my $pattern=shift;
  my $ArrayPointer=shift;

  my @myArray = $$ArrayPointer;
  
  for my $j(0..$#myArray) {
     doEqualsOnStrings($myArray[$j], $pattern) and splice(@myArray,$j,1);
  }
  return \@myArray;
}




sub checkGuidForLFN{
  my $id=shift;
  my $filename=shift;

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  my(@outputarch)=$cat->execute("lfn2guid","/proc/newuser/$id/job-output/$filename") or exit(-2);
  $cat->close();
 
  return \@outputarch;
}


sub doLsOnJobsOutputDir{
  my $id=shift;

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  my @listing = $cat->execute("ls","/proc/newuser/$id/job-output/");
  $cat->close();
  return \@listing; 
}



sub  setUnspecifiedCopyCount{

     my $ldap = new AliEn::Config();
     if ($ldap->{SEDEFAULT_QOSAND_COUNT}) {  # check if LDAP knows sth about this
         my ($repltag, $replcopies)=split (/\=/, $ldap->{SEDEFAULT_QOSAND_COUNT},2);
         return $replcopies;
     }

     return  1; 
}




sub cleanUpTestJDL{
  my $case_name=shift;

  my $cat=AliEn::UI::Catalogue::LCM::Computer->new({"user", "newuser",});
  my $stat =$cat->execute("rm", "jdl/date_${case_name}.jdl");
  $cat->close();
  return $stat; 
}



