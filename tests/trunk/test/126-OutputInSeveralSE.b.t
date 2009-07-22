#!/bin/env useren-perl

use strict;
use Test;
use POSIX;


use AliEn::UI::Catalogue::LCM::Computer;

BEGIN { plan tests => 1 }

{
  $ENV{ALIEN_TESTDIR} or $ENV{ALIEN_TESTDIR}="/home/userenmaster/AliEn/t";
  eval `cat $ENV{ALIEN_TESTDIR}/functions.pl`;
  includeTest("16-add") or exit(-2);
  includeTest("126-OutputInSeveralSE") or exit(-2);
  #  eval `cat $ENV{ALIEN_TESTDIR}/FillTableFor126-OutputInSeveralSE.pl`;

  my $outputstring=shift;

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

       my $noLinkRegistration=0;

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

       my @archives=();
       ($testTable->{$tcase}->{archivename} ne "") and
              push @archives, $testTable->{$tcase}->{archivename};


       my @defaultFiles=("stdout","stderr","resources");

       for my $filename (@{$fileTable->{$tcase}->{listing}}) {




#         print "FILENAME: $filename\n";
        
         for my $j(0..$#defaultFiles) {
             if($defaultFiles[$j] eq $filename) {
               delete $defaultFiles[$j]; 
               last;
             }
         }


         if(grep(/$filename/, @archivefiles)) {
#         print "FILE IN archivefiles, archivefiles: @archivefiles\n";
           if(grep(/no_se/, @{$fileTable->{$tcase}->{$filename}->{ses}})){
#          print "ARCHIVE FILE: $filename\n";
#          print "ARCHIVE FILE: $fileTable->{$tcase}->{$fileTable->{$tcase}->{$filename}->{guid}}\n";
#          print "ARCHIVE FILE: $testTable->{$tcase}->{archivename}\n";
               if($fileTable->{$tcase}->{$fileTable->{$tcase}->{$filename}->{guid}} eq $testTable->{$tcase}->{archivename}) {
                   if($testTable->{$tcase}->{asec} eq "") {
                        if( $fileTable->{$tcase}->{$testTable->{$tcase}->{archivename}}->{copies} >= $defaultCopies ) {
                           for my $j(0..$#archivefiles) {
                                   ($archivefiles[$j] eq $filename) and delete $archivefiles[$j];
                           }   
                        }
                   } else {
                        if($testTable->{$tcase}->{asec} eq $fileTable->{$tcase}->{$testTable->{$tcase}->{archivename}}->{copies}){
                           for my $j(0..$#archivefiles) {
                                   ($archivefiles[$j] eq $filename) and delete $archivefiles[$j];
                           }   
                        } 
                   }
               }
           }
         }


         if(grep(/$filename/, @filefiles)) {
#         print "GREP FILE: $filename\n";
             if(grep(/no_archive/, @{$testTable->{$tcase}->{fopt}})){
                 if(!grep(/no_se/, @{$fileTable->{$tcase}->{$filename}->{ses}})){ 
                     if($testTable->{$tcase}->{fsec} eq "") {
                        if($fileTable->{$tcase}->{$filename}->{copies}  >= $defaultCopies) {
                            for my $j(0..$#filefiles) {
                                    ($filefiles[$j] eq $filename) and delete $filefiles[$j];
                             } 
                        }
                     } else {
                        if($fileTable->{$tcase}->{$filename}->{copies} eq $testTable->{$tcase}->{fsec}) {
                            for my $j(0..$#filefiles) {
                                    ($filefiles[$j] eq $filename) and delete $filefiles[$j];
                             } 
                        }
                     }
                 }
             } else {
                 if(grep(/no_se/, @{$fileTable->{$tcase}->{$filename}->{ses}}) && (scalar( @{$fileTable->{$tcase}->{$filename}->{ses}}) eq 1)) {
                     my $archivename = $fileTable->{$tcase}->{$fileTable->{$tcase}->{$filename}->{guid}};
                     if(grep(/$archivename/, @archives)) {
                         if($testTable->{$tcase}->{fsec} eq "") {
                           if( $fileTable->{$tcase}->{$testTable->{$tcase}->{archivename}}->{copies} >= $defaultCopies ) {
                             for my $j(0..$#filefiles) {
                                     ($filefiles[$j] eq $filename) and delete $filefiles[$j];
                             } 
                           }   
                         } else {
                           if( $fileTable->{$tcase}->{$testTable->{$tcase}->{archivename}}->{copies} eq $testTable->{$tcase}->{fsec} ) {
                              for my $j(0..$#filefiles) {
                                   ($filefiles[$j] eq $filename) and delete $filefiles[$j];
                              } 
                           } 
                         }
                     } else {
                         if($testTable->{$tcase}->{fsec} eq "") {
                           if( $fileTable->{$tcase}->{$fileTable->{$tcase}->{$fileTable->{$tcase}->{$filename}->{guid}}}->{copies} >= $defaultCopies ) {
                             for my $j(0..$#filefiles) {
                                     ($filefiles[$j] eq $filename) and delete $filefiles[$j];
                             } 
                           }   
                         } else {
                           if( $fileTable->{$tcase}->{$fileTable->{$tcase}->{$fileTable->{$tcase}->{$filename}->{guid}}}->{copies} eq $testTable->{$tcase}->{fsec} ) {
                              for my $j(0..$#filefiles) {
                                   ($filefiles[$j] eq $filename) and delete $filefiles[$j];
                              } 
                           } 
                         }
                     }
                 }
             }
         }

         if(grep(/$filename/, @archives)) {
             if($testTable->{$tcase}->{asec} eq $fileTable->{$tcase}->{$filename}->{copies}) {
                      for my $j(0..$#archives) {
                                 ($archives[$j] eq $filename) and delete $archives[$j];
                      } 
             } 
         }
 
       
       }
       for my $j(0..$#defaultFiles) {
             if(grep(/$defaultFiles[$j]/, @{$testTable->{$tcase}->{archivecontent}})) {
                 if(grep(/no_link_registration/, @{$testTable->{$tcase}->{aopt}})){ 
                        $noLinkRegistration=1;
                        delete $defaultFiles[$j];
                 } 
             }
             if(grep(/$defaultFiles[$j]/, @{$testTable->{$tcase}->{filetag}})) {
                 if(grep(/no_link_registration/, @{$testTable->{$tcase}->{fopt}})){ 
                        $noLinkRegistration=1;
                        delete $defaultFiles[$j];
                 } 
             }
             
       }
       for my $j(0..$#archivefiles) {
             if(grep(/$archivefiles[$j]/, @{$testTable->{$tcase}->{archivecontent}})) {
                 if(grep(/no_link_registration/, @{$testTable->{$tcase}->{aopt}})){ 
                        $noLinkRegistration=1;
                        delete $archivefiles[$j];
                 } 
             }
       }
       for my $j(0..$#filefiles) {
             if(grep(/$filefiles[$j]/, @{$testTable->{$tcase}->{filetag}})) {
                 if(grep(/no_link_registration/, @{$testTable->{$tcase}->{fopt}})){ 
                        $noLinkRegistration=1;
                        delete $filefiles[$j];
                 } 
             }
       }

  
       (scalar(@archivefiles) eq 0) and $archiveContentStatus=1;
       (scalar(@archives) eq 0) and $archiveStatus=1;
       (scalar(@filefiles) eq 0) and $fileStatus=1;
       (scalar(@defaultFiles) eq 0) and $defaultFileStatus=1;

       $testTable->{$tcase}->{status} = $archiveStatus && $archiveContentStatus && $fileStatus && $defaultFileStatus;
       $TotalTestStatus = $TotalTestStatus && $testTable->{$tcase}->{status}; 

  print "#########################################\n";
  print "Test result of $tcase with id: $testTable->{$tcase}->{id}\n";
  statusPrint("   ArchiveOutput",$archiveStatus);   
  statusPrint("   ArchiveContentOutput",$archiveContentStatus);   
  statusPrint("   OutputFiles",$fileStatus);   
  statusPrint("   Default Files",$defaultFileStatus);   
  statusPrint("   TEST FINAL STATUS",$testTable->{$tcase}->{status});




  } 
  print "#########################################\n";
  print "#########################################\n";
  print "#########################################\n";
  statusPrint("ALL TESTS FINAL STATUS",$TotalTestStatus); 

  $TotalTestStatus and ok(1);
 
  exit(-2);
  
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



