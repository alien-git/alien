#!/bin/env alien-perl

use strict;
use threads;
use DBI;
use Data::Dumper;
use AliEn::Database;
use AliEn::UI::Catalogue::LCM;
use Net::Domain qw(hostname hostfqdn hostdomain);

### Set direct connection
$ENV{SEALED_ENVELOPE_REMOTE_PUBLIC_KEY}="$ENV{ALIEN_HOME}/authen/rpub.pem";
$ENV{SEALED_ENVELOPE_REMOTE_PRIVATE_KEY}="$ENV{ALIEN_HOME}/authen/rpriv.pem";
$ENV{SEALED_ENVELOPE_LOCAL_PUBLIC_KEY}="$ENV{ALIEN_HOME}/authen/lpub.pem";
$ENV{SEALED_ENVELOPE_LOCAL_PRIVATE_KEY}="$ENV{ALIEN_HOME}/authen/lpriv.pem";
$ENV{ALIEN_DATABASE_ROLE}='admin';
$ENV{ALIEN_DATABASE_PASSWORD}='pass';

### Get connections and DB objects
my $db_now = shift;
(defined $db_now) or $db_now="alice_users";
my $no_threads = shift;
(defined $no_threads) or $no_threads=24;

my $db = AliEn::Database->new({DRIVER => "mysql",
                               HOST   => Net::Domain::hostfqdn().":3307",
                               DB     => "$db_now",
                               ROLE   => "admin"});
my $cat = AliEn::UI::Catalogue::LCM->new({ROLE => "admin"});

### Get index table values for GUID and LFN
my $indexTable = $db->query("SELECT tableName FROM INDEXTABLE");
my $guidIndex  = $db->query("SELECT tableName FROM GUIDINDEX");
my $table;
my $chk=0;

print "2. Doing the alteration in L#L tables\n";
my $status=0;
my $collation='latin1_general_cs';

# Parallization parameters
my $thrI;
my $thrG;
my $startIndex = 0;
my $endIndex = 0;
my $noIndexTables = @$indexTable;
my $tablesPerThread = int($noIndexTables/$no_threads + 0.99);
print "SIZE:$noIndexTables  STEP:$tablesPerThread\n" ;
for(my $thId=1;$thId<=$no_threads;$thId++){
  $startIndex = $endIndex ;
  $endIndex = $endIndex + $tablesPerThread -1;
  if($endIndex>=$noIndexTables){
    $endIndex=$noIndexTables-1;
  }
  if($startIndex>=$noIndexTables){
    last;
  }
  $thrI = threads->new(\&IndexTablesKernel, $startIndex,$endIndex,$thId,$indexTable);
  $endIndex ++;
} 
$thrI->join; 
print "Thread returned -->";
print "New Changes made successfully !!!\n";
print "2. DONE\n".scalar(localtime(time))."\n";
print "Part 2: DONE\n".scalar(localtime(time))."\n";

################################################################################
# Kernel Functions

sub IndexTablesKernel {
  my $startIndx = $_[0];
  my $endIndx = $_[1];
  my $thid = $_[2];
  my $iTable = $_[3];

  ### Get connections and DB objects
  my $dbc = AliEn::Database->new({DRIVER => "mysql",
                               HOST   => Net::Domain::hostfqdn().":3307",
                               DB     => "$db_now",
                               ROLE   => "admin"});
  print "In the thread [$thid] | StartIndex=$startIndx | EndIndex=$endIndx \n ";
  for(my $indx = $startIndx ; $indx<=$endIndx; $indx++)
  { 
    my $table="L".@$iTable[$indx]->{tableName}."L";
    #my $table="L".$row->{tableName}."L";
    print "\n".scalar(localtime(time))."\n";
    print "START: $table\n";
    $dbc->do("ALTER TABLE $table ADD (ownerId MEDIUMINT UNSIGNED, gownerId MEDIUMINT UNSIGNED), 
      ADD FOREIGN KEY (ownerId) REFERENCES USERS(uId),ADD FOREIGN KEY (gownerId) REFERENCES GRPS(gId)" , {timeout=>[60000]} );
    my $status=$dbc->do("select 1 from information_schema.tables where table_name='$table' and table_collation='$collation'");
    ($status) or $status=0;
    if($status==0){
      print "Changing the collation of table $table\n";
      $dbc->do("ALTER TABLE $table CONVERT TO CHARACTER SET latin1 COLLATE latin1_general_cs");
      $dbc->do("ALTER TABLE $table COLLATE latin1_general_cs");
    }
    $dbc->do("UPDATE $table JOIN USERS ON $table.owner=USERS.Username JOIN GRPS ON $table.gowner=GRPS.Groupname SET $table.ownerId=USERS.uId,
      $table.gownerId=GRPS.gId",{timeout=>[60000]} ); 
    $dbc->do("ALTER TABLE $table DROP COLUMN owner, DROP COLUMN gowner",{timeout=>[60000]});
    print "END: $table\n";
  }
}

