package AliEn::Service::Optimizer::Popularity;

use strict;
use Switch;

use vars qw (@ISA);

use vars qw ($interval);
$interval = 1; # hours

use AliEn::Service::Optimizer;
use AliEn::Database::Accesses;

# require AliEn::UI::Catalogue::LCM;
@ISA = qw(AliEn::Service::Optimizer);

use Data::Dumper;

my $self;

sub initialize {
  $self = shift;
  my $options = (shift or {});

  $self->{SERVICE} = "Popularity";
  $self->{DB_MODULE} = "AliEn::Database::Accesses";

  $self->SUPER::initialize(@_) or return;

  $options->{ROLE} = $options->{role} = "admin";

  $self->{ACCESSES} = AliEn::Database::Accesses->new($options);

  ($self->{ACCESSES})
    or $self->{LOGGER}->error("PopularityOptimizer", "In initialize error creating AliEn::Database::Accesses instance")
    and return;

#  my @optimizers = ("Parser", "HourlyCollector", "DailyCollector");
  my @optimizers = ("Parser", "HourlyCollector");

  $self->StartChildren(@optimizers) or return;

  return $self;
}

sub checkWakesUp {
  my $this   = shift;
  my $silent = shift;
  my $method = "info";
  $silent and $method = "debug";
  $self->info("Checking if there is anything to do");
  return;
}

sub getCollectorTasks {
  my $self = shift;
  my $collectorName = shift;
  my $completedHourlyTasks = shift;
  my $tasks = 0;
  switch($collectorName) {
    case "HourlyCollector" {
      my $tableName = "collectors";
      my $where = "name='$collectorName' and actions=0";
      $tasks = $self->{DB}->query("select startTime from $tableName where $where");
      $tasks or return 0;
      $self->{DB}->update($tableName, {actions=>1}, "$where") or $self->info("could not update actions") and return 0;  
      last;
    }
    case "DailyCollector" {
      my $tableName = "dailySchedule";
      my $where = "name='$collectorName' and actions=0 and completed=$completedHourlyTasks";
      $tasks = $self->{DB}->query("select day from $tableName where $where");
      $tasks or return 0;
      $self->{DB}->update($tableName, {actions=>1}, "$where") or $self->info("could not update actions") and return 0;  
      last;
    }
    else{
      return 0;
    }
  }
   
  return $tasks;
}

sub setCollectorTaskPending {
  my $self = shift;
  my $collectorName = shift;
  my $startTime = shift;
  switch($collectorName) {
    case "Parser" {
      my $tableName = "collectors";
      $self->{DB}->update($tableName, {actions=>0}, "name='$collectorName' and startTime='$startTime'") 
      or $self->info("Could not update actions") and return 0;
      last;
    }
    case "HourlyCollector" {
      my $tableName = "collectors";
      $self->{DB}->update($tableName, {actions=>0}, "name='$collectorName' and startTime='$startTime'") 
      or $self->info("Could not update actions") and return 0;
      last;
    }
    case "DailyCollector" {
      my $tableName = "dailySchedule";
      $self->{DB}->update($tableName, {actions=>0}, "name='$collectorName' and day='$startTime'") or $self->info("could not update actions") and return 0;  
      last;
    }
    else{
      return 0;
    }
  }
 
  return 1;
}

sub increaseCompletedTasksNumber {
  my $self = shift;
  my $collectorName = shift;
  my $startTime = shift;
  switch($collectorName) {
    case "DailyCollector" {
      my $tableName = "dailySchedule";
      $self->{DB}->do("INSERT INTO $tableName (`name`,`actions`, `day`, `completed`) VALUES ('$collectorName', 0, DATE('$startTime'), 1) ON DUPLICATE KEY UPDATE `completed`=completed+1") or $self->info("Could not update completed tasks number") and return 0;  
      last;
    }
    else{
      return 0;
    }
  }
 
  return 1;
}

sub removeCollectorTask {
  my $self = shift;
  my $collectorName = shift ;
  my $startTime = shift;
 
  switch($collectorName) {
    case "HourlyCollector"{
      my $tableName = "collectors";
      $self->{DB}->delete($tableName, "name='$collectorName' and startTime='$startTime'") 
      or $self->info("Could not delete the collector task") and return 0;
      last;
    }
    case "DailyCollector"{
      my $tableName = "dailySchedule";
      $self->{DB}->delete($tableName, "name='$collectorName' and day='$startTime'") or $self->info("Could not delete the collector task") and return 0;  
      last;
    }
    else{
      return 0;
    }
  }
  
  return 1;
}
    
1;

