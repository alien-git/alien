#/**************************************************************************
# * Copyright(c) 2001-2003, ALICE Experiment at CERN, All rights reserved. *
# *                                                                        *
# * Author: The ALICE Off-line Project / AliEn Team                        *
# * Contributors are mentioned in the code where appropriate.              *
# *                                                                        *
# * Permission to use, copy, modify and distribute this software and its   *
# * documentation strictly for non-commercial purposes is hereby granted   *
# * without fee, provided that the above copyright notice appears in all   *
# * copies and that both the copyright notice and this permission notice   *
# * appear in the supporting documentation. The authors make no claims     *
# * about the suitability of this software for any purpose. It is          *
# * provided "as is" without express or implied warranty.                  *
# **************************************************************************/

package AliEn::Database::IS;

use AliEn::Database;

use strict;

use vars qw(@ISA);
@ISA=("AliEn::Database");
my @services = ("SE", "CLC", "CLCAIO", "ClusterMonitor", "FTD","TcpRouter", "PackMan");


sub new {
  my $self=shift;
  my $options=(shift or {});
  my $c=new AliEn::Config();

  $options->{DB} or $options->{DB}= $c->{IS_DATABASE};
  $options->{HOST} or $options->{HOST}=$c->{IS_DB_HOST};
  $options->{DRIVER} or $options->{DRIVER}=$c->{IS_DRIVER};

  return $self->SUPER::new($options);
}

sub initialize {
  my $self=shift;
  my %columns= (host=>"varchar(100) not null",
		port=>"int(5)",
		status=>"char(15)",
		lastchecked =>"int(11)",
		version=>"char(10)",
		name=>"varchar(200)",
		defaultSE=>"int(1)",
		URI=>"char(50)",
		protocols=>"char(255)",
		certificate=>"char(255)",);


  $self->createCLCCERTTable();
  foreach (@services, "Services" ) {
    $self->checkTable($_, "host", \%columns, "name");
  }
  $self->checkTable("cpu_si2k", "cpu_model_name", {
  		cpu_model_name => "varchar(100)",
		cpu_cache => "int(5)",
		cpu_MHz => "int(6)",
		si2k => "int(7)"
  	});
  return $self;
}
sub setService{
  my $self = shift;
  my $table = shift
    or $self->{LOGGER}->error("IS","In setService service name is missing")
      and return;
  my $set = {};
  
  $set->{name} = shift
    or $self->{LOGGER}->error("IS","In setService service name is missing")
      and return;
  $set->{host} = shift;
  $set->{port} = shift;
  $set->{status} = shift;
  $set->{lastchecked} = shift;
  $set->{version} = shift;
  $set->{URI} = shift;
  $set->{protocols} = shift;
  $set->{certificate} =shift;

  $self->debug(1,"In setService testing if service $table $set->{name} exists");

  if($self->queryValue("SELECT count(*) FROM $table where name='$set->{name}'")){
    $self->debug(1,"In setService updating service $table $set->{name}");
    $self->update($table,$set,"name= ?", {bind_values=>[$set->{name}]});
  } else {
    $self->debug(1,"In setService inserting service $table $set->{name}");
    $self->insert($table,$set);
  }
}

sub getServiceNameByHost {
    my $self = shift;
    my $service = shift
	or $self->{LOGGER}->error("IS","In getServiceNameByHost service name is missing")
	and return;


    my $host = shift 
	or $self->{LOGGER}->error("IS","In getServiceNameByHost host name is missing")
	and return;
    
    $self->debug(1,"In getServiceNameByHost for service $service and host $host");
    $self->query("SELECT name FROM $service where host=? order by lastchecked desc", undef, {bind_values=>[$host]});
}
    
sub getActiveServices{
  my $self = shift;
  my $service = shift
    or $self->{LOGGER}->error("IS","In setService service name is missing")
      and return;
  my $attr = shift || "*";
  my $name = shift;
  
  $name
    and $name = " and name='$name'"
      or $name = "";
  
  $self->debug(1,"In getActiveServices fetching attributes $attr of active services $service");
  $self->query("SELECT $attr FROM $service where status='ACTIVE'$name");
}

sub getRouteByPath {
  my $self = shift;
  my $source = shift
    or $self->{LOGGER}->error("IS","In getRouteByPath source is missing")
      and return;
  my $dest = shift
    or $self->{LOGGER}->error("IS","In getRouteByPath destination is missing")
      and return;

  $self->debug(1,"In getRouteByPath fetching attributes nextdest,method,soaphost,soapport for route $source, $dest");
  return $self->query("SELECT nextdest,method,soaphost,soapport FROM ROUTE WHERE SOURCE = '$source' AND FINALDEST = '$dest'");
}

sub getFields{
  my $self = shift;
  my $service = shift
    or $self->{LOGGER}->error("IS","In getFields service name is missing")
      and return;
  my $host = shift
    or $self->{LOGGER}->error("IS","In getFields host is missing")
      and return;
  my $port = shift
    or $self->{LOGGER}->error("IS","In getFields port is missing")
      and return;

  my $attr = shift || "*";

  $self->debug(1,"In getFields fetching attributes $attr from $service");
  $self->queryRow("SELECT $attr FROM $service WHERE host= ? AND port= ?", undef, {bind_values=>[$host, $port]});
}

sub getField{
  my $self = shift;
  my $service = shift
    or $self->{LOGGER}->error("IS","In getField service name is missing")
      and return;
  my $host = shift
    or $self->{LOGGER}->error("IS","In getField host is missing")
      and return;
  my $port = shift
    or $self->{LOGGER}->error("IS","In getField port is missing")
      and return;

  my $attr = shift || "*";

  $self->debug(1,"In getField fetching attribute $attr from $service");
  $self->queryValue("SELECT $attr FROM $service WHERE host=? AND port=?", undef, {bind_values=>[$host, $port]});
}

sub createCLCCERTTable{
  my $self = shift;

  $self->debug(1,"In createCLCCERTTable creating CLCCERT table");
  $self->createTable("CLCCERT","(user char(200), name char(200), certificate blob)",1);
}

sub insertCertificate{
  my $self = shift;
  my $user = shift;
  my $name = shift;
  my $cert = shift;
  
  $self->debug(1,"In insertCertificate inserting certificate");
  $self->insert("CLCCERT",{user=>$user,name=>$name,certificate=>$cert});
}

sub deleteCertificate{
  my $self = shift;
  my $user = shift
    or $self->{LOGGER}->error("IS","In deleteCertificate user is missing")
      and return;
  my $name = shift
    or $self->{LOGGER}->error("IS","In deleteCertificate name is missing")
      and return;
  
  $self->debug(1,"In deleteCertificate deleting certificate for user $user and name $name");
  $self->delete("CLCCERT","user=? AND name = ?", {bind_values=>[$user, $name]});
}

sub getCertificate{
  my $self = shift;
  my $user = shift
    or $self->{LOGGER}->error("IS","In getCertificate user is missing")
      and return;
  my $name = shift
    or $self->{LOGGER}->error("IS","In getCertificate name is missing")
      and return;
  
  $self->debug(1,"In getCertificate fetching certificate for user $user and name $name");
  $self->queryValue("SELECT certificate FROM CLCCERT WHERE user=? AND name = ?", undef, {bind_values=>[$user, $name]});
}

# Look into the cpu_si2k table for an entry about a cpu type and return back the corresponding SI2k
# In case there is no entry for that cpu, but there are at least two for similar cpus (same model_name & cache)
# it tries to estimate the value.
sub getCpuSI2k {
  my $self = shift;
  my $cpu_type = shift;  # from JobAgent
  my $cm_host = shift;  # host of the cluster monitor

  return (-1, 'Invalid cpu_type hash.') if(! ($cpu_type && $cpu_type->{cpu_model_name} && $cpu_type->{cpu_cache} && $cpu_type->{cpu_MHz}));
 
  my $min_cpu_mhz = $cpu_type->{cpu_MHz} - $cpu_type->{cpu_MHz} * 0.02; # allow 2% deviation
  my $max_cpu_mhz = $cpu_type->{cpu_MHz} + $cpu_type->{cpu_MHz} * 0.02;
  # try querying the database for exactly this configuration
  my $result = $self->queryValue("SELECT si2k FROM cpu_si2k WHERE ? LIKE cpu_model_name AND ? LIKE cpu_cache AND cpu_MHz >= ? AND cpu_MHz <= ?", undef, {bind_values=>[$cpu_type->{cpu_model_name}, $cpu_type->{cpu_cache}, $min_cpu_mhz, $max_cpu_mhz]});
  
  if(! defined($result)){
    my $list = $self->query("SELECT DISTINCT si2k, cpu_MHz FROM cpu_si2k WHERE ? LIKE cpu_model_name AND ? LIKE cpu_cache order by abs(cpu_MHz - ?) asc", undef, {bind_values=>[$cpu_type->{cpu_model_name}, $cpu_type->{cpu_cache}, $cpu_type->{cpu_MHz}]});
    if(defined($list) && (@$list >= 2)){
      my $cpu1 = $list->[0];
      my $cpu2 = $list->[1];
      if($cpu2->{cpu_MHz} != $cpu1->{cpu_MHz}){
        my $factor = ($cpu2->{si2k} - $cpu1->{si2k}) / ($cpu2->{cpu_MHz} - $cpu1->{cpu_MHz});
        $result = $cpu1->{si2k} + $factor * ($cpu_type->{cpu_MHz} - $cpu1->{cpu_MHz});
      }else{
        $self->info("cpu_si2k table contains two entries with the same CPU: cpu_model_name='$cpu1->{cpu_model_name}' cpu_cache='$cpu1->{cpu_cache}' cpu_MHz='$cpu1->{cpu_MHz}'");
      }
    }
  }
  if(! defined($result)){
    $self->info("SI2k unknown: cm_host='$cm_host' host='$cpu_type->{host}' cpu_model_name='$cpu_type->{cpu_model_name}' cpu_cache='$cpu_type->{cpu_cache}' cpu_MHz='$cpu_type->{cpu_MHz}'");
  }else{
    $self->info("SI2k resolved: cm_host='$cm_host' host='$cpu_type->{host}' cpu_model_name='$cpu_type->{cpu_model_name}' cpu_cache='$cpu_type->{cpu_cache}' cpu_MHz='$cpu_type->{cpu_MHz}' ===> $result");
  }
  return $result;
}

=head1 NAME

AliEn::Database::IS

=head1 DESCRIPTION

The AliEn::Database::IS module extends AliEn::Database module. Module
contains method specific for tables from database INFORMATIONSERVICE.
Database INFORMATIONSERVICE contains informations about services. It 
contains following tables: tables for services, Service table and certificate
tables. Each service has it's own table. All tables for services have
same set of attributes. 

=head1 SYNOPSIS

  use AliEn::Database::IS;

  my $dbh = AliEn::Database::IS->new($dbOptions);

  $res = $dbh->getField($serviceName, $host, $port, $attr);
  $hashRef = $dbh->getFields($serviceName, $host, $port, $attr);
    
  $arrRef = $dbh->getActiveServices($serviceName, $attr, $name);
  $arrRef = $dbh->getRouteByPath($source, $destionation);
  $res = $dbh->getCertificate($user, $name);
  
  $res = $dbh->setService($serviceName, $name, $host, $port, $status, $lastchecked, $version, $URI);
  
  $res = $dbh->insertCertificate($user, $name, $cert);
  $res = $dbh->deleteCertificate($user, $name);
  
  $res = $dbh->createCLCCERTTable();
  $res = $dbh->createCLCTable();
  $res = $dbh->createTcpRouterTable();

=cut

=head1 METHODS

=over

=item C<new>

  $dbh = AliEn::Database::IS->new( $attr );

  $dbh = AliEn::Database::IS->new( $attr, $attrDBI );

Creates new AliEn::Database::IS instance. Arguments are passed to AliEn::Database
method new. For details about arguments see AliEn::Database method C<new>.

=item C<getField>

  $res = $dbh->getField($serviceName, $host, $port, $attr);

Method fetches value of attribute $attr for tuple with host $host and port 
$port from table $serviceName.
If any of arguments is not defined method will return undef and report error.
Method calls AliEn::Database metod queryValue.

=item C<getFields>

  $hashRef = $dbh->getFields($serviceName, $host, $port, $attr);

Method fetches set of attributes $attr for tuple with host $host and port 
$port from table $serviceName.
Result is reference to hash. Keys in hash are identical to names of attriutes 
in $attr set.
If set of attributes is not defined method returns values of all attributes. If
transfer id is not defined method will return undef and report error.
Method calls AliEn::Database metod queryRow.

=item C<getActiveServices> 

  $arrRef = $dbh->getActiveServices($serviceName, $attr);  
  
  $arrRef = $dbh->getActiveServices($serviceName, $attr, $name);
  
Method fetches set of attributes $attr for services with status ACTIVE and name $name 
from table $serviceName. 
If set of attributes is not defined method returns values of all attributes. If name is
not defined method will return all services with status ACTIVE. If $serviceName is not
defined method will return undef and report error.

=item C<getRouteByPath> 

  $arrRef = $dbh->getRouteByPath($source, $dest);

Method retrieves values of attributes nextdest, method, soaphost, soapport for tuples
with source $source and finaldest $dest from table ROUTE. Return value is reference to
array.
If any of arguments is not defined method will return undef and report error. 

=item C<getCertificate> 

  $res = $dbh->getCertificate($user, $name);
  
Method retrieves certificate from table CLCCERT for user $user and 
name $name. If any of arguments is not defined method will return undef
and report error.

=item C<setService> 

  $res = $dbh->setService($serviceName, $name, $host, $port, $status, $lastchecked, $version, $URI);

Method checks if service with name $name exists in table $serviceName.
If service exists method will update service with given arguments. If service
doesn't exist method will insert new with given data.
If $serviceName or $name are not defined method will return undef and report
error.  
  
=item C<insertCertificate> 

  $res = $dbh->insertCertificate($user, $name, $cert);
  
Method inserts certificate $cert into table CLCCERT for user $user and 
name $name. 

=item C<deleteCertificate> 

  $res = $dbh->deleteCertificate($user, $name);
  
Method deletes certificate from table CLCCERT for user $user and 
name $name. If any of arguments is not defined method returns undef
and reports error.

=item C<createCLCCERTTable>, C<createCLCTable>, C<createTcpRouterTable> 

  $res = $dbh->createCLCCERTTable();
  
  $res = $dbh->createCLCTable();
  
  $res = $dbh->createTcpRouterTable();  

Methods for creating tables CLCCERT, CLC and TcpRouter.

=back

=head1 SEE ALSO

AliEn::Database

=cut

1;
