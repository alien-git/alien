#/**************************************************************************
# * Copyright(c) 2001-2002, ALICE Experiment at CERN, All rights reserved. *
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
package AliEn::Database::Accesses;

use AliEn::Database;

use strict;

=head1 NAME

AliEn::Database::Catalogue - database wrapper for AliEn catalogue

=head1 DESCRIPTION

This module interacts with a database of the AliEn Catalogue. The AliEn Catalogue can be distributed among several databases, each one with a different layout. In this basic layout, there can be several tables containing the entries of the catalogue. 

=cut

use vars qw(@ISA $DEBUG);
 
# push @ISA, qw(AliEn::Database AliEn::Database::Catalogue::LFN AliEn::Database::Catalogue::GUID );
push @ISA, qw(AliEn::Database);
$DEBUG = 0;

=head1 SYNOPSIS

  use AliEn::Database::Accesses;

  my $accesses=AliEn::Database::Accesses->new() or exit;

=head1 METHODS

=over

=cut

sub preConnect {
  my $self = shift;
 
  $self->{DB} and $self->{HOST} and $self->{DRIVER} and return 1;

  #! ($self->{DB} and $self->{HOST} and $self->{DRIVER} ) or (!$self->{CONFIG}->{ACCESSES_DATABASE}) and  return;
  $self->debug(2, "Using the default 
  $self->{CONFIG}->{POPULARITY_DATABASE}");
  ($self->{HOST}, $self->{DRIVER}, $self->{DB}) = split(m{/}, $self->{CONFIG}->{POPULARITY_DATABASE});
  my $dbb = $self->{DB};
  my $hostb = $self->{HOST};
  my $driverb = $self->{DRIVER};

  return 1;
}

=item C<createAccessesTables>

This methods creates the database schema in an empty database.

=cut


sub createAccessesTables {
  my $self = shift;
  my $options = shift || {};
    
  $DEBUG and $self->debug(2, "In createAccessesTables creating all tables...");
  
  my %tables = (
    collectors => [
      "name",
      {
      	name => "varchar(20) NOT NULL",
      	startTime => "timestamp NOT NULL DEFAULT 0",
      	actions => "tinyint(1) NOT NULL",
      },
       undef,
      ['PRIMARY KEY(`name`, `startTime`)'],
    ],
    dailySchedule => [
      "name",
      {
      	name => "varchar(20) NOT NULL",
      	day => "DATE NOT NULL DEFAULT 0",
      	completed => "mediumint(8) NOT NULL",
      	actions => "tinyint(1) NOT NULL",
      },
      undef,
      ['PRIMARY KEY(`name`, `day`)'],
    ],
    userInfo => [
      "userId",
      {
      	userId   => "mediumint(8) NOT NULL auto_increment primary key",
        userName => "char(20) NOT NULL UNIQUE",
      }
    ],
    seInfo => [
      "seId",
      {
        seId    => "int(11) NOT NULL auto_increment primary key",
        seName  => "varchar(60) NOT NULL UNIQUE",
      }
    ],
    periods => [
      "perId",
      {
      	perId         => "tinyint UNIQUE NOT NULL auto_increment primary key",
      	periodName         => "varchar(30) UNIQUE NOT NULL",
      }
    ],
    categoryPattern => [
      "categoryId",
      {
      	categoryId         => "tinyint UNIQUE NOT NULL auto_increment",
      	perId         => "tinyint",
      	categoryName         => "varchar(10) NOT NULL",
      	pattern => "varchar(255) NOT NULL",
      },
      undef,
      ['PRIMARY KEY(`perId`, `pattern`)',
      'FOREIGN KEY (`perId`) REFERENCES `periods` (`perId`) ON DELETE CASCADE ON UPDATE CASCADE'],
    ],
    fileAccessInfo => [
      "fileName",    
      {
        fileName         => "varchar(255) not null",
        seId => "int(11) not null",
		operation => "varchar(6) not null",
		accessTime => "timestamp NOT NULL DEFAULT 0",
		userId => "mediumint(8) not null",
		success => "tinyint(1) NOT NULL"
      },
      undef,
      ['PRIMARY KEY(`fileName`, `seId`, `operation`, `userId`, `accessTime`, `success`)',
       'FOREIGN KEY (`userId`) REFERENCES `userInfo` (`userId`) ON DELETE CASCADE ON UPDATE CASCADE',
       'FOREIGN KEY (`seId`) REFERENCES `seInfo` (`seId`) ON DELETE CASCADE ON UPDATE CASCADE'
      ],
    ],
    filePopHourly => [
      "fileName",
      {
        fileName           => "varchar(255) NOT NULL",
        seId     	   => "int(11) NOT NULL",
        nbUserSuccess      => "int(11) NOT NULL",
        nbUserFailure      => "int(11) NOT NULL",
        accessTime         => "timestamp NOT NULL DEFAULT 0",
        nbReadOp 		   => "int(11) NOT NULL",
        nbWriteOp 	   => "int(11) NOT NULL",
        nbReadFailure     => "int(11) NOT NULL",
        nbWriteFailure    => "int(11) NOT NULL",
      },
      undef,
      ['PRIMARY KEY(`fileName`, `seId`, `accessTime`)', 
       'FOREIGN KEY (`seId`) REFERENCES `seInfo` (`seId`) ON DELETE CASCADE ON UPDATE CASCADE' 
      ],
    ], 
    categoryPopHourly => [
      "accessTime",
      {
        userId     => "mediumint(8) NOT NULL",
        categoryId     => "tinyint NOT NULL",
        accessTime     => "timestamp NOT NULL DEFAULT 0",
 #       nbWriteOp => "int(11) NOT NULL",
        nbReadOp => "int(11) NOT NULL",
      },
      undef,
      ['PRIMARY KEY(`userId`, `categoryId`, `accessTime`)',
      'FOREIGN KEY (`categoryId`) REFERENCES `categoryPattern` (`categoryId`) ON DELETE CASCADE ON UPDATE CASCADE',
      'FOREIGN KEY (`userId`) REFERENCES `userInfo` (`userId`) ON DELETE CASCADE ON UPDATE CASCADE'
      ], 
    ],
    filePopDaily => [
      "fileName",
      {
        fileName           => "varchar(255) NOT NULL",
        seId     	   => "int(11) NOT NULL",
        nbUserSuccess      => "int(11) NOT NULL",
        nbUserFailure      => "int(11) NOT NULL",
        accessDate         => "date NOT NULL DEFAULT 0",
        nbReadOp 		   => "int(11) NOT NULL",
        nbWriteOp 	   => "int(11) NOT NULL",
        nbReadFailure     => "int(11) NOT NULL",
        nbWriteFailure   => "int(11) NOT NULL",

      },
      undef,
      [ 'PRIMARY KEY(`fileName`, `seId`, `accessDate`)', 'FOREIGN KEY (`seId`) REFERENCES `seInfo` (`seId`) ON DELETE CASCADE ON UPDATE CASCADE' ],
    ],
    categoryPopDaily => [
      "accessDate",
      {
        userId     => "mediumint(8) NOT NULL",
        categoryId     => "tinyint NOT NULL",
        accessDate     => "date NOT NULL DEFAULT 0",
        nbReadOp => "int(11) NOT NULL",
#        nbWriteOp => "int(11) NOT NULL"        
      },
      undef,
      ['PRIMARY KEY(`userId`, `categoryId`, `accessDate`)', 
       'FOREIGN KEY (`userId`) REFERENCES `userInfo` (`userId`) ON DELETE CASCADE ON UPDATE CASCADE',
       'FOREIGN KEY (`categoryId`) REFERENCES `categoryPattern` (`categoryId`) ON DELETE CASCADE ON UPDATE CASCADE'
      ], 
    ],
  );



=comment

priority         => "tinyint NOT NULL",

#FOR NOW WE DON'T NEED THESE TABLES ===
  popularFiles => [
      "fileName",
      {
        fileName        => "varchar(255) NOT NULL",
        popularity      => "int(11) NOT NULL",
        startDate       => "date NOT NULL",
        endDate         => "date NOT NULL",
        nbUsers         => "int(11) NOT NULL",
      },
      undef,
      [ 'PRIMARY KEY(`fileName`, `startDate`)'],
    ],
    usersPopularCategories => [
      "userId",
      {
        userId     => "mediumint(8) NOT NULL",
        categoryId     => "tinyint NOT NULL",
        startDate     => "date NOT NULL",
        endDate     => "date NOT NULL",
        popularity => "int(11) NOT NULL"
      },
      undef,
      ['PRIMARY KEY(`userId`, `categoryId`, `startDate`)', 
       'FOREIGN KEY (`userId`) REFERENCES `userInfo` (`userId`) ON DELETE CASCADE ON UPDATE CASCADE',
       'FOREIGN KEY (`categoryId`) REFERENCES `categoryPattern` (`categoryId`) ON DELETE CASCADE ON UPDATE CASCADE'
      ],
    ],
    corruptedFiles => [
      "fileName",
      {
        fileName           => "varchar(255) NOT NULL",
        seId     	   => "int(11) NOT NULL",
        startDate         => "date NOT NULL",
        endDate         => "date NOT NULL",
        nbReadFailure     => "int(11) NOT NULL",
        nbUsers     => "int(11) NOT NULL",
      },
      undef,
      [ 'PRIMARY KEY(`fileName`, `seId`, `startDate`)', 
        'FOREIGN KEY (`seId`) REFERENCES `seInfo` (`seId`) ON DELETE CASCADE ON UPDATE CASCADE' 
      ],
    ]
=cut    






        
  foreach my $table (keys %tables) {
    $self->checkTable($table, @{$tables{$table}}) or return;
  }
  
  foreach my $table (keys %tables) {
    $self->checkTable($table, @{$tables{$table}}) or return;
  }
$self->do("INSERT INTO periods (periodName) VALUES (\"OTHER\")"); 
$self->do("INSERT INTO categoryPattern (categoryName, pattern, perId)  
VALUES 
  ('other','\.+', LAST_INSERT_ID() ),
  ('USER', 	'\^\/alice\/cern\\.ch\/user\/\.+\$', LAST_INSERT_ID() ),
  ('COND', 	'\^\/alice\/.*\/*\(OCDB\)\|\(CDB\)\/\.+', LAST_INSERT_ID() )") or $self->info("We could not fill categoryPattern table!") and return;

#  (3, 'AOD', 	'\^\/alice\/data\/\.+AliAOD\.\*\\.root\$'),
#  (4, 'ESD', 	'\^\/alice\/data\/\.+\/ESDs\/\.+\$'),
#  (5, 'ESDSIM', '\^\/alice\/sim\/\.+AliESD\.\*\\.root\$'),


  $self->do("INSERT INTO collectors (name, startTime, actions) VALUES ('Parser', curtime(), 0 )") or return ;

  $DEBUG and $self->debug(2, "In createAccessesTables creation of tables finished.");
  
  return 1;
}


1;
