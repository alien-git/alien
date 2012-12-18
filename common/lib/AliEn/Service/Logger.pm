select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

use strict;

package AliEn::Service::Logger;

use AliEn::Service;

use vars qw(@ISA);

@ISA=("AliEn::Service");

use AliEn::Database;

my %lastmsg;

my $self;


sub initialize {
  $self=shift;
 
	$self->{PORT}=$self->{CONFIG}->{'LOG_PORT'};
	$self->{HOST}=$self->{CONFIG}->{'LOG_HOST'};
	$self->{SERVICE}="Logger";
	$self->{SERVICENAME}="Logger";
	$self->{LISTEN}=1;
	$self->{PREFORK}=5;

	$self->{LOGGER}->removeSOAPLogger();
	print "Starting logserver\n";
    $lastmsg{count} = 0;

  
    my $db     = $self->{CONFIG}->{AUTHEN_DATABASE};
    my $host   = $self->{CONFIG}->{AUTHEN_HOST};
    my $driver = $self->{CONFIG}->{AUTHEN_DRIVER};

    $self->{DB} = AliEn::Database->new(
        {
            "DB"     => $db,
            "HOST"   => $host,
            "DRIVER" => $driver,
            "DEBUG"  => $self->{DEBUG},
            "SILENT" => $self->{SILENT},
			"ROLE"	 => "admin"
        }
    );

    $self->{DB} or return;

    #$self->{DB}->validateUser( "admin", "" ) or return;

  $self->{hostname}=$host;
    return 1;
}

sub log {
    my $this  = shift;
    my $host  = $self->prepareForDB(shift);
    my $level = $self->prepareForDB(shift);
    $level = "\U$level";
    my $message = $self->prepareForDB(shift);
    my $target  = $self->prepareForDB(shift);
    my $time    = time;
    my $date    = localtime;
    $date =~ s/^\S+\s(.*):[^:]*$/$1/;

    my $done;
    print STDERR "$date   New message: $message\n";
    $self->setAlive();
    if (   ( $lastmsg{host} eq $host )
        && ( $lastmsg{level}   eq $level )
        && ( $lastmsg{message} eq $message )
        && ( $lastmsg{target}  eq $target ) )
    {
        $lastmsg{count}++;
        if ( $lastmsg{count} == 2 ) {
			$done = $self->{DB}->insert("Log",{
										source=>$self->{hostname},
										time=>$time,
										level=>$level,
										message=>'Last message repeated 2 times',
										target=>'Logserver'});
# 			$SQL =
# "INSERT INTO Log values('$self->{hostname}','$time','$level','Last message repeated 2 times','Logserver')";
             $lastmsg{time} = $time;
        }
        else {
		$done = $self->{DB}->update("Log", {time=>$time,
						message=>"Last message repeated $lastmsg{count} times"},
						"source=? AND time = ? AND message like 'Last message repeated % times'",
						{bind_values=>[$self->{hostname}, $lastmsg{time}]}
					);
#             $SQL =
# "UPDATE Log set time='$time', message='Last message repeated $lastmsg{count} times' where source='$self->{hostname}' AND time = $lastmsg{time} AND message like 'Last message repeated % times'";
        }
    }
    else {
#         $SQL =
# "INSERT INTO Log values('$host','$time','$level','$message','$target')";
		$done = $self->{DB}->insert("Log",{
									source=>$host,
									time=>$time,
									level=>$level,
									message=>$message,
									target=>$target});
        $lastmsg{host} = $host;

        $lastmsg{level}   = $level;
        $lastmsg{target}  = $target;
        $lastmsg{message} = $message;
        $lastmsg{count}   = 1;
    }
    #my $done=$self->do($SQL);

    print "Log updated with status $done\n";

    $done or print "Exiting...\n" and exit;
    return 1;
}

sub prepareForDB {
    my $this = shift;
    my $msg  = shift;
    $msg =~ s/\"/\\\"/g;
    $msg =~ s/\'/\\\'/g;

    return $msg;
}

sub do {
    my $this = shift;
    my $SQL  = shift;

    ( $self->{DB}->insert($SQL) )
      or print STDERR "There was an error: $DBI::errstr\nSQL: $SQL\n"
      and return;

}

sub reConnect() {
    print STDERR "Doing a reconnect...";
    $self->{DB}->reconnect or return;
    print STDERR "ok!!\n";
    return 1;
}

1;

