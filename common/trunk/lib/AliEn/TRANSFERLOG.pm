# TRANSFER INFORMATION LOGGING
# AJP 06/2004
# ----------------------------------------------------------

package AliEn::TRANSFERLOG;
use AliEn::Logger;
use strict;

sub new {
    my $proto = shift;
    my $self  = {};
    bless( $self, ( ref($proto) || $proto ) );

    $self->{LOGGER}=new AliEn::Logger;
    $self->{CONFIG}=AliEn::Config->new();

    if ( $ENV{'ALIEN_TRANSFERINFORMATION'} ) {
      if (! -d $ENV{'ALIEN_TRANSFERINFORMATION'} ) {
	    print STDERR "I don't write the TRANSFERLOG for $ENV{'ALIEN_TRANSFERINFORMATION'} \n";
	    $self->{enabled} = 0;
	  }
    } else {
      $self->{LOGGER}->info("TRANSFERLOG", "WARNING!! The directory for the transfer log is not defined. Taking $self->{CONFIG}->{TMP_DIR}/transferlog");
      $ENV{'ALIEN_TRANSFERINFORMATION'}="$self->{CONFIG}->{TMP_DIR}/transferlog";
    }
    
    $self->{enabled} = 1;

    system("mkdir -p $ENV{'ALIEN_TRANSFERINFORMATION'}");
    return $self;
}

sub setlogfile {
    my $self = shift;
    my $transferid = shift;
    my $transferdir = sprintf "%04d", int($transferid /10000);
    $self->{TRANSFERLOGFILE} = $ENV{'ALIEN_TRANSFERINFORMATION'}."/" .$transferdir. "/"."$transferid.log";
    mkdir $ENV{'ALIEN_TRANSFERINFORMATION'}."/" .$transferdir ,0755;
}

 sub putlog {
	my $self = shift;
	my $transferid = shift;
	my $status  = shift;
	#     my $destination = shift;
	my $messages = join (" ",@_);
	#     my $now  = time;
	my $now    = localtime;
# 	$now =~ s/^\S+\s(.*):[^:]*$/$1/;
	
	$self->{enabled} or return;
	
	$self->setlogfile($transferid);
	
	open OUTPUT,  ">> $self->{TRANSFERLOGFILE}";
	#     printf OUTPUT "$now [%-10s]: $messages\n", $status;
	printf OUTPUT "$now [%-10s]: $messages\n", $status;
	close OUTPUT;
}

# sub getlog {
#   my $self = shift;
#   my $transferid = shift;
#   my @tags = @_;
# 
#   if ($tags[0] eq "all") {
#     undef @tags;
#     push @tags,"proc";
#     push @tags,"error";
#     push @tags,"submit";
#     push @tags,"move";
#     push @tags,"state";
#     push @tags,"trace";
#   }
# 
#   grep (/^error$/, @tags) or push @tags,"error";
# 
#   $self->{enabled} or return;
#   $self->setlogfile($transferid);
#   map {$_="($_)"} @tags;
#   my $status=join ("|", @tags);
#   open INPUT,  "$self->{TRANSFERLOGFILE}";
#   my @result= grep (/\[$status/, <INPUT>);
#   close INPUT;
#   $self->{LOGGER}->info("TRANSFERLOG", "Looking for $status of $transferid and found $#result");
# 
#   return @result;
# }

return 1;
