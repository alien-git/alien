package AliEn::UI::Catalogue;

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

=head1 B<NAME>

AliEn::UI::Catalogue

=head1 B<SYNOPSIS>

    my $options={role=>admin, user=>"psaiz"};
    my $ui=AliEn::UI::Catalogue->new($options) or exit(-2);

    $ui->execute("cd", "/bin");
    my @files=$ui->execute("ls", "/bin");
    $ui->execute("mkdir", "/test");

=head1 B<DESCRIPTION>

This is the basic interface to the file and metadata catalogue. This base class is extended by modules like AliEn::UI::Catalogue::LCM and AliEn::UI::Catalogue::LCM::Computer, which provide some extra functionality (access to files in the case of the LCM, and to the task manager in case of the Computer). For more details on those classes, check their man pages.

=cut

use strict;
use DBI;
use Time::HiRes;
use Term::ReadLine;
#$readline 'rl_NoInitFromFile = 1;    #'


use Getopt::Long ();
require AliEn::Catalogue;
require AliEn::ClientCatalogue;
use AliEn::UI;
use AliEn::SOAP;
use Data::Dumper;
use vars qw(@ISA $DEBUG);
use AliEn::GUID;


push @ISA, 'AliEn::Logger::LogObject';
$DEBUG=0;

my (%commands);
my ($catalog);

my ( @oldData, @oldDB );
my ($oldpos) = 0;


=pod

All the commands that can be executed are in the hash %commands. The hash contains the name of the command as it will be displayed to the user and the function that the system has to do to call that function.

This interface can also be used to get a UNIX-like prompt. The methods that the object has are:

=over 

=cut

# format: 'command' => ('function', 'type'), where

# type
# bit 0 : 0 for old behavior (1)
# bit 1	: extract options (2)
# bit 2	: pass first argument to all calls (4)
# bit 3	: pass last argument to all calls (8)
# bit 4 : expand wildcards (16)
# bit 5 : preserve last dir while expanding wildcards (32)
# bit 6 : do not make separate calls (64)

%commands = (
    '.'        => ['$self->shell', 0],
    'pwd'      => ['$self->{CATALOG}->f_pwd', 0],
    'cd'       => ['$self->{CATALOG}->f_cd', 0],
    'ls'       => ['$self->{CATALOG}->f_ls', 3+16+32],
    'lsinternal' => ['$self->{CATALOG}->f_lsInternal', 3+16+32],
    'mkremdir' => ['$self->{CATALOG}->f_mkremdir', 0],
    'mkdir'    => ['$self->{CATALOG}->f_mkdir', 3],
    'quit'     => ['$self->{CATALOG}->f_quit', 0],
    'exit'     => ['$self->{CATALOG}->f_quit', 0],
    'whoami'   => ['$self->{CATALOG}->f_whoami', 0],
    'user'     => ['$self->{CATALOG}->f_user', 0],
    'passwd'   => ['$self->{CATALOG}->f_passwd', 0],
    'find'     => ['$self->{CATALOG}->f_find', 0],
    'findEx'   => ['$self->{CATALOG}->findEx', 0],
    'linkfind' => ['$self->{CATALOG}->f_linkfind', 0],
    'cp'       => ['$self->{CATALOG}->f_cp', 16+64],
    'ln'       => ['$self->{CATALOG}->f_ln', 16+64],
    'tree'     => ['$self->{CATALOG}->f_tree', 0],
    'zoom'     => ['$self->{CATALOG}->f_zoom', 0],
    'getsite'  => ['$self->{CATALOG}->f_getsite',0],
    'partitions'  => ['$self->{CATALOG}->f_partitions',0],
    'echo'     => ['$self->{CATALOG}->f_echo', 0],
    'mv'       => ['$self->{CATALOG}->f_mv', 11+16],
    'glob'     => ['$self->{CATALOG}->f_glob', 3],
    'mlconfig' => ['$self->{CATALOG}->f_mlconfig',0],
    'locatesites'    => ['$self->{CATALOG}->f_locatesites',0],
    'type'    => ['$self->{CATALOG}->f_type',0],
    'showcertificates' => ['$self->{CATALOG}->f_showcertificates',0],
     '$?'      => ['$self->{CATALOG}->displayLastError',0],
     'history'=>['$self->history',0],
     'du'=>['$self->{CATALOG}->f_du',3],
	     'md5sum'=>['$self->{CATALOG}->f_getMD5', 3+16+32],
	     'phone'=>['$self->phone', 0],

    #Admin Interface
    'addHost' 		=> ['$self->{CATALOG}->f_addHost', 0],
    'host'    		=> ['$self->{CATALOG}->f_host', 0],
    'addUser' 		=> ['$self->{CATALOG}->f_addUser', 0],
    'mount'   		=> ['$self->{CATALOG}->f_mount', 0],
    'verifyToken' 	=> ['$self->{CATALOG}->f_verifyToken', 0],
    'verifySubjectRole' => ['$self->{CATALOG}->f_verifySubjectRole', 0],
    'moveDirectory'	=> ['$self->{CATALOG}->moveDirectory',0],
	     'moveGUID'	=> ['$self->{CATALOG}->moveGUIDToIndex',0],
    'addSE'		=> ['$self->{CATALOG}->addSE',0],
    'setSEio'		=> ['$self->{CATALOG}->setSEio',0],
    'getSEio'		=> ['$self->{CATALOG}->getSEio',0],
    'resyncLDAP'        => ['$self->{CATALOG}->resyncLDAP',0],
    'calculateFileQuota'        => ['$self->{CATALOG}->calculateFileQuota',0],
    'checkLFN'          => ['$self->{CATALOG}->checkLFN',0],
    'removeExpiredFiles' => ['$self->{CATALOG}->removeExpiredFiles',0],
    'checkOrphanGUID'   => ['$self->{CATALOG}->checkOrphanGUID',0],
    'optimizeGUIDtables'   => ['$self->{CATALOG}->optimizeGUIDtables',0],
	     'setSElimit'=>['$self->{CATALOG}->setSElimit',0],
	     'refreshSERankCache'=> ['$self->{CATALOG}->refreshSERankCache',0],
       
    
    
    #	       'addMethod' =>'f_addMethod', 0],
    #	       'showMethods' =>'f_showMethods', 0],
    #Group Interface
    'groups'  => ['$self->{CATALOG}->f_groups', 0],
    'chgroup' => ['$self->{CATALOG}->f_chgroup', 0],
    'chmod'   => ['$self->{CATALOG}->f_chmod', 1+4+16],
    'umask'   => ['$self->{CATALOG}->f_umask', 0],
    'chown'   => ['$self->{CATALOG}->f_chown', 3+4+16],

    #Tag Interface
    'showTags'       => ['$self->{CATALOG}->f_showTags', 2],
    'addTagValue'    => ['$self->{CATALOG}->f_addTagValue', 0],
    'updateTagValue' => ['$self->{CATALOG}->f_updateTagValue', 0],
    'showTagValue'   => ['$self->{CATALOG}->f_showTagValue', 2 + 64],
    'showAllTagValue'=> ['$self->{CATALOG}->f_showAllTagValues', 2 + 64],
    'removeTag'      => ['$self->{CATALOG}->f_removeTag', 0],
    'removeTagValue' => ['$self->{CATALOG}->f_removeTagValue', 0],
	    'showTagDescription'=> ['$self->{CATALOG}->f_showTagDescription', 67],
    'cleanupTagValue'=> ['$self->{CATALOG}->f_cleanupTagValue',0],

    #File Interface
    'register'     => ['$self->f_registerFile', 0],
    'bulkRegister' => ['$self->{CATALOG}->f_bulkRegisterFile', 0],
    'update'       => ['$self->{CATALOG}->f_updateFile', 0],
    'rm'      	   => ['$self->{CATALOG}->f_removeFile', 3+16],
    'remove'   	   => ['$self->{CATALOG}->f_removeFile', 3+16],
    'rmdir'        => ['$self->{CATALOG}->f_rmdir', 3+16],
    'stat'         => ['$self->{CATALOG}->f_stat', 0],
    'addMirror'    => ['$self->{CATALOG}->f_addMirror', 0],
    'masterCopy'   => ['$self->{CATALOG}->f_masterCopy', 0],
    'deleteMirror' => ['$self->{CATALOG}->f_deleteMirror', 2+64],
    'showMirror'   => ['$self->{CATALOG}->f_showMirror', 3],
    'help'         => ['$self->help', 0],
    'man'         => ['$self->help', 0],
    'debug'        => ['$self->setDebug', 0],
    'silent'       => ['$self->setSilent', 0],
	     'pattern'       => ['$self->pattern', 0],

	     'guid2lfn' => ['$self->{CATALOG}->f_guid2lfn', 3+16+32],
	     'lfn2guid' => ['$self->{CATALOG}->f_lfn2guid', 3+16+32],
	     'touch'              => ['$self->{CATALOG}->f_touch', 3],
	     'version' => ['$self->version', 0],
	     'time' => ['$self->time', 0],
	     'expungeTables' => ['$self->{CATALOG}->expungeTables',0],
	     'filecomplete' =>['file_complete',0],

	    #Triggers
	     'addTrigger' => ['$self->{CATALOG}->f_addTrigger', 0],
	     'showTrigger' => ['$self->{CATALOG}->f_showTrigger', 0],
	     'removeTrigger' => ['$self->{CATALOG}->f_removeTrigger', 0],
	     'setExpired' =>['$self->{CATALOG}->f_setExpired',16+64],
	     #Collections
#	     'removeCollection' => ['$self->{CATALOG}->f_addCollection', 0],
	     'addFileToCollection' => ['$self->{CATALOG}->f_addFileToCollection', 0],
	     'listFilesFromCollection' => ['$self->{CATALOG}->f_listFilesFromCollection', 0],
	     'removeFileFromCollection' => ['$self->{CATALOG}->f_removeFileFromCollection', 2+64],
	     'showStructure' => ['$self->{CATALOG}->f_showStructure', 2],
	     'renumberDirectory' => ['$self->{CATALOG}->f_renumber', 0],
);

sub AddCommands {
    my $self = shift;
    %commands = ( %commands, @_ );
}

my %help_list = 
  (
   'help'     => "\tDisplay this message",
   'pwd'      => "\tDisplay the current directory",
   'cd'       => "\tChange directory",
   'ls'       => "\tList directory",
   'mkremdir' => "Make remote directory",
   'mkdir'    => "Make directory",
   'rmdir'    => "Remove directory",
   'quit'     => "\tExit the program",
   'exit'     => "\tExit the program",
   'whoami'   => "Display the current user",
   'user'     => "\tChange user",
   'chown'    => "Change owner of directory or file",
   'passwd'   => "Change password",
   'find'     => "\tFind files",
   'find'     => "\tFind files and return list of physical file names per file",
   'rm'       => "Remove file from the catalog",
   'remove'   => "Remove file from the catalog",
   'guid2lfn' => "Give the lfn(s) for a guid", 
   'lfn2guid' => "Give the guid for an lfn",
   'checkLFN' => "Verify the consistency of the catalogue (admin operation)",
   
   'addUser'  => "Add user",
   'addSE' => "Define a new SE",
   'glob'     => "Toggle expansion of wildcards",
   'locatesites' => "List all configured sites and geografical location",
   'register' => "\tEnters a new entry in the file catalogue",
   #	       'addMethod' =>"Add method",
   'addHost'     => "Add host",
   'addMirror'   => 'Add an alternative pfn for a file',
   'masterCopy'   => 'Updates the pfn of the lfn, and set the old pfn as a mirror',
   'showMirrors' => "Display possible methods to get a file",
   'host'        => "\tDisplay the current host",
   'groups'      => 'Display the groups of a user',
   'chgroup'     => 'Change the groups of a user',
   'chmod'       => 'Change permissions for a file',
   'umask'       => 'Display or change default creation permitions',
   'cp'          => "\tCopy an entry in the catalog to another place",
   'mv'          => "Rename LFN ",
    'tree' => "\tDisplays teh tree structure under the current directory",
    'zoom' => "\tGoes to the first directory with a file",

    'addTagValue'  => "Gives a value for a tag for a file",
    'updateTagValue' => "Updates the value of a tag for a file",
    'showTags'     => "Displays all the tags of a directory",
    'showTagValue' => "Shows all the values of a file for a tag",
    'showAllTagValue' => "Shows all the values of all the tags for a directory and its subdirectories",
    'removeTagValue'  => "Removes the value of a tag",
    'removeTag'       => "Removes a tag from a directory",
    'echo'            => "\tDisplays a variable from the configuration",
    'debug'           => "Sets the debug level (from 0 to 7)",
    'update'          => "Updates the pfn or the size of an lfn",
    'showMirror'      => "Shows the possible mirrors of an lfn",
    'deleteMirror'    => "Deletes a mirror copy of an lfn",
    'silent'          => "Sets the silent flag on and off",
#    '.'              => "\tExecute your default shell",
    'touch'	      =>"Creates an empty file",
    '$?'	      =>"Print last error code",
    'version'	      =>"\tDisplay the version of the system",
    'history'         =>"\tPrints the last commands",
    'moveDirectory'   =>"\tPuts the directory in another table",
    'time'            => "\tmeasures the time needed to execute a command",
    'pattern'         => "\tGives all the lfn that match a pattern",
    
    'setExpired' => "\tSets the expiration date for a file",
   'phone'=> "\tdisplays the username behinds a userid",
   'type' =>'\treturns the type of lfn (file, directory or collection)',
   'showStructure'=>'\tSee the tables that are under a directory',
);

sub AddHelp {
    my $self = shift;
    %help_list = ( %help_list, @_ );
}

my (@normalUser) = (
    'add',         'cd',         'chown',  'exit',
    'find',        'get',        'help',   'ls',
    'mkdir',       'pwd',        'quit',   'rm',
    'rmdir',       'user',       'whoami', 'groups',
    'chmod',       'umask',      'cat',    'cp',
    'tree',        'zoom',       'addTag', 'showTags',
    'addTagValue', 'showTagValue', 'echo', 'host',
    'register','whereis','history'
);
my $attribs;

=item C<complete($word, $line, $pos)>

This subroutine is used for tab completion. It receives what the user has typed so far, and it returns the possible expansions.

=over

=item Input

=over

=item $word

the characters of the current word that the user is typing

=item $line

All the line that the user has typed

=item $pos

The number of characters

=back

=item Output

A list of possibilities to expand the word. If the user is typing the first word, it will return the list of commands. In the rest of the cases, it will return completion from the file catalogue

One exception is in the case of completion of 'add' and 'register'. These two commands take as a first argument an lfn, and a pfn as the second. For the pfn, the completion does not look at the file catalogue, but at the local disk.

=back


=cut

my $complete = sub {
  my ( $word, $line, $pos ) = @_;

  if ( $pos eq 0 ) {
    my @list=();
    eval {@list=grep ( /^$word/, ( keys %commands ) );};
    @list=sort @list;
    return @list;
  }
  #If we want to complete a pfn, check in the local file system
  if ($line =~ /^\s*(add|register)\s+\S+\s+/) {
    my @matches;
    my $v=$attribs->{filename_completion_function}->($word,0);
    while ($v){
      push @matches, $v;
      $v=$attribs->{filename_completion_function}->($word,1);
    }
    return @matches;
  }
  return file_complete($word);
};


sub file_complete {
  my $word=shift;
  my $path = $catalog->f_complete_path($word);
  $path or return;

  my ($dirname) = $catalog->f_dirname($path);

  $catalog->selectDatabase($dirname) or return;
  my @result=$catalog->{DATABASE}->tabCompletion ($dirname);
  @result = grep (s/^$path/$word/, @result);


  ($#result)
    or ( $result[0] =~ /\/$/ )
      and return ( @result, $result[0] . "." );

  return @result;
  
}



=item C<help([$command])>

Prints information about the commands that can be executed. Without any arguments,
it prints all the available commands. 

If it receives an argument, and the argument is a command, and the command has
some help information, it will display that information

=cut

sub help {
  my $self=shift;
  my $command=shift;
  if ($command) {
    $commands{$command} or $self->info( "Command '$command doesn't exist. Type 'help' to get  a list of all the available commands") and return;
    my $function="\$message=${$commands{$command}}[0]_HELP()";
    my $message;
    if ( eval $function){
      $self->info( $message,0,0);
      return 1;
    }
    if ($@) {
      $DEBUG and $self->debug(1, "FAILED $function and $@");
    }
    $self->info( "The command '$command' doesn't have any extra info");
    return ;
  }
  my ( @commands, %hash );
  @commands = sort keys %commands;
  print STDOUT ("Possible commands:\n");
  map {my $message=$help_list{$_} || "";
       print STDOUT "\t$_ ->\t$message\n" } sort @normalUser;

  print STDOUT "\nAdvanced functions (only for the administrator):\n";

  my @mio = grep { join ( " ", @normalUser ) !~ /\b$_\b/ } keys %commands;

  map {my $message=$help_list{$_} || "";
       print STDOUT "\t$_ ->\t$message\n" } sort @mio;
}

=item C<shell(@command)>

If called with @command, it will execute it in the shell of the user
Otherwise, it will start a shell of the user.

=cut

sub shell {
  my $self = shift;
  my @arg = @_;
  my $shell="/bin/bash";
  open(SHELL,"cat /etc/passwd | grep  \/\$USER: | cut -d \":\" -f 7|");
  while (<SHELL>) {
    $shell = $_;
  }
  close SHELL;
  chomp $shell;
  if ($#arg == -1) {
    system("$shell");
  } else {
    system("$shell -c \"@arg\"");
  }
}

=item C<new($options)>

Creates the object. This subroutine will call C<initialize>, which is usually overloaded by the clases that inherit from it. The possible options are: 

=over

=item debug: debug level, or name of the components that will be debuged

=item exec:command that will be executed.

=back

The options are also passed to the Catalogue object, where more options are also defined (see AliEn::Catalogue)


=cut

sub new {
  my $proto   = shift;
  my $self    = {};
  my $options = shift;
  my $class   = ref($proto) || $proto;
  my $debug    = $options->{debug};
  my $sentence = $options->{exec};

  my $silent = 0;
  ($sentence) and ( $silent = 1 );
 
  bless( $self, $class );
  $self->SUPER::new();

  if(! $options->{no_catalog}) {
      if ($options->{gapi_catalog}) {
	      eval {
	        require gapi::catalogue;
	        };
	      if (! defined $options->{user}) {
	        $self->{CONFIG}=AliEn::Config->new();
	        $options->{user}=$self->{CONFIG}->{LOCAL_USER};
	      }
	      if (! defined $options->{noprompt}) {
	        $options->{noprompt}=1;
	      }
	      if (! defined $options->{nogsi}) {
	        $options->{nogsi}=1;
	      }

	      $self->{CATALOG} = gapi::catalogue->new($options)
	        or return;
	      $self->{CATALOG}->{GLOB} = 1;
      }
      else {
        if($options->{role} =~ /^admin$/) {
          $self->{CATALOG} = AliEn::Catalogue->new($options)
            or return;
        } else {
          $self->{CATALOG} =AliEn::ClientCatalogue->new($options)
            or return;
        }
      }
    }
  
    if ($self->{CATALOG}) {
    $AliEn::UI::catalog=$self->{CATALOG};
    $options->{DATABASE} = $self->{CATALOG}->{DATABASE};
    if (! $options->{gapi_catalog}) {
      $self->{CONFIG}=$self->{CATALOG}->{CONFIG};
    }
  }else {
    $self->{CONFIG}=AliEn::Config->new($options);
  }
  if(! $self->initialize($options)){
    $self->close();
    return;
  }

  $self->{SOAP}=new AliEn::SOAP;
  $self->{GUID}=AliEn::GUID->new();
  
  if ($sentence) {
    $DEBUG and $self->debug(1, "Executing '$sentence'...");

    $self->setSilent(0);
    my ( $command, @arg ) = split ( /\s+/, $sentence );
    $self->execute( $command, @arg );
    $self->setSilent(1);
    $self->close();
    return;
  }

  return $self;
}

=item C<startPrompt()>

Starts an interative prompt. The prompt is based on the Term::ReadLine::GNU, which provides history, hook for tab completion...

=cut

my $term;
sub startPrompt {
  my $self = shift;
	
  my ( $prompt, $line );
  if (!$self->{CATALOG}) {
    $self->info("AliEn was started with '-no_catalog' option. You cannot have the prompt");
    return;
  }
  my $host = $self->{CATALOG}->getHost();
  $catalog=$self->{CATALOG};
  $AliEn::UI::catalog=undef;
  $term    = new Term::ReadLine 'ALIEN';
  $attribs = $term->Attribs;

  $attribs->{completion_function} = $complete;
  $term->OUT;
  $prompt = "[$host] " . $self->{CATALOG}->getDispPath() . " > ";
  my $continue=1;
  my $historyFile= "$ENV{ALIEN_HOME}/.alien.history";	

  $term->read_history($historyFile);
  while ($continue) {
    eval {
      while ( defined( $line = $term->readline( $prompt, "" ) ) ) {
	     $line =~ s/^\s+//;
	
    	 my ( $command, @arg ) = split ( /\s+/, $line );
	
	     ($command) and
	       $self->execute( $command, @arg );
	     $prompt = "[$host] " . $self->{CATALOG}->getDispPath() . " > ";
      }
      $continue=0;
      $term->write_history($historyFile);
    };
    if ($@) {
      $DEBUG and $self->debug(1, "The prompt has died with $@");
    }
  }

}

sub initialize {
  return 1;
}

sub setSilent {
  my $self   = shift;
  my $silent = shift;
  defined $silent or $silent=( 1 - $self->{CATALOG}->{SILENT});

  #    print "SETTING SILENT TO $silent\n";
  ($silent) and $self->{LOGGER}->silentOn();
  ($silent) or $self->{LOGGER}->silentOff();

  $self->{CATALOG} and $self->{CATALOG}->{SILENT} = $silent;
}

sub partSilent{ 
  my $self=shift;
  $self->{CATALOG} and $self->{CATALOG_SILENT}= $self->{CATALOG}->{SILENT};
  $self->{LOG_OLDMODE} = $self->{LOGGER}->getMode();
  $DEBUG and $self->debug(1, "Previous Mode: $self->{LOG_OLDMODE}. Setting to silent");
  $self->setSilent(1);

  return 1;
}

sub restoreSilent{
  my $self=shift;
  $self->{CATALOG} and  $self->{CATALOG}->{SILENT}= $self->{CATALOG_SILENT};
  $self->{LOGGER}->setMinimum(split(" ",$self->{LOG_OLDMODE}));

  $DEBUG and $self->debug(1, "Setting back to : '$self->{LOG_OLDMODE}'");

  return 1;
}


sub setDebug {
  my $self = shift;
  my $silent = (join ("", grep (/^\d*$/, @_)) or 0);
  my @modules =grep (! /^\d*$/, @_);

  $self->{CATALOG} and 
    $self->{CATALOG}->{DEBUG} = $silent;

  if (($silent) or @modules)  {
    @modules and print "Module(s) to debug: @modules\n";
    $self->{LOGGER}->debugOn($silent, @modules);
  } else {
    $self->{LOGGER}->debugOff();
  }
}

sub GetOpts {
  my $self = shift;
  my ( $word, @files, $flags );
  
  $flags = "";
  @files = ();
  
  foreach $word (@_) {
    if ( $word =~ /^-.*/ ) {
      $flags = substr( $word, 1 ) . $flags;
    }
    else {
      @files = ( @files, $word );
    }
  }
  
  $DEBUG and $self->debug(1, "Got options $flags and @files");
  
  return ( $flags, @files );
}

=item C<execute(@command)>

This subroutine passes any command to the underlying Catalogue module. It receives as the first argument the name of the command that it has to execute, and then all the arguments of the command. The return value depends on the function that is being called. 'undefined' always means that something went wrong. 

If one of the arguments is I<-silent>, there will be no output of the command (although it still gives back a return code). 

If one of the arguments is I<-help>, and there is a variable with the name of the method followed by '_HELP', it will print  that message.

You can also redirect the output with '>' to another file in the catalogue. 

=cut

sub execute {
  my $self    = shift;
  my $command = shift;
  my @arg     = grep ( !/-silent/, @_ );

  $DEBUG and $self->debug(1, "Doing execute '$command @_'");
  my $silent = grep ( /-silent/, @_ );
  my $retref = grep ( /-z/, @_ );
  my $help = grep (/^-?-help$/, @_);
  if ($help && $commands{$command}) {
    my $function="${$commands{$command}}[0]_HELP";
    $DEBUG and $self->debug(1, "Checking if $function exists");
    my $message;
    my $com="\$message=$function()";
    if ( eval $com){
      $self->info( $message,0,0);
      return 1;
    };
  }
  my $cnt=0;

  # move "<command> > <file>" to "<command> ><file>"
  foreach(@_) {
    $cnt++;
    if ($_ =~ /^>/) {
      if (defined $_[$cnt]) {
	$_ .= $_[$cnt];
	pop @arg;
      }
    }
  }

  my @stdoutredirect = ();
  $#stdoutredirect = -1;
  @stdoutredirect = grep ( /^\>/ , @_ );
  my $tmpstdout="";
  @arg = grep ( !/^\>/, @arg);

  my $oldmode;

  ($command) or return;

  ($silent) and $self->partSilent();

  if ($command eq "dump") {
      # dumps cannot be redirected!
      $#stdoutredirect =-1;
  }

  if ($#stdoutredirect>-1) {
      $stdoutredirect[0] =~ s/\>//g;
      open SAVE_STDOUT,">&STDOUT";
      open SAVE_STDOUT,">&STDOUT"; #the second is to get rid of the warning
      $tmpstdout = "/tmp/". time() . $$ . rand();
      open STDOUT,"> $tmpstdout";
      open STDOUTTMP,"$tmpstdout";
  }

  my @error=();

  if (my $rcom = $commands{$command}) {
    my @com = @{$rcom};
    my $command;
    if ($com[1] != 0) {
      $DEBUG and $self->debug(1, "Parsing the arguments of the function");
      my $options = "";
      my @newargs=@arg;

      ($com[1] & 2) and  ($options, @newargs)=$self->GetOpts(@arg);

      my ($firstarg, $lastarg);
      $firstarg = (shift @newargs or "") if ($com[1] & 4);
      $lastarg = (pop @newargs or "") if ($com[1] & 8);

      # wildcards!!!
      my $ok=1;
      if ($self->{CATALOG} && $self->{CATALOG}->{GLOB} == 1) {
	if ($com[1] & 16) {
	  my $files=$self->expandWildcards($com[1] & 32,@newargs);
	  if ($files){
	    @newargs=@$files;
	  }else{
	    $ok=0;
	  }
	} else {
	  map  {s/([^\\])\*/$1%/g} @newargs;
	  map  {s/([^\\])\?/$1_/g} @newargs;
	}

	map  {s/\\\?/\?/g} @newargs;
	map  {s/\\\*/\*/g} @newargs;

      }
      if ($ok) {
	push @newargs, undef if ($#newargs == -1);
	
	my $lcommand = "$com[0](";
	$lcommand .= "'$options'," if ($com[1] & 2);
	if ($com[1] & 64 ){
	  #doing a single call with all the entries
	  map  {$_= ((defined $_) ? "'$_', " : "")} @newargs;
	  $command="$lcommand @newargs )";
	  $DEBUG and $self->debug(1, "Executing the command: $command");
	  push @error, eval $command;
	}else {
	  $lcommand .= "'$firstarg'," if ($com[1] & 4);
	  my $rcommand = "";
	  $rcommand .= "'$lastarg'" if ($com[1] & 8);
	  $rcommand .= ")";

	  for (@newargs) {
	    $command = $lcommand . ((defined($_)) ? "'$_'," : "") . $rcommand;
	    $command =~ s/,\)/\)/;
	    $command =~ s/\@/\\\@/g;
	    $DEBUG and $self->debug(1, "Executing  the command: '$command'");
	    push @error, eval $command;
	  }
	}
      }
    } else {
      grep s/"/\\"/g, @arg;
      $command = "$com[0](split \" \", \"@arg\")";
      $command =~ s/\$\?/\\\$\?/g;
      $command =~ s/\@/\\\@/g;
      $DEBUG and $self->debug(1, "Executing the command '$command'");
      push @error, eval $command;
    }


    if ($@) {
      print STDERR "Error executing the AliEn command:  $command $@\n";   # propagate unexpected errors
      if ($@ =~  /We got a ctrl\+c\.\.\./) {
	die($@);
      }
      # timed out
      $silent and $self->restoreSilent();

      if ($#stdoutredirect>-1) {
	unlink $tmpstdout;
	close STDOUTTMP;
	open STDOUT, ">& SAVE_STDOUT";
      }

      return;
    }
  }
  else {
    $self->info( "Unknown command: $command",300,0);
  }

  $silent and $self->restoreSilent();

  if ($#stdoutredirect>-1) {
    my @stdoutoutput = <STDOUTTMP>;
    close STDOUTTMP;
    open STDOUT, ">& SAVE_STDOUT";
    my ($path,$se) = split ('@',$stdoutredirect[0]);
    $se or $se="";
    #print "Path $path Se $se\n";
    if ($path=~/^file\:\/\/(.*)/) {
      system("mv $tmpstdout $1") or print "Output piped into local file $1 !\n";
    } else {
      $self->addFile("$path","$tmpstdout",$se) and print "Output piped into lfn $path !\n";
      #	  $self->aioput("$tmpstdout","$path","$se") and print "Output piped into lfn $path!\n";

      unlink "$tmpstdout";
    }
  }

  if ($self->{CATALOG} and $self->{CATALOG}->{DEBUG} ) {
    $DEBUG and $self->debug(1,Dumper([@error]));
  }
  if ($retref) {
    return \@error;
  }
  return @error;
}



sub expandWildcards {
  my $self=shift;
  my $options=shift;
  my @newargs=();
  my $foundwildcards;
  for (@_) {
    if (s/([^\\])\*/$1%/g or s/([^\\])\?/$1_/g 
	or s/^\*/%/  or s/^\?/_/) {
      $foundwildcards = 1;
      push @newargs, $self->{CATALOG}->ExpandWildcards($_, $options);
    } else {
      push @newargs, $_;
    }
  }
  if ($foundwildcards and $#newargs == -1) {
    print STDERR "File or directory not found\n"; 
    return;
  }
  return \@newargs
}


sub pattern {
  my $self=shift;
  $self->info("Ready to expand @_");
  my $list=$self->expandWildcards(0, @_);
  $list or return;
  my $message=join("\n\t", @$list);
  $self->info("\t$message",0,0);
  return @$list;
}



=item C<close()>

Close the connection to the catalogue.

=cut

sub close {
  my $self = shift;
  if ( $self->{CATALOG} ){
    eval{
      $self->{CATALOG}->f_disconnect;
    };
    if ($@){
      $self->info("The call to disconnect died!! $@");
    }
    undef $self->{CATALOG};
  }
  undef $AliEn::UI::catalog;
}

=item C<version()>

Displays the version of the client

=cut

sub version {
  my $self=shift;
  $self->info( "Version: $self->{CONFIG}->{VERSION}");
  return $self->{CONFIG}->{VERSION};

}

=item C<f_registerFile($lfn, $pfn, $size, $SE, $guid)>

Registers a file in the SE and in the catalogue
Possible options:
    -f: register even if the pfn doesn't exist

=cut

sub f_registerFile {
  my $self = shift;
  my $opt;

  my $options={};

  $self->info("Registering a new file " . join(",",(map ({$_ or ""} @_))) . "\n");
  @ARGV=@_;
  Getopt::Long::GetOptions($options, "silent", "md5=s", "force", "nose")
      or $self->info("Error checking the options of add") and return;
  @_=@ARGV;

  my $file = shift;
  my $pfn  = shift;
  my $size = shift;
  my $destSE = shift || $self->{CONFIG}->{SAVESE_FULLNAME} || $self->{CONFIG}->{SE_FULLNAME};
  my $guid = ( shift or "");
  my $type = (shift or $self->{UMASK});
  $options->{nose} and $destSE="";
  (not $destSE and $pfn ) and
    $self->info("Warning! The SE is not defined, but we are trying to store a $pfn... it won't work"); 
  if ($options->{force}){
    $size or $size=0;
    $pfn or $pfn="";
  } else {
    if ( ! $pfn  ) {
      $self->info("Error in register: not enough arguments\n'register' enters a new entry in the catalogue. It does not copy the pfn to the SE\nUsage register <lfn> <pfn> [<size> [<SE> [<GUID>]]] [-md5 <md5>]
Possible pfns:\tsrm://<host>/<path>, castor://<host>/<path>, 
\t\tfile://<host>/<path>\nIf the method and host are not specified, the system will try with 'file://<localhost>'");
      return;
    }
    $pfn=$self->checkLocalPFN($pfn);

    $DEBUG and $self->debug(1, "Verifying $pfn " );
    if (!  AliEn::SE::Methods->new($pfn)){
      #    if ( !$self->validatePFN( $pfn, $se ) )    {
      return;
    }
    if (! defined $size) {
      $self->info("Trying to get the size of $pfn");
      my $url=AliEn::SE::Methods->new($pfn);
      $url and $size=$url->getSize();
      if (!defined $size) {
	$self->info( "Error getting th size of $pfn");
	return;
      }
    }
  }

  $DEBUG and $self->debug(1, "We added the file to the SE ($guid and $destSE)");

  return $self->{CATALOG}->f_registerFile( $opt, $file, $size, $destSE, $guid, $type, undef,$options->{md5}, $pfn);
}


sub registerFileInSE {
  my $self=shift;
  my $destSE=shift;
  my $guid=shift;
  my $pfn=shift;
  my $size=shift;
  my $options=shift || {};
  my ($newguid, $sename);

  $DEBUG and $self->debug(1, "Ok, let's try to put the entry directly in the database of $destSE");
  my $service=$self->{CONFIG}->CheckServiceCache("SE", ($destSE || $self->{CONFIG}->{SE_NAME}))
    or $self->info( "Error getting the info of $destSE") and return;
  my $db=$service->{DATABASE};
  if (!$db) {
    $db=$self->{CONFIG}->{CATALOGUE_DATABASE};
    $db =~ s{/[^/]*$}{/\Lse_$service->{FULLNAME}\E};
    $db =~ s{::}{_}g;
  }

  $DEBUG and $self->debug(1, "Using $db");
  my ($host, $driver, $dbName)=split ( m{/}, $db);
  my $done;

  if ($self->{CATALOG} && ($host eq $self->{CATALOG}->f_Database_getVar("HOST") 
      && ($driver eq $self->{CATALOG}->f_Database_getVar("DRIVER")))) {
    $DEBUG and $self->debug(1, "We are in the right host. We only have to insert");
    $sename=($destSE || $self->{CONFIG}->{SE_FULLNAME});
    $newguid=$guid;
    if (!$newguid) {
      $newguid=$self->{GUID}->CreateGuid();
    }
    if ($newguid){
      $DEBUG and $self->debug(1, "Ok, we are ready to insert $newguid and $sename and $options->{md5}");
      my $oldmode=$self->{LOGGER}->getMode();
      $DEBUG or $self->{LOGGER}->setMinimum("critical");
      my $insert="INSERT into $dbName.FILES (size, pfn, guid, md5) values(?,?,string2binary(?), ?)";

      if ($self->{CATALOG}->f_Database_do($insert, {bind_values=>[$size,$pfn,$newguid, $options->{md5}]})){
	$DEBUG and $self->debug(1, "File registered in the SE database");
	$done=1;
      }
      $self->{LOGGER}->setMinimum(split(" ",$oldmode));
    }
  }
  
  if (! $done) {
    my $serviceName="SE";
    my $serviceName2=$self->{SE_FULLNAME};
    if ($destSE) {
      ($serviceName, my $secert)=$self->{SOAP}->resolveSEName($destSE)
	or $self->info( "Error getting the endpoint of $destSE")
	  and return;
      $serviceName2=$serviceName;
    }
    my $done=$self->{SOAP}->CallSOAP($serviceName,"registerFile",$serviceName2, $pfn,$size, $guid, $options)
      or return;
    
    $newguid=$done->result()->{guid};
    $sename=$done->result()->{se};
    $newguid or return;
  }

  return ($newguid, $sename);
}

=item C<f_registerFile($lfn, $pfn, $size, $SE, $guid)>

Registers a file in the SE and in the catalogue
Possible options:
    -f: register even if the pfn doesn\'t exist

=cut

#sub f_addMirrorHelp {
#  return "'addMirror' adds a new pfn to an existent entry in the catalogue. It does not copy the pfn to the SE (it assumes that the pfn is already there)\nUsage addMirror <lfn> <SE> [<pfn> [-md5 <md5>]] 
#The SE will first check that it is able to access the copy indicated by pfn. 
#If the pfn is not specified, the system will not contact the SE at all, and it will assume that the entry has already been replicated somehow";
#}
#
#sub f_addMirror {
#  my $self = shift;#
#
#  $self->info("Adding a mirror for the file " . join(",",(map ({$_ or ""} @_))) . "\n");#
#
#  my @args;
#  my $md5;
#  while( my $opt=shift) {
#    if ($opt=~ /^-md5$/){
#      $md5=shift;
#      $md5 or $self->info("Error option md5 needs an argument". $self->f_addMirrorHelp()) and return;
#     next;
#    }
#    push @args, $opt;
#  }
#  @_=@args;
###
#
#  my $file = $self->{CATALOG}->GetAbsolutePath(shift,1);
#  my $destSE = shift;
#  my $pfn  = shift;#
#
#  if ( !$destSE  ) {
#    $self->info("Error in addMirror: not enough arguments\n". f_addMirrorHelp());
#    return;
#  }
#  my $entry=$self->{CATALOG}->checkPermissions("w", $file, undef, {RETURN_HASH=>1}) 
#    or return;
#  ($self->{CATALOG}->isFile( $file, $entry->{lfn})) or
#    $self->info( "Entry $file doesn't exist (or is not a file)",11) 
#      and return; 
  
#  if ($pfn) {
#    (my $newguid, $destSE)=
#      $self->registerFileInSE($destSE, $entry->{guid}, $pfn, $entry->{size}, {md5=>$md5}) or return;
#    $DEBUG and $self->debug(1, "The file has been replicated in $destSE ($newguid)");
#  }
#  $DEBUG and $self->debug(1, "Adding the entry to the catalogue");
#
#  return $self->{CATALOG}->f_addMirror( $file, $destSE, $pfn);
#}

=item C<history()>

Prints the recent history of commands.

=cut

sub history{
  my $self=shift;
  $self->info("Printing the history");
  my $counter=0;
  foreach my $item ($term->GetHistory()){
    print "$counter $item\n";
    $counter++;
  }
  return 1;
}

=item C<time($command, [@arg])>

Measures the time needed for the execution of the '$command @args'

=cut

sub time{
  my $self=shift;
  $DEBUG and $self->debug(1,"Measuring the time that takes to execute @_");
  my $start=[Time::HiRes::gettimeofday];
  my @return=$self->execute(@_);
  $DEBUG and $self->debug(1,"Time finished!!");
  my $time=Time::HiRes::tv_interval ($start);
  $self->info("It took $time seconds to complete '@_'");
  return @return;

}

sub checkLocalPFN {
  my $self=shift;
  my $pfn=shift;
  $pfn =~ s{^file:///}{file://$self->{CONFIG}->{HOST}/};
  ($pfn !~ m{^\w*://}) or return $pfn;
  my $orig=$pfn;
  $pfn=~ m{^/} or $pfn=`pwd`."/$pfn";
  $pfn ="file://$self->{CONFIG}->{HOST}$pfn";
  $pfn =~ s/\n//gs;
  $self->info( "The pfn '$orig' does not look like a pfn... let's hope that it refers to '$pfn'");
  return $pfn;
}
sub phone_HELP{
  return "phone: prints the real name behind an username
Usage:
   phone <username>"
}

sub phone {
  my $self=shift;
  my $userid=shift;
  $userid or $self->info("Error: not enough arguments in phone". $self->phone_HELP()) and return;
  my $user=$self->{CONFIG}->CheckUser($userid) or return;

  $self->debug(2,"Got the user");
  my $message="The user '$userid' is:";
  foreach my $field ('CN','SUBJECT'){
    $user->{$field} and $message .="\n   $field: ". join ("\n\t", @{$user->{"${field}_LIST"}});
  }
  $self->info($message);
  return $user;
}

sub access {
  my $self=shift;
  $self->info("We are going to ask for an envelope");
  my $user=$self->{CONFIG}->{ROLE};
  $self->{CATALOG} and $self->{CATALOG}->{ROLE} and $user=$self->{CATALOG}->{ROLE};

  if($_[0] =~ /^-user=([\w]+)$/)  {
    $user = shift;
    $user =~ s/^-user=([\w]+)$/$1/;
  }
  
  my $info=0;
  for (my $tries = 0; $tries < 5; $tries++) { # try five times
    $info=$self->{SOAP}->CallSOAP("Authen", "createEnvelope", $user, @_) and last;
    $self->info("Sleeping for a while before retrying...");
    sleep(5);
  }
  $info or $self->info("Connecting to the [Authen] service failed!") 
       and return ({error=>"Connecting to the [Authen] service failed!"}); 
  my @newhash=$self->{SOAP}->GetOutput($info);
  if (!$newhash[0]->{envelope}){
    my $error=$newhash[0]->{error} || "";
    $self->info($self->{LOGGER}->error_msg());
    $self->info("Access [envelope] creation failed: $error", 1);
    ($newhash[0]->{exception}) and 
      return ({error=>$error, exception=>$newhash[0]->{exception}});
    return (0,$error) ;
   }
  $ENV{ALIEN_XRDCP_ENVELOPE}=$newhash[0]->{envelope}||"";
  $ENV{ALIEN_XRDCP_URL}=$newhash[0]->{url}||"";
  return (@newhash);
  
}

return 1;

