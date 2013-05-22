package AliEn::JDL;

use strict;
use Switch;
use AliEn::Logger::LogObject;
use vars qw(@ISA);
push @ISA, 'AliEn::Logger::LogObject';

sub new {
  my ($this) = shift;
  my $class = ref($this) || $this;
  my $self = {};

  bless $self, $class;

  $self->SUPER::new() or $self->info("Error constructing SUPER") and return; 

  $self->{JDL} = shift and length $self->{JDL} or $self->info("Error getting JDL source") and return; 

  $self->removeComments();
  
  $self->getKeysAndValues() or return;

  return $self;
}

sub removeComments {
	my $self = shift;
	my @array = split(/\n/, $self->{JDL});
	
	foreach my $line (@array){
		my $full_line = $line;
		$line =~ s/^\s*//g;
		$line =~ s/\s*$//g;
		$line =~ s/\[/\\[/g;
		$line =~ s/\]/\\]/g;
		$full_line =~ s/\[/\\[/g;
		$full_line =~ s/\]/\\]/g;
		$line =~ /^\#/ and $self->{JDL} =~ s/$full_line//g; 
	}
	return 1;
}

sub getKeysAndValues {
	my $self = shift;

	$self->{JDLMAP} = {};
	$self->{JDLMAPTYPES} = {};

	my $iPrevPos = 0;
	my ($idxEqual) = index($self->{JDL}, '=', $iPrevPos+1);
	
	while( $idxEqual > 0 ){	  			
	  my $sKey = substr $self->{JDL}, $iPrevPos, $idxEqual-$iPrevPos;	 
	  
	  $sKey =~ s/^[\s\n]*//g;
	  $sKey =~ s/[\s\n]*$//g;
	 	  
	  my $idxEnd = $idxEqual + 1;
	 	  	  
	  my $bEsc = 0;
	  my $bQuote = 0;
	  my $bClean = 0;	  
	   
	  while ( $idxEnd < length $self->{JDL} ) {
	  	my $c = substr( $self->{JDL}, $idxEnd , 1 );
	  		  		  		  	  		  		  	
	  	switch ($c){
	  	  case '\\' { $bEsc=1-$bEsc; }
	  	  case '"'  { if(!$bEsc) { $bQuote=1-$bQuote; } $bEsc=0; }
	  	  case ';'  { if(!$bEsc && !$bQuote) { $bClean=1; goto outer; } $bEsc=0;  }
	  	  else      { $bEsc=0; }	  	  	         
	  	}    
	  	
	  	$idxEnd++;
	  }
	  outer:
	  
	  $bEsc or $bQuote and $self->info("JDL syntax error: unfinished ".( $bQuote ? "quotes" : "escape" )." in the value of tag $sKey") and return;
	  !$bClean and $self->info("JDL syntax error: Tag $sKey does not finish with a semicolumn") and return;
	  	  
	  my $sValue = substr $self->{JDL}, $idxEqual+1, $idxEnd-$idxEqual-1;
	  $sValue =~ s/^[\s\n]*//g;
	  $sValue =~ s/[\s\n]*$//g;
	  
	  $self->{JDLMAP}->{lc($sKey)}=$sValue;
	  $self->{JDLMAPTYPES}->{lc($sKey)}=$self->parseValue($sValue);
	  	  
	  $iPrevPos = $idxEnd + 1;
	  ($idxEqual)=index($self->{JDL}, '=', $iPrevPos+1);
    }
	
	return 1;
}

sub parseValue {
    my $self = shift;
    my $value = shift; 
    
    my $firstChar = substr $value, 0, 1;
    
    $firstChar =~ /^\"/ and return "string";
    $firstChar =~ /^{/ and return "list";
    $value =~ /^\d+$/ and return "integer";
    $value =~ /^\d+\.\d+$/ and return "double";
    return "expression";
}

sub isOK {
	my $self = shift;
	my $hash = $self->{JDLMAP};
	my @keys = keys %$hash;
	
    foreach my $sKey (@keys){
      my $type = $self->{JDLMAPTYPES}->{$sKey};
      
      $type eq "list" or $type eq "expression" or next;
      my $sValue = $self->{JDLMAP}->{$sKey};
      $sValue =~ s/[\s]//g;
      
      if($type eq "list"){
      	my $patternText = qr"[ \w\/\!\=\+\:\.\,\\\'\@\#\-\>\<\*\(\)]+";  
      	$sValue =~ /^{[\n\s]*\"$patternText\"([\n\s]*,[\n\s]*\"$patternText\")*[\n\s]*}$/ 
      	  or $self->info("Tag $sKey contains incorrectly defined list ( $sValue ). 
      	                  Only alfanumerical, whitespace or [ # ' = @ > - . , : () / * ! ] characters are accepted,
      	                  in the form { \"e1\", \"e2\"... }") 
      	    and return;	
      }
      
      if($type eq "expression"){	
      	my $pCondOp = qr"(&&|\|\|)";
      	my $pQuotedText = qr"\"[ \w\.\=\@\#\:\-\'\,\>\<]+\"";
      	my $pMember = qr/\!?member\([\w\.]+\,$pQuotedText\)/i;
      	my $pMemberB = qr"(\($pMember\)|$pMember)";
      	my $pOther = qr/\!?other\.\w+(\=\=|\!\=|\>|\>\=|\<|\<\=)($pQuotedText|[\d\.]+)/i;    	
        my $pOtherB = qr"(\($pOther\)|$pOther)";
        my $pComponent = qr"($pMemberB|$pOtherB)";
        my $pExpression = qr"($pComponent($pCondOp$pComponent)*|\($pComponent($pCondOp$pComponent)*\)|\($pComponent\)|$pComponent)";
        my $pSentence = qr"($pExpression($pCondOp$pExpression)*|\($pExpression($pCondOp$pExpression)*\))";    
                      	
      	$sValue =~ s/$pSentence//g;
      	$sValue and $self->info("Tag $sKey has incorrect expression ( ".$self->{JDLMAP}->{$sKey}." )") 
      	  and return; 
      }
        
    } 

	return 1;
}

sub asJDL {
	my $self = shift;
	my $jdl = "";
	my $hash = $self->{JDLMAP};
    
    foreach my $sKey (keys %$hash){
    	$jdl.=ucfirst($sKey)." = ".$hash->{$sKey}.";\n";
    }
	
	return $jdl;	
}

sub evaluateAttributeString {
	my $self = shift;
	my $tag = shift;
	#$self->{JDLMAPTYPES}->{$tag} and $self->{JDLMAPTYPES}->{$tag} eq "string"
	$self->{JDLMAP}->{lc($tag)} or return (0,undef);
	my $value = $self->{JDLMAP}->{lc($tag)};
	$value =~ s/^\"//g;  
	$value =~ s/\"$//g;  
	return (1,$value);
}

sub evaluateAttributeInt {
	my $self = shift;
	my $tag = shift;
	#$self->{JDLMAPTYPES}->{$tag} and $self->{JDLMAPTYPES}->{$tag} eq "integer"
	$self->{JDLMAP}->{lc($tag)} or return (0,undef);
	return (1,$self->{JDLMAP}->{lc($tag)});
}

sub evaluateAttributeDouble {
	my $self = shift;
	my $tag = shift;
	#$self->{JDLMAPTYPES}->{$tag} and $self->{JDLMAPTYPES}->{$tag} eq "double" 
	$self->{JDLMAP}->{lc($tag)} or return (0,undef);
	return (1,$self->{JDLMAP}->{lc($tag)});
}

sub evaluateExpression {
	my $self = shift;
	my $tag = shift;
	#$self->{JDLMAPTYPES}->{$tag} and $self->{JDLMAPTYPES}->{$tag} eq "expression"
	$self->{JDLMAP}->{lc($tag)} or return (0,undef);
	return (1,$self->{JDLMAP}->{lc($tag)});
}

sub evaluateAttributeVectorString {
	my $self = shift;
	my $tag = shift;
	
	$self->{JDLMAPTYPES}->{lc($tag)} and $self->{JDLMAPTYPES}->{lc($tag)} eq "list" or return (0,());
	
	my $sValue = $self->{JDLMAP}->{lc($tag)};
	$sValue =~ s/\{//g;
	$sValue =~ s/\}//g;
	$sValue =~ s/\n//g;
	$sValue =~ s/^\s*\"/\"/g;
	$sValue =~ s/\"\s*$/\"/g;
	$sValue =~ s/\"\s*,\s*\"/\",\"/g;	
	 
	my @array = split(/\",/, $sValue);
	foreach (@array){
	  $_ =~ s/\"//g;
	  $_ =~ s/\\'//g;	
	} 
	 
	return (1,@array);
}

sub insertAttributeString {
	my $self = shift;
	my $tag = shift;
	my $value = shift;
	
	$tag and $value or return;
	
	$self->{JDLMAPTYPES}->{lc($tag)} = "string";
	substr $value, 0, 1 =~ /^\"/ or $value = "\"$value\""; 
	$self->{JDLMAP}->{lc($tag)} = $value;
		
	return 1;
}

sub insertAttributeVectorString{
	my $self = shift;
	my $tag = shift;
	my @list = shift;
	
	$tag and scalar(@list)>0 or return;
	
	$self->{JDLMAPTYPES}->{lc($tag)} = "list";
    my $value ="{";
    foreach (@list){
      $value.="\"$_\",";
    }
    $value =~ s/,$//g;
    $value .="}";
	$self->{JDLMAP}->{lc($tag)} = $value;
		
	return 1;
}

sub set_expression {
	my $self = shift;
	my $tag = shift;
	my $value = shift;
	
    $tag and $value or return;
		
	$self->{JDLMAPTYPES}->{lc($tag)} or $self->{JDLMAPTYPES}->{lc($tag)} = $self->parseValue($value);
	$self->{JDLMAP}->{lc($tag)} = $value;
			
	return 1;
}

sub lookupAttribute {
	my $self = shift;
	my $tag = shift;
	
	return defined($self->{JDLMAP}->{lc($tag)});
}


sub Match {
	my $self = shift;
	my $sitejdl = shift;
	my $db = shift;
	my $requirements = $self->evaluateExpression("Requirements");
	my $job_ca = $self;

	use AliEn::Service::Optimizer::Job qw(getJobAgentRequirements);
	my ($req) = AliEn::Service::Optimizer::Job::getJobAgentRequirements(undef, $requirements, $job_ca);
    my ($params) = $db->extractFieldsFromReq($req);

    my ($ok, $value) = $sitejdl->evaluateAttributeInt("Ttl");
    $ok and ( $params->{ttl} < $value or return 0,"Job TTL ($params->{ttl}) higher than in the site ($value)" );	
        
    ($ok, $value) = $sitejdl->evaluateAttributeInt("Localdiskspace");
    $ok and ( $params->{disk} < $value or return 0,"Job disk space ($params->{disk}) higher than in the site ($value)" );
        
    ($ok, $value) = $sitejdl->evaluateAttributeString("Ce");
    $ok or goto noce;
    my @value = split(/::/, $value);
    my $site = $value[1];
    $params->{site} and ( $params->{site}=~/$site/ or return 0,"Job site[s] ($params->{site}) not including CE site ($site)" );
    
    $params->{noce} and $params->{noce}=~/$value/ and return 0,"Job excluded CE[s] ($params->{noce}) including the CE ($value)";
    
    $params->{ce} and ( $params->{ce}=~/$value/ or return 0,"Job demanded CE[s] ($params->{ce}) not including the CE ($value)" );
    
    noce:
    ($ok, my @list) = $sitejdl->evaluateAttributeVectorString("Packages");
    
    $params->{packages} eq '%' and delete $params->{packages};
        
    if( $params->{packages} ){
    	$params->{packages} =~ s/^%,//g;
        $params->{packages} =~ s/,%$//g;
        my @pack = split(',', $params->{packages});
    	$ok or return 0;
    	foreach my $package (@pack){
    		grep(/$package/, @list) or return 0,"Job package ($package) not available in the site packages list (@list)";
    	}
    }
        
    ($ok, @list) = $sitejdl->evaluateAttributeVectorString("Gridpartitions");
    $params->{partition} eq '%' and delete $params->{partition};
   
    $params->{partition} and ($ok and grep(/$params->{partition}/, @list) or return 0,"Job partition ($params->{partition}) not included in the site partitions" );
		
	$requirements =~ s/other.Memory\s*>\s*(\d+)//g and my $memory = $1;
	$memory and ( ($ok, $value) = $sitejdl->evaluateAttributeInt("Memory") and $ok and $value>$memory 
	  or return 0,"Job demanded memory ($memory) bigger or equal than in the site ($value)" );
	  
	$requirements =~ s/other.FreeMemory\s*>\s*(\d+)//g and my $freememory = $1;
	$freememory and ( ($ok, $value) = $sitejdl->evaluateAttributeInt("FreeMemory") and $ok and $value>$freememory 
	  or return 0,"Job demanded free memory ($freememory) bigger or equal than in the site ($value)" );
	  
	$requirements =~ s/other.Swap\s*>\s*(\d+)//g and my $swap = $1;
	$swap and ( ($ok, $value) = $sitejdl->evaluateAttributeInt("Swap") and $ok and $value>$swap 
	  or return 0,"Job demanded swap ($swap) bigger or equal than in the site ($value)" );
	  
    $requirements =~ s/other.FreeSwap\s*>\s*(\d+)//g and my $freeswap = $1;
	$freeswap and ( ($ok, $value) = $sitejdl->evaluateAttributeInt("FreeSwap") and $ok and $value>$freeswap 
	  or return 0,"Job demanded free swap ($freeswap) bigger or equal than in the site ($value)" );  

	#price?

	return 1, "ok";
}

sub MatchTransfer {
	my $self = shift;
	my $sitejdl = shift;

    my ($ok, $value) = $self->evaluateAttributeString("Type");
    my ($ok2, $value2) = $sitejdl->evaluateExpression("Requirements");
    $ok and $ok2 or return 0;
    $value2 =~ /other.type\s*==\s*\"$value\"/i or return 0;         
    
    ($ok, $value) = $self->evaluateExpression("Requirements");
    $ok or return 0;
    $value =~ s/other.type\s*==\s*"([^"]*)"//i and my $type=$1;
    $value =~ s/member\(other.SupportedProtocol,\s*"([^"]*)"\s*\)//si and my $protocol=$1;
    
    $type and $protocol or return 0;
    
    ($ok, my @list) = $sitejdl->evaluateAttributeVectorString("SupportedProtocol");
    $ok and grep(/$protocol/, @list) or return 0;
    
    ($ok, $value) = $sitejdl->evaluateAttributeString("Type");
    $ok and $value eq $type or return 0;   
    
    return 1;
}

return 1;
