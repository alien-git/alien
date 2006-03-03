package AliEn::Monitor;

sub new {
    my ($class, $method, @params) = @_;
    my $this = {};
    bless $this;
    
    $method 
	or print("Monitor: no method given") 
	and return;
    $method = "AliEn::Monitor::$method";
    eval "require $method"  
	or print("Monitor: Method $name does not exist $! and $@")
	and return;

    $this->{METHOD} = new $method(@params);
    $this->{METHOD}
	or print("Monitor: Error getting instance of $method")
	and return;
}

sub sendParameters {
    my ($this, @params) = @_;
    
    $this->{METHOD}->sendParameters(@params);
}

sub sendParams {
    my ($this, @params) = @_;

    $this->{METHOD}->sendParams(@params);
}

sub setJobPID {
    my ($this, @params) = @_;

    $this->{METHOD}->setJobPID(@params);
}

sub setJobWorkDir {
    my ($this, @params) = @_;

    $this->{METHOD}->setJobWorkDir(@params);
}

sub setConfCheck {
    my ($this, @params) = @_;

    $this->{METHOD}->setConfCheck(@params);
}

sub setConfRecheckInterval {
    my ($this, @params) = @_;

    $this->{METHOD}->setConfRecheckInterval(@params);
}

sub setMonitorClusterNode {
    my ($this, @params) = @_;

    $this->{METHOD}->setMonitorClusterNode(@params);
}

sub setJobMonitorClusterNode {
    my ($this, @params) = @_;

    $this->{METHOD}->setJobMonitorClusterNode(@params);
}

1;
