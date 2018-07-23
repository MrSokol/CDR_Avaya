#!/usr/bin/perl
package Net::Server::CDR; # Объявляем свой пакет
use strict;
use base qw(Net::Server::MultiType); # Наследуем
use threads;
use Thread::Queue;
use DBI;

sub options {
    my $self = shift;
    my $ref  = $self->SUPER::options(@_);
    my $prop = $self->{'server'};
    $ref->{$_} = \$prop->{$_} for qw(
		DBI_type
		DBI_server
		DBI_database
		DBI_table
		DBI_user
		DBI_password
	);
    return $ref;
}

sub getDB_connect {
	my $self = shift;
    my $prop = $self->{'server'};
	my $db_type = $prop->{'DBI_type'};
	my $db_server = $prop->{'DBI_server'};
	my $db_port = $prop->{'DBI_port'};
	my $db_database = $prop->{'DBI_database'};
	my $db_user = $prop->{'DBI_user'};
	my $db_password = $prop->{'DBI_password'};
	my $dsn = "DBI:mysql:database=$db_database;host=$db_server;port=$db_port";
	$prop->{'DBI_connect'} = DBI->connect($dsn, $db_user, $db_password) if ! $prop->{'DBI_connect'};
	return $prop->{'DBI_connect'};
}

sub getATS_type {
	my $self = shift;
    my $prop = $self->{'server'};
	my $ip = @_[0];
	my $result = $self->getDB_connect()->selectrow_hashref("SELECT id, type FROM ats_info WHERE ip = INET_ATON('$ip') LIMIT 1");
	return %{$result};
}

sub insertData {
	my $self = shift;
	my $sth = $self->getDB_connect()->prepare(@_[0]);
	$sth->execute();
}

sub queue {
	my $self = shift;
    my $prop = $self->{'server'};
	$prop->{'queue'} = Thread::Queue->new() if ! $prop->{'queue'};
	return $prop->{'queue'};
}

sub thread {
	my $self = shift;
    my $prop = $self->{'server'};
	my $q = $self->queue();
	$prop->{'thread'} = threads->create(
		sub {
			while (defined(my $item = $q->dequeue())) {
				my ($ip, $data) = split(/\|/, $item);
				eval {	
					my %ats;
					%ats = $self->getATS_type($ip);
					if ($ats{'type'} eq "Definity") {
						if (length($data) eq 75) {
						$self->log(3, "ID: ".$ats{'id'}." ".$data); # very verbose log
						#----------------
						#Работа с данными
						my %parsedData = $self->DefinityParseLog($data);
						my $table = $prop->{'DBI_table'};
						my $query = qq{INSERT into $table (ats,timestamp,extension,dialednumber,duration,cond,frl) VALUES ($ats{'id'}, FROM_UNIXTIME($parsedData{'DateTime'}), '$parsedData{'clg-num'}', '$parsedData{'dialed-num'}', '$parsedData{'Sec-dur'}', '$parsedData{'cond-code'}', '$parsedData{'frl'}')};
						my $sth = $self->getDB_connect()->prepare($query);
						$sth->execute();
						$self->log(3, "IP: $ip | $query");
						#----------------
						} elsif (length($data) eq 12) {
							$self->log(3, "$ip: Date filtered $data"); # very verbose log
						} else {
							$self->log(2, "$ip: $ats{'type'} Garbage happened: $data"); # very verbose log
						}
					} elsif ($ats{'type'} eq "IPO") {
						my %parsedData = IPOParseLog($data);
						my $table = $prop->{'DBI_table'};
						$parsedData{'Call Start'} =~ s/\//-/g;
						$parsedData{'Connected Time'} = (substr($parsedData{'Connected Time'}, 0, 2) * 3600) + (substr($parsedData{'Connected Time'}, 3, 2) * 60) + substr($parsedData{'Connected Time'}, 6, 2);
						my $query = qq{INSERT into $table (ats,timestamp,extension,dialednumber,duration,cond,frl) VALUES ($ats{'id'}, '$parsedData{'Call Start'}', '$parsedData{'Caller'}', '$parsedData{'Called Number'}', '$parsedData{'Connected Time'}', ' ', ' ')};
						my $sth = $self->getDB_connect()->prepare($query);
						$sth->execute();
						$self->log(3, "IP: $ip | $query");
					} else {
						$self->log(2, "$ip($ats{'type'}): Awesome data $data"); # very verbose log
					}
				};
				if ($@) {
					$self->log(2, "IP: $ip Garbage happened: $@");
				};
			}
		}
	);
}

sub post_bind_hook {
	my $self = shift;
	$self->thread();
}

sub post_accept_hook {
	my $self = shift;
	$self->log(2, $self->log_time . " Connect from IP: " . $self->get_property('peeraddr'));
}

sub pre_server_close_hook {
	my $self = shift;
	$self->queue()->end();
	$self->getDB_connect()->disconnect;
}

sub process_request {
	my $self = shift;
	my $ip = $self->get_property('peeraddr');
	eval {
		while (<STDIN>) {
            s/\x00//g;
			#open( LOG, ">> $ip.txt" );
			my $data = $ip."|".$_;
			$self->queue()->enqueue($data);
			#print LOG $_;
			#close (LOG);
        }
    };
}

sub DefinityParseLog {
	require Time::Local;
	my %parsedData;
	my $self = shift;
	my $result = $_[0];
	my $day=(substr($result,0,2));
	my $month=(substr($result,2,2));
	my $year=(substr($result,4,2));
	my $hour=(substr($result,7,2));
	my $minute=(substr($result,9,2));
	$parsedData{'DateTime'} = Time::Local::timelocal(0,$minute,$hour,$day,$month-1,$year);
	$parsedData{'Sec-dur'} = substr($result,12,5);
	$parsedData{'Sec-dur'} =~ s/^\s+|\s+$//g;
	$parsedData{'clg-num'} = substr($result,18,15);
	$parsedData{'clg-num'} =~ s/^\s+|\s+$//g;
	$parsedData{'in-trk-code'} = substr($result,34,4);
	$parsedData{'in-trk-code'} =~ s/^\s+|\s+$//g;
	$parsedData{'dialed-num'} = substr($result,39,23);
	$parsedData{'dialed-num'} =~ s/^\s+|\s+$//g;
	$parsedData{'cond-code'} = substr($result,63,1);
	$parsedData{'cond-code'} =~ s/^\s+|\s+$//g;
	$parsedData{'vdn'} = substr($result,65,7);
	$parsedData{'vdn'} =~ s/^\s+|\s+$//g;
	$parsedData{'frl'} = substr($result,73,1);
	$parsedData{'frl'} =~ s/^\s+|\s+$//g;
	return %parsedData;
}

sub IPOParseLog {
	my %parsedData;
	my $result = shift;
	my @data=split(/,/,$result);
	$parsedData{'Call Start'} = $data[0];
	$parsedData{'Connected Time'} = $data[1];
	$parsedData{'Ring Time'} = $data[2];
	$parsedData{'Caller'} = $data[3];
	$parsedData{'Direction'} = $data[4];
	$parsedData{'Called Number'} = $data[5];
	$parsedData{'Dialled Number'} = $data[6];
	$parsedData{'Account'} = $data[7];
	$parsedData{'Is Internal'} = $data[8];
	$parsedData{'Call ID'} = $data[9];
	$parsedData{'Continuation'} = $data[10];
	$parsedData{'Party1Device'} = $data[11];
	$parsedData{'Party1Name'} = $data[12];
	$parsedData{'Party2Device'} = $data[13];
	$parsedData{'Party2Name'} = $data[14];
	$parsedData{'Hold Time'} = $data[15];
	$parsedData{'Park Time'} = $data[16];
	$parsedData{'AuthValid'} = $data[17];
	$parsedData{'AuthCode'} = $data[18];
	$parsedData{'User Charged'} = $data[19];
	$parsedData{'Call Charge'} = $data[20];
	$parsedData{'Currency'} = $data[21];
	$parsedData{'Amount at Last User Change'} = $data[22];
	$parsedData{'Call Units'} = $data[23];
	$parsedData{'Units at Last User Change'} = $data[24];
	$parsedData{'Cost per Unit'} = $data[25];
	$parsedData{'Mark Up'} = $data[26];
	$parsedData{'External Targeting Cause'} = $data[27];
	$parsedData{'External Targeter Id'} = $data[28];
	$parsedData{'External Targeted Number'} = $data[29];
	return %parsedData;
}

1;