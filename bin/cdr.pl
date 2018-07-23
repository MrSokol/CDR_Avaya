#!/usr/bin/perl

use strict;
use warnings;
use FindBin qw($Bin);
use lib "$Bin/../lib"; 

use Net::Server::CDR;
# Создаем экземпляр сервера
our $server = Net::Server::CDR->new(conf_file => "$Bin/../etc/cdr.cfg"); 
$server->run();
