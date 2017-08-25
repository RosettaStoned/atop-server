#!/usr/bin/perl -w

use strict;
use warnings;
use AtopServer;
use App::Daemon qw( daemonize );

#daemonize();

my $atop = AtopServer->new();
$atop->start();

my $cv = AnyEvent->condvar();
$cv->recv();

