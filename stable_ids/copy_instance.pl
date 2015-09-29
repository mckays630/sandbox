#!/usr/local/bin/perl  -w
use strict;
use common::sense;
use Getopt::Long;
use Data::Dumper;

use lib '/usr/local/gkb/modules';
use GKB::DBAdaptor;
use GKB::Config;

our($pass,$user,$from_db,$to_db,$db_id);

my $usage = "Usage:\n\t$0 -from_db db_to_copy_from -to_db db_to_copy_to -user db_user -pass db_pass -instance db_id";

GetOptions(
    "user:s"  => \$user,
    "pass:s"  => \$pass,
    "from_db:s" => \$from_db,
    "to_db:s"   => \$to_db,
    "instance:i" => \$db_id
    );

($db_id && $from_db && $to_db && $user && $pass) || die "$usage\n";

# DB adaptors
my %dba = get_api_connections(); 

# Evaluate each instance
my $from_instance  = get_instance($db_id, $from_db);
my $to_instance    = get_instance($db_id, $to_db);

unless ($from_instance) {
    die "I could not find a GKIstance for $db_id";
}

if ($to_instance) {
#    $to_instance->inflate();
#    say "In order to copy the instance from $from_db to $to_db, I have to delete it from $to_db first.\nDoing that now";
#    store($to_instance,'delete',$to_db);
}

$from_instance->inflate();

my $operation = $to_instance ? 'update' : 'store';
say "I will operation";
store($from_instance,$operation,$to_db);

sub get_api_connections {
    return (
	$from_db => GKB::DBAdaptor->new(
	    -dbname  => $from_db,
	    -user    => $user,
	    -pass    => $pass
        ),
	
	$to_db => GKB::DBAdaptor->new(
            -dbname  => $to_db,
            -user    => $user,
            -pass    => $pass
        )
	);
}

sub get_instance {
    my $db_id = int shift || die "DB_ID must always be an integer";
    my $db    = shift;
    say "hello there $db_id";
    my $instance = $dba{$db}->fetch_instance_by_db_id($db_id)->[0];
    return $instance;
}

#########################################################################
## failure tolerant(?) wrapper for the GKInstance store and update methods
my $attempt = 1;
sub store {
    my $instance = shift;
    my $action   = shift || 'undefined';
    my $db       = shift || die ('No Database specified for $action operation');

    if ($attempt > 2) {
	warn("Oops, I tried to $action $attempt times, there must be a good reason it failed, giving up");
	$attempt = 1;
	return undef;
    }

    my $force = $action eq 'store' ? 1 : 0;

    my $stored = eval {$dba{$db}->$action($instance,$force)};
    unless ($stored) {
	warn("Oops, the $action operation (attempt $attempt) failed for $db:\n$@_\nI'll try again!");
	sleep 1;
	$attempt++;
	store($instance,$action,$db);
    }
    else {
	$attempt = 1;
    }
}


