#!/usr/local/bin/perl  -w
use strict;
use common::sense;
use autodie;
use Cwd;
use Getopt::Long;
use Data::Dumper;
use Log::Log4perl qw/get_logger/;
use Digest::MD5 'md5_hex';

use lib '/usr/local/gkb/modules';
use GKB::DBAdaptor;
use GKB::Config;

$| = 1;

Log::Log4perl->init(\$LOG_CONF);
my $logger = get_logger(__PACKAGE__);

our($pass,$user,$release_db,$slice_db,%seen_id,%species);

my $usage = "Usage: $0 -user user -pass pass -db test_reactome_XX\n";

GetOptions(
    "user:s"  => \$user,
    "pass:s"  => \$pass,
    "db:s"    => \$release_db
    );

($release_db && $user && $pass) || die $usage;

# DB adaptors
(my $slice_db = $release_db) =~ s/reactome/slice/;
my %dba = get_api_connections(); 

my %st_id_classes = map {$_ => 1} classes_with_stable_ids();

# Get list of all curated instances (from slice_db) that have ST_IDs
my @db_ids = get_db_ids($slice_db);

my ($human,%human);
my $total;
my $missing;

# Evaluate each instance
for my $db_id (@db_ids) {
    my $instance   = get_instance($db_id, $release_db);
    $instance or next;

    my $stable_id = has_stable_id($instance);
    next unless $stable_id;
    next unless $stable_id->displayName =~ /R-HSA/;

    my $ortho_events = $instance->attribute_value('orthologousEvent');
    push @$ortho_events, @{$instance->attribute_value('inferredTo')};
    next unless $ortho_events->[0];

    $human{$instance->class}++;
    $human++;
    
    for my $ortho (@$ortho_events) {
	#say $ortho->class, " REF: ", ref $ortho;;
	if (!ref($ortho) && $ortho =~ /^\d+$/) {
	    say Dumper [$ortho];
	    $ortho = get_instance($ortho, $release_db);
	}
	if (my $st_id = has_stable_id($ortho)) {
	    #say "\t",$st_id->displayName;
	    $total++;
	}
	else {
	    $missing++;
	    say join("\t","NO ST_ID!", $ortho->class, $ortho->db_id);
	}
    }

#    say $stable_id->displayName, "\t", $instance->class;
}

say "I count a total of $human human things with $total ortho-stable_ids; $missing missing ortho stable ids";

print Dumper \%human;


sub has_stable_id {
    my $instance = shift;
    return $instance->attribute_value('stableIdentifier')->[0];
}

sub get_api_connections {

    return 
	( $release_db => GKB::DBAdaptor->new(
	  -dbname  => $release_db,
	  -user    => $user,
	  -pass    => $pass
	  ),
	  $slice_db => GKB::DBAdaptor->new(
	      -dbname  => $slice_db,
	      -user    => $user,
	      -pass    => $pass
	  )
	);
}

sub get_db_ids {
    my $sth = $dba{$release_db}->prepare('SELECT DB_ID FROM DatabaseObject WHERE _class = ?');
    my @db_ids;
    for my $class (classes_with_stable_ids()) {
	$sth->execute($class);
	while (my $db_id = $sth->fetchrow_array) {
	    push @db_ids, $db_id;
	} 
    }
    return @db_ids;
}

sub get_instance {
    my $db_id = int shift || die "DB_ID must always be an integer";
    my $db    = shift;
    my $instance = $dba{$db}->fetch_instance_by_db_id($db_id)->[0];
    return $instance;
}

sub classes_with_stable_ids {
    my $sth = $dba{$slice_db}->prepare('select distinct _class from DatabaseObject where StableIdentifier is not null');
    $sth->execute();
    my $classes = $sth->fetchall_arrayref();
    my @classes = map {@$_} @$classes;
    return @classes;
}

sub fetch_stable_id {
    my $instance = shift;
    return $instance->attribute_value('stableIdentifier')->[0];
}

