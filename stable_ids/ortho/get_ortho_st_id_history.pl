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

my $usage = "Usage: $0 -user user -pass pass -db test_release_XX\n";

GetOptions(
    "user:s"  => \$user,
    "pass:s"  => \$pass,
    "db:s"    => \$release_db
    );

($release_db && $user && $pass) || die $usage;

my %st_id_classes = map {$_ => 1} classes_with_stable_ids();

# DB adaptors
(my $slice_db = $release_db) =~ s/reactome/slice/;
my %dba = get_api_connections(); 

# Get list of all instances that have or need ST_IDs
my @db_ids = get_db_ids($release_db);

# Evaluate each instance
for my $db_id (@db_ids) {
    my $instance   = get_instance($db_id, $release_db);

    # test for orthoinference 1: has parent in slice
    my $parent = $instance->attribute_value('inferredFrom')->[0];
    $parent = get_instance($parent->db_id,$slice_db) if $parent;
    next unless $parent;
    
    # test for orthoinference 2: not in slice database
    my $in_slice = get_instance($db_id, $slice_db);
    next if $in_slice;

    my $st_id = eval {$instance->attribute_value('stableIdentifier')->[0]->identifier->[0]};
    $st_id || next;

    # test for orthoinference 4: is not a human event
    my $species = species($instance);
    next unless $species ne 'HSA';


    $instance->inflate();

#    my @attributes = ($species, 
#		      $parent->db_id,
#		      map {$_->displayName} 
		      #@{$instance->input},
		      #@{$instance->output},
		      #@{$instance->hasEvent},
		      #@{$instance->representedInstance},
#		      @{$instance->compartment},
#		      @{$instance->catalystActivity}
#		      );

#    say Dumper \@attributes;
    say join("\t",
	     $st_id,
	     $db_id,
	     $instance->class,
	     $instance->displayName,
	     $species,
	     $parent->displayName,
	     $parent->db_id);#, md5_hex(@attributes));
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
    # derived from:
    # select distinct _class from DatabaseObject where StableIdentifier is not null 
    #return ('EntityWithAccessionedSequence','ReferenceGeneProduct');
    qw/
    Pathway SimpleEntity OtherEntity DefinedSet Complex EntityWithAccessionedSequence GenomeEncodedEntity
    Reaction BlackBoxEvent PositiveRegulation CandidateSet NegativeRegulation OpenSet Requirement Polymer
    Depolymerisation EntitySet Polymerisation FailedReaction ReferenceGeneProduct
    /;
}

# Add the necessary attributes to our stable ID instance 
sub set_st_id_attributes {
    my ($instance,$identifier,$version) = @_;
    $instance->attribute_value('identifier',$identifier);
    $instance->attribute_value('identifierVersion',$version);
    $instance->attribute_value('_class','StableIdentifier');
    $instance->attribute_value('_displayName',"$identifier.$version");
}

sub species {
    my $instance = shift;
    my $name = $instance->displayName;
    #return $species{$name} if $species{$name};
    my $long = eval{$instance->attribute_value('species')->[0]->displayName};
    $long or return undef;
    $species{$name} = abbreviate($long);
    return $species{$name};
}

sub abbreviate {
    local $_ = shift;
    my $short_name = uc(join('', /^([A-Za-z])[a-z]+\s+([a-z]{2})[a-z]+$/));
    return $short_name;
}

# Make a new ST_ID instance from scratch
sub create_stable_id {
    my ($instance,$identifier,$version) = @_;

    my $db_id = new_db_id($release_db);
    my $st_id = $dba{$release_db}->instance_from_hash({},'StableIdentifier',$db_id);
    set_st_id_attributes($st_id,$identifier,$version);

    say("creating new ST_ID " . $st_id->displayName . " for " . $instance->displayName);
    
    store($st_id,'store');
    
    # Attach the stable ID to its parent instance
    $instance->inflate();
    $instance->stableIdentifier($st_id);
    store($instance,'update');

    return $st_id;
}


#########################################################################
## failure tolerant(?) wrapper for the GKInstance store and update methods
sub store {
    my $instance = shift;
    my $action   = shift;

    say("Performing $action operation for ".$instance->displayName);

    my $force = $action eq 'store' ? 1 : 0;

    my $stored = eval {$dba{$release_db}->$action($instance,$force)};
    unless ($stored) {
	warn("Oops, the $action operation failed:\n$@_\nI'll try again!");
	sleep 1;
	store($instance,$action);
    }

}
##
######################################################################### 

sub fetch_stable_id {
    my $instance = shift;
    return $instance->attribute_value('stableIdentifier')->[0];
}

sub max_db_id {
    my $db = shift;
    my $sth = $dba{$db}->prepare('SELECT MAX(DB_ID) FROM DatabaseObject');
    $sth->execute;
    my $max_db_id = $sth->fetchrow_arrayref->[0];
    return $max_db_id;
}

# Get the largest DB_ID from the release database
sub new_db_id {
    my $max_id = max_db_id($release_db);
    return $max_id + 1;
}

