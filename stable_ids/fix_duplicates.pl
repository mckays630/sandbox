#!/usr/local/bin/perl  -w
use strict;
use common::sense;
use autodie;
use Cwd;
use Getopt::Long;
use Data::Dumper;
use Log::Log4perl qw/get_logger/;

use lib '/usr/local/gkb/modules';
use GKB::DBAdaptor;
use GKB::Config;

# A few bare SQL queries
use constant DB_IDS => 'SELECT DB_ID FROM DatabaseObject WHERE _class = ?';
use constant MAX_ID => 'SELECT MAX(DB_ID) FROM DatabaseObject';
use constant ST_ID  => 'SELECT DB_ID FROM StableIdentifier WHERE identifier = ?';
use constant ALL_ST => 'SELECT DB_ID FROM StableIdentifier';

# a few hard to place species names
use constant SPECIES => {
    'Hepatitis C virus genotype 2a'         => 'HEP',
    'Human herpesvirus 8'                   => 'HER',
    'Molluscum contagiosum virus subtype 1' => 'MCV',
    'Mycobacterium tuberculosis H37Rv'      => 'MTU',
    'Neisseria meningitidis serogroup B'    => 'NME',
    'Influenza A virus'                     => 'FLU',
    'Human immunodeficiency virus 1'        => 'HIV'
};

Log::Log4perl->init(\$LOG_CONF);
my $logger = get_logger(__PACKAGE__);

our($pass,$user,$release_db,$prev_release_db,$gk_central,$ghost,%attached,$release_num,%history,%species);

my $usage = "Usage:\n\t" . join("\n\t", 
				"$0 -sdb slice_db_name -gdb gk_central_db_name -pdb prev_release_db_name \\",
				"-ghost gk_central_db_host  -user db_user -pass db_pass");

GetOptions(
    "user:s"  => \$user,
    "pass:s"  => \$pass,
    "gdb:s"   => \$gk_central,
    "ghost:s" => \$ghost,
    "sdb:s"   => \$release_db,
    "pdb:s"   => \$prev_release_db,
    "release:i" => \$release_num
    );

($release_db && $prev_release_db && $gk_central && $ghost && $user && $pass && $release_num) || die "$usage\n";


# Make sure our requested DBs are slice DBs
check_db_names();

my %st_id_classes = map {$_ => 1} classes_with_stable_ids();

# DB adaptors
my %dba = get_api_connections(); 

# Get list of all instances that have or need ST_IDs
my @db_ids = get_db_ids($release_db);

my $delete = [$dba{$release_db}->prepare("UPDATE DatabaseObject SET StableIdentifier = NULL WHERE DB_ID = ?"),
	      $dba{$gk_central}->prepare("UPDATE DatabaseObject SET StableIdentifier = NULL WHERE DB_ID = ?")]; 

# Evaluate each instance
for my $db_id (@db_ids) {
    my $instance   = get_instance($db_id, $gk_central);
    my $identifier = identifier($instance);
    my $st = $instance->StableIdentifier->[0];
    my $st_db_id = $st->db_id if $st;
    my $st_history_id = has_history($st);

    next if $st && $identifier eq $st->attribute_value('identifier')->[0];

    say ("NO ST_ID for $db_id") and next unless $st; 
    my $name = $st->displayName;
    say ("Removing wrong stable id ($name) for $db_id");

    next;
    say ("deleting ST_instance from $release_db");
    $dba{$release_db}->delete($st);
    
    $st = get_instance($st_db_id, $gk_central);
    if ($st) {
	say ("...and from $gk_central");
	$dba{$gk_central}->delete($st) if $st;
    }
    $st = get_instance($st_db_id, 53);
    if ($st) {
	say ("...and from 53");
	$dba{53}->delete($st) if $st;
    }

    if ($st_history_id) {
	say ('cleaning up stable id database');
	my $sth = $dba{history}->prepare('DELETE FROM StableIdentifier WHERE DB_ID = ?');
	$sth->execute($st_history_id);
	$sth = $dba{history}->prepare('DELETE FROM Changed WHERE ST_ID = ?');
	$sth->execute($st_history_id);
    }
}

#remove_orphan_stable_ids();

# If stable ID exists, return instance.  If not, 
# create and store new ST_ID instance and return that.
sub stable_id {
    my $instance = shift;
    my $identifier = identifier($instance);

    my $st_id = fetch_stable_id($instance);

    unless ( $st_id ) {
	$st_id = create_stable_id($instance,$identifier,1);
    }

    $attached{$st_id->db_id} = $instance;

    add_stable_id_to_history($st_id,$instance);
    return $st_id;
}

sub rename_identifier {
    my $st_id = shift;
    my $identifier = shift;
    $st_id->inflate();

    my $old_id = $st_id->attribute_value('identifier')->[0];
    my $old_version =  $st_id->attribute_value('identifierVersion')->[0];

    $st_id->attribute_value('identifier',$identifier);
    $st_id->attribute_value('identifierVersion',1);
    $st_id->displayName("$identifier.1");
    $st_id->attribute_value('oldIdentifier',$old_id);
    $st_id->attribute_value('oldIdentifierVersion',$old_version);

    store($st_id,'update');
    my $parent = $attached{$st_id->db_id};
    add_stable_id_to_history($st_id,$parent);
    log_renaming($st_id);
}

sub should_be_incremented {
    my $instance = shift;
    my $db_id = $instance->db_id();
    my $prev_instance = get_instance($db_id,$prev_release_db);
    
    unless ($prev_instance) {
	#say ($instance->displayName  . " is new, no increment.\n");
	return 0;
    }

    my $mods2 = @{$instance->attribute_value('modified')} || 0;
    my $mods1 = @{$prev_instance->attribute_value('modified')} || 0;
    if ($mods1 == $mods2) {
	return 0;
    }
    elsif ($mods2 > $mods1) {
	return 1;
    }
    else {
	$logger->warn("Something is fishy with the modifications for instance $db_id");
	return 0;
    }
}

sub check_db_names {
    unless ($prev_release_db =~ /slice/ && $release_db =~ /slice/) {
        die "Both of these databases ($release_db and $prev_release_db) should be slice databases";
    }
}

sub get_api_connections {
    my $r_53 = GKB::DBAdaptor->new(
        -dbname  => 'test_reactome_53',
	-user    => $user,
        -pass    => $pass
	);


    my $r_dba = GKB::DBAdaptor->new(
	-dbname  => $release_db,
	-user    => $user,
	-pass    => $pass
	);

    my $p_dba = GKB::DBAdaptor->new(
	-dbname  => $prev_release_db,
	-user    => $user,
	-pass    => $pass
	);

    my $g_dba = GKB::DBAdaptor->new(
        -dbname  => $gk_central,
	-host    => $ghost,
        -user    => $user,
        -pass    => $pass
	);

    my $s_dbh = DBI->connect(
	"dbi:mysql:stable_identifiers",
	$user,
	$pass
	);

    return ( $release_db      => $r_dba,
	     $prev_release_db => $p_dba,
	     $gk_central      => $g_dba,
	     'history'        => $s_dbh,
	     53               => $r_53
	);
}

sub get_db_ids {
    my $sth = $dba{$gk_central}->prepare(DB_IDS);
    my @db_ids;
    for my $class (classes_with_stable_ids()) {
	$sth->execute($class);
	while (my $db_id = $sth->fetchrow_array) {
	    push @db_ids, $db_id;
	} 
    }
    return @db_ids;
}



#####################################################################
##  This set of functions deals with the stable identifier history db
sub has_history {
    my $instance = shift;
    my $identifier = $instance->attribute_value('identifier')->[0]; 
    
    if ($history{$identifier}) {
	return  $history{$identifier};
    }

    my $dbh = $dba{history};
    my $sth = $dbh->prepare("SELECT DB_ID FROM StableIdentifier WHERE identifier = ?");
    $sth->execute($identifier);
    my $ary = $sth->fetchrow_arrayref || [];
    my $db_id = $ary->[0];

    if ($db_id) {
	$history{$identifier} = $db_id;
	return $db_id;
    }
    else {
	return undef;
    }
}

sub add_stable_id_to_history {
    my $instance  = shift;
    my $parent    = shift;
    my $parent_id = $parent ? $parent->db_id : 'NULL';

    my $history_id = has_history($instance);
    if ($history_id) {
	return $history_id;
    }

    my $dbh = $dba{history};
    my $identifier = $instance->attribute_value('identifier')->[0];
    my $version = $instance->attribute_value('identifierVersion')->[0];

    # We need a parent ID for all non-orphans, try to get one if it is missing
    # The parent DB_ID unifies all ST_IDs for an event
    unless ($parent_id) {
        ($parent_id) = $identifier =~ /R-\S{3}-(\d+)$/;
    }

    # It is possible this stable ID has been used before.  If so,
    # we will pick up the last known version 
    my $sth = $dbh->prepare("SELECT DB_ID, identifierVersion FROM StableIdentifier WHERE identifier = ?");
    $sth->execute($identifier);
    my ($st_db_id,$st_version) = eval{@{$sth->fetchrow_arrayref}};
    
    # This means we will revive the old stable ID
    if ($st_db_id) {
	# bump the version
	$instance->inflate();
	$instance->attribute_value('identifierVersion',$st_version + 1);
	store($instance,'update');
	log_reactivation($instance);

	if ($parent_id) {
	    $sth =  $dbh->prepare("UPDATE StableIdentifier SET instanceId = ? WHERE DB_ID = ?");
	    $sth->execute($parent_id,$st_db_id);
	}

	return $st_db_id;
    }
    else {
	$sth = $dbh->prepare('INSERT INTO StableIdentifier VALUES (NULL, ?, ?, ?)');
	$sth->execute($identifier,$version,$parent_id);

	$sth = $dbh->prepare("SELECT DB_ID FROM StableIdentifier WHERE identifier = ?");
	$sth->execute($identifier);

	my $db_id = eval{$sth->fetchrow_arrayref->[0]};
	
	if ($db_id) {
	    $history{$identifier} = $db_id;
	    log_creation($instance) if $identifier =~ /R-[A-Z]{3}-\d+/; # only create new ones
	}
	else {
	    return undef;
	}
    }
}

sub log_renaming {
    add_change_to_history(@_,'renamed');
}

sub log_deletion {
    add_change_to_history(@_,'deleted');
}

sub log_creation {
    add_change_to_history(@_,'created');
}

sub log_incrementation {
    add_change_to_history(@_,'incremented');
}

sub log_reactivation {
    add_change_to_history(@_,'reactivated');
}

sub add_change_to_history {
    my ($st_id,$change) = @_;
    my $parent = $attached{$st_id->db_id};
    #say ("Logging $change event for " . $st_id->displayName . " in history database\n");
    my $st_db_id = has_history($st_id,$parent) || add_stable_id_to_history($st_id,$parent);
    my $dbh = $dba{history};
    my $sth = $dbh->prepare('INSERT INTO Changed values (NULL, ?, ?, ?, NOW())');
    $sth->execute($st_db_id,$change,$release_num);
}
##
##################################################################### 

sub get_instance {
    my $db_id = int shift || die "DB_ID must always be an integer";
    my $db    = shift;
    
    my $instance = $dba{$db}->fetch_instance_by_db_id($db_id)->[0];
    return $instance;
}

sub classes_with_stable_ids {
    # derived from:
    # select distinct _class from DatabaseObject where StableIdentifier is not null 
    qw/
    Pathway SimpleEntity OtherEntity DefinedSet Complex EntityWithAccessionedSequence GenomeEncodedEntity
    Reaction BlackBoxEvent PositiveRegulation CandidateSet NegativeRegulation OpenSet Requirement Polymer
    Depolymerisation EntitySet Polymerisation FailedReaction
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

sub identifier {
    my $instance = shift;
    my $species = species($instance);
    return join('-','R',$species,$instance->db_id());
}

sub species {
    my $instance = shift;
    my $name = $instance->displayName;
    return $species{$name} if $species{$name};
    my $long = make_decision_on_species($instance);
    $species{$name} = abbreviate($long);
    return $species{$name};
}

sub abbreviate {
    local $_ = shift;
    return $_ if /ALL|NUL/;

    # an instance?
    $_ = $_->displayName if ref($_);

    my $other_species = SPECIES;

    my $short_name = uc(join('', /^([A-Za-z])[a-z]+\s+([a-z]{2})[a-z]+$/));
    unless ($short_name) {
	if (/Bacteria/) {
	    $short_name = 'BAC';
	}
	elsif (/Virus/) {
            $short_name = 'VIR';
        }
	else {
	    $short_name = $other_species->{$_} || 'NUL';
	}
	#say ("Set short name for '$_' to $short_name\n");
    }
    return $short_name;
}

# Make a new ST_ID instance from scratch
sub create_stable_id {
    my ($instance,$identifier,$version,$db_id) = @_;
    $instance->inflate();

    $db_id ||= new_db_id($gk_central);
    my $st_id = $dba{$gk_central}->instance_from_hash({},'StableIdentifier',$db_id);
    set_st_id_attributes($st_id,$identifier,$version);

    #say ("creating new ST_ID " . $st_id->displayName . " for " . $instance->displayName);
    
    store($st_id,'store');
    add_stable_id_to_history($st_id,$instance);
    
    # Attach the stable ID to its parent instance
    $instance->stableIdentifier($st_id);
    store($instance,'update');

    return $st_id;
}


#########################################################################
## failure tolerant(?) wrapper for the GKInstance store and update methods
sub store {
    my $instance = shift;
    my $action   = shift;
    my @dbs = @_;

    my $force = $action eq 'store' ? 1 : 0;

    unless (@dbs > 0) {
	@dbs =($gk_central,$release_db);
    }
    for my $db (@dbs) {
        my $stored = eval {$dba{$db}->$action($instance,$force)};
	unless ($stored) {
	    $logger->warn("Oops, the $action operation failed for $db:\n$@_\nI'll try again!");
	    sleep 1;
            store($instance,$action,$db);
	}
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
    my $sth = $dba{$db}->prepare(MAX_ID);
    $sth->execute;
    my $max_db_id = $sth->fetchrow_arrayref->[0];
    return $max_db_id;
}

# Get the largest DB_ID from slice or gk_central
sub new_db_id {
    my $max_id = 0;
    for my $db ($gk_central,$release_db) {
	my $id = max_db_id($db);
	$max_id = $id if $id > $max_id;
    }
    return $max_id + 1;
}


sub fetch_species {
    my $instance = shift;
    my $species = $instance->attribute_value('species');
    return undef if @$species == 0;
    my @species = map {$_->displayName} @$species;
    return wantarray ? @species : $species[0];
}

# Hopefully not-too-compicated reasoner to deal with entities that lack a species
sub make_decision_on_species {
    my $instance = shift;
    my $class = $instance->class;
    my @all_species = fetch_species($instance);
    my $species = $all_species[0];
    my $last_species  = $all_species[-1];
    
    # Regulator?  Get last species if applicable
    if ($class =~ /regulation|requirement/i) {
	$species = $last_species || $species;
	unless ($species) {
	    #say ("Looking for species of pathways or regulators for this $class\n");
	    my @entities = @{$instance->attribute_value('containedinPathway')};
	    push @entities, @{$instance->attribute_value('regulator')};
	    for my $entity (@entities) {
		#say ("Checking species for ".$entity->displayName);
		$species = fetch_species($entity);
		#say ("No species found") unless $species;
		last if $species;
	    }
	    $species ||= 'ALL';
	}
    }
    elsif ($class =~ /SimpleEntity|Polymer/) {
	$species ||= 'ALL';
    }
    elsif (!$species && $class eq 'Complex') {
	my $members = $instance->attribute_value('hasComponent');
	while (!$species && @$members >0) {
            my $member = shift @$members;
            $species = $member->attribute_value('species')->[0];
        }
    }
    elsif (!$species && $class =~ /Set/) {
	my $members = $instance->attribute_value('hasMember');
	while (!$species && @$members > 0) {
	    my $member = shift @$members;
            $species = $member->attribute_value('species')->[0];
        }
    }
    else {
	$species ||= 'NUL';
    }
    
    #say (join("\t","SPECIES",$class,$species,abbreviate($species))."\n");
    return $species;
}
