#!/usr/local/bin/perl  -w
use strict;
use common::sense;
use autodie;
use Cwd;
use Getopt::Long;
use Data::Dumper;

use lib '/usr/local/gkb/modules';
use GKB::DBAdaptor;
use GKB::Config;

use constant CHECK => 'SELECT DB_ID from StableIdentifier WHERE identifier = ?';
use constant FIX => 'UPDATE StableIdentifier SET identifier = ? WHERE identifier = ?';

use constant DEBUG => 0; 

my $user = 'smckay';
my $pass = '1fish2stink';
my $gk_central = 'gk_central';
my $release_db = 'test_slice_53';
my $ghost      = 'reactomecurator';

# DB adaptors
my %dba = get_api_connections(); 

open FIXED, "species_checked.txt" or die $!;

my $fix_st_id   = $dba{history}->prepare(FIX);
my $check_st_id = $dba{history}->prepare(CHECK); 

while (<FIXED>) {
    chomp;
    $_ or next;
    my (undef,$db_id,$class,$species,$spc,$st_id_string,$reason) = split "\t";
    die $_ unless $db_id;
    my $instance   = get_instance($db_id, $release_db);
    my $name       = $instance->displayName;
    my $stable_id  = fetch_stable_id($instance) or die "Did not find stable id for $db_id";
    $stable_id->inflate();
    $st_id_string =~ s/.\d+$//;
    $st_id_string =~ s/R-[A-Z]{3}/R-$spc/;
    my $old_st_id_name = $stable_id->identifier->[0];
    say join("\t",$db_id,$species,$spc,$st_id_string,$old_st_id_name);
    #$fix_st_id->execute($st_id_string,$old_st_id_name);
    $check_st_id->execute($st_id_string);
    my $ok1 = eval{$check_st_id->fetchrow_arrayref->[0]};
    my $ok2 = $stable_id->identifier->[0] eq $st_id_string;
#    $stable_id->identifier($st_id_string);
#    $stable_id->displayName("$st_id_string.1");
#    store($stable_id);
    say join("\t",$stable_id->identifier->[0], $stable_id->displayName);
    say "$st_id_string BOTH CHECKS PASSED" if $ok1 && $ok2;
    say "$st_id_string check1 failed" unless $ok2;
    say "$st_id_string check2 failed" unless $ok2;
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
             $gk_central      => $g_dba,
             'history'        => $s_dbh,
             53               => $r_53
        );
}

sub get_instance {
    my $db_id = shift;
    int $db_id || die "DB_ID ($db_id) must always be an integer";
    my $db    = shift;
    
    my $instance = $dba{$db}->fetch_instance_by_db_id($db_id)->[0];
    return $instance;
}

sub identifier {
    my $instance = shift;
    my $species  = shift;
    $species = abbreviate($species);
    return join('-','R',$species,$instance->db_id());
}

sub abbreviate {
    local $_ = shift;

    my $short_name = uc(join('', /^([A-Za-z])[a-z]+\s+([a-z]{2})[a-z]+$/));
    unless ($short_name) {
	die "NO SHORT NAME FOR $_";
    }
    return $short_name;
}


## failure tolerant(?) wrapper for the GKInstance store and update methods
my $attempt = 1;
sub store {
    my $instance = shift;
    my $action   = 'update';
    my @dbs = @_;

    if ($attempt > 2) {
	warn("Oops, I tried to $action $attempt times, there must be a good reason it failed, giving up");
	$attempt = 1;
	return undef;
    }

    unless (@dbs > 0) {
	@dbs =($gk_central,$release_db,53);
    }
    for my $db (@dbs) {
        my $stored = eval {$dba{$db}->$action($instance)};
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
}
##
######################################################################### 

sub fetch_stable_id {
    my $instance = shift;
    return $instance->attribute_value('stableIdentifier')->[0];
}

sub fetch_species {
    my $instance = shift;
    my $species = $instance->attribute_value('species');
    return undef if @$species == 0;
    my @species = map {$_->displayName} @$species;
    return wantarray ? @species : $species[0];
}


