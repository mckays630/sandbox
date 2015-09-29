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
use constant ATTACHED => 'SELECT DB_ID FROM DatabaseObject WHERE StableIdentifier = ?';

use constant DEBUG => 0; 

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

our($pass,$user,$release_db,$prev_release_db,$gk_central,$ghost,$release_num,%history,%species,%attached);

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

($release_db && $prev_release_db && $gk_central && $ghost && $user && $pass && $release_num) || say "$usage\n";

say '';

my %st_id_classes = map {$_ => 1} classes_with_stable_ids();

# DB adaptors
my %dba = get_api_connections(); 

# Get list of all instances that have or need ST_IDs
my @db_ids = get_db_ids($release_db);

my %seen_bad;
while (<DATA>) {
    chomp;
    $seen_bad{$_}++;
}
my $decision;

# Evaluate each instance
for my $db_id (@db_ids) {
    my $instance   = get_instance($db_id, $release_db);
    $instance->inflate();
    my $class      = $instance->class;
    my $name       = $instance->displayName;
    my $stable_id  = fetch_stable_id($instance)->displayName;

    $decision = '';
    my $species    = make_decision_on_species($instance,$db_id);
    my $abbreviate = abbreviate($species);
    
    #say "This one is OK $stable_id" and next if $stable_id =~ /R-$abbreviate/;
    my $OK = $stable_id =~ /R-$abbreviate/ ? 'OK' : 'BAD';
    my $seen = $seen_bad{$db_id} ? 'SEEN' : '';
    say join("\t","STABLE_ID",$db_id,$class,$species,$abbreviate,$stable_id,$decision,$seen,$OK) if $OK eq 'BAD';

}


sub get_api_connections {
    my $r_dba = GKB::DBAdaptor->new(
        -dbname  => $release_db,
        -user    => $user,
        -pass    => $pass
        );
    my $s_dbh = DBI->connect(
        "dbi:mysql:stable_identifiers",
        $user,
        $pass
        );

    return ( $release_db      => $r_dba,
             'history'        => $s_dbh,
        );
}

sub get_db_ids {
    my $sth = $dba{$release_db}->prepare(DB_IDS);
    my @db_ids;
    for my $class (classes_with_stable_ids()) {
	$sth->execute($class);
	while (my $db_id = $sth->fetchrow_array) {
	    push @db_ids, $db_id;
	} 
    }
    return @db_ids;
}


sub increment_stable_id {
    my $instance = shift;
    $instance->inflate();

    my $identifier =  $instance->attribute_value('identifier')->[0];
    my $version  = $instance->attribute_value('identifierVersion')->[0];
    my $new_version = $version + 1;

    $logger->info("Incrementing ".$instance->displayName." version from $version to $new_version\n");

    $instance->attribute_value('identifierVersion',$new_version);
    $instance->displayName("$identifier.$new_version");
    
    store($instance,'update');
    log_incrementation($instance);

    my $sth = $dba{history}->prepare("UPDATE StableIdentifier SET identifierVersion = $new_version");
    $sth->execute();
}

sub attached {
    my $st_id = shift;
    my $parent = shift;

    if ($parent) {
	$attached{$st_id} = $parent;
	return $parent;
    }

    if ($attached{$st_id}) {
	return $attached{$st_id};
    }

    my $sth = $dba{$release_db}->prepare(ATTACHED);
    $sth->execute($st_id);
    while (my $result = $sth->fetchrow_arrayref) {
	my $id = $result->[0];
	if ($id) {
	    $parent = get_instance($id,$dba{$release_db});
	    if ($parent) {
		$attached{$st_id} = $parent;
		return $parent;
	    }
	}
    }
    return undef;
}

# If the stable ID is not attached to an event, put it in the attic
sub remove_orphan_stable_ids {
    my $sth = $dba{$gk_central}->prepare(ALL_ST);
    $sth->execute();
    while (my $res = $sth->fetchrow_arrayref) {
	my $db_id = $res->[0] || next;
	next if attached($db_id);

	my $deleted;
	for my $db ($release_db,$gk_central) {
	    my $st_id =  get_instance($db_id,$db) || next;

	    my $history_id = add_stable_id_to_history($st_id);
	    #$history_id || Bio::Root::Root->throw("Problem getting/setting history for " . $st_id->displayName());

	    log_deletion($st_id) unless $deleted;

	    $logger->info("Deleting orphan stable identifier ".$st_id->displayName."\n");

	    $dba{$db}->delete($st_id);
	    $deleted++;
	}
    }
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
       # ($parent_id) = $identifier =~ /R-\S{3}-(\d+)$/;
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
    my $parent = attached($st_id->db_id);
    $logger->info("Logging $change event for " . $st_id->displayName . " in history database\n");
    my $st_db_id = has_history($st_id,$parent) || add_stable_id_to_history($st_id,$parent) || return;
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
    ('Complex');
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
	$logger->info("Set short name for '$_' to $short_name\n");
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

    $logger->info("creating new ST_ID " . $st_id->displayName . " for " . $instance->displayName);
    
    store($st_id,'store');
    add_stable_id_to_history($st_id,$instance);
    
    # Attach the stable ID to its parent instance
    $instance->stableIdentifier($st_id);
    store($instance,'update');

    return $st_id;
}


#########################################################################
## failure tolerant(?) wrapper for the GKInstance store and update methods
my $attempt = 1;
sub store {
    my $instance = shift;
    my $action   = shift;
    my @dbs = @_;

    if ($attempt > 2) {
	$logger->warn("Oops, I tried to $action $attempt times, there must be a good reason it failed, giving up");
	$attempt = 1;
	return undef;
    }

    my $force = $action eq 'store' ? 1 : 0;

    unless (@dbs > 0) {
	@dbs =($gk_central,$release_db,53);
    }
    for my $db (@dbs) {
        my $stored = eval {$dba{$db}->$action($instance,$force)};
	unless ($stored) {
	    $logger->warn("Oops, the $action operation (attempt $attempt) failed for $db:\n$@_\nI'll try again!");
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
    for my $db ($gk_central,$release_db,53) {
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
    my $db_id = shift;
    my $class = $instance->class;
    my @all_species = fetch_species($instance);
    my $species = $all_species[0];
    
    # skip if unambiguous
    if ($species && @all_species == 1) {
	$decision = "plain";
	return $species;
    }

    # chimeric things are 'NUL' species
    my $chimeric = $instance->attribute_value('isChimeric')->[0] || '';
    if ($chimeric eq 'TRUE') {
	$decision = "chimeric";
	return 'NUL';
    }
    elsif ($chimeric) {
	print join("\t", $chimeric, @all_species), "\n";
    }

    # Regulator?  Get last species if applicable
    if ($class =~ /regulation|requirement/i) {
	$decision = "regulator";
	my $last_species  = $all_species[-1];
	$species = $last_species || $species;
	unless ($species) {
	    $logger->info("Looking for species of pathways or regulators for this $class\n");
	    my @entities = @{$instance->attribute_value('regulatedEntity')};
	    push @entities, @{$instance->attribute_value('containedinPathway')};
	    push @entities, @{$instance->attribute_value('regulator')};
	    for my $entity (@entities) {
		$logger->info("Checking species for ".$entity->displayName);
		$species = fetch_species($entity);
		$logger->info("No species found") unless $species;
		last if $species;
	    }
	}
	return $species || 'ALL';
    }

    if ($class =~ /SimpleEntity|Polymer/) {
	$decision = "SimpleEntity";
	return 'ALL';
    }

    if (!$species && $class eq 'Complex') {
	$decision = "Complex";
	my $members = $instance->attribute_value('hasComponent');
	while (!$species && @$members > 0) {
            my $member = shift @$members;
            $species = fetch_species($member);
        }
	return $species || 'ALL';
    }

    if (!$species && $class =~ /Set/) {
	$decision = "Set";
	my $members = $instance->attribute_value('hasMember');
	while (!$species && @$members > 0) {
	    my $member = shift @$members;
            $species = fetch_species($member);
        }
	return $species || 'ALL'
    }
    
    $logger->info(join("\t","SPECIES",$class,$species,abbreviate($species))."\n");

    $decision = "fall-through";
    return $species || 'NUL';
}



__DATA__
2179307
353455
182053
187617
205060
3814821
418870
2484926
373015
1253318
2534162
2161617
443768
4396363
428715
419440
448959
212920
984713
3451144
2161147
1251929
391437
353298
1250468
2473583
4551554
573343
1168927
391512
421694
593695
2179313
4084922
1458902
2064883
2064853
1236959
189019
1169195
992753
420393
1227694
187608
2530504
420390
351455
5432456
421821
5667039
141698
2176431
433578
353313
5432387
448954
420389
187588
1250500
265862
391517
5610711
420394
4085088
433584
448947
421635
1227755
2484854
198739
548973
2426460
443818
2176393
2176428
159887
391533
2161165
443049
2534247
419465
2484957
353056
351468
419461
353434
2530565
2167872
5605301
517521
163440
4549261
392030
5362406
2169004
419474
2169036
163472
3299417
593680
2537524
2422406
2484859
912506
391408
1253330
353357
421147
2482182
984665
141687
5432586
2169012
1458871
5432520
4549228
75040
2186736
213297
2064916
194544
351442
433592
2167888
2132286
215126
389967
909781
212917
2533972
4084497
622372
622403
373177
437240
437938
353416
427288
443772
427279
391520
2161847
373889
379960
2731112
265529
997411
421138
421143
420398
2485115
3779389
508751
448961
391535
420397
433599
2545201
4084899
2176396
548972
372855
2473498
449201
194339
573389
351444
5432536
2731130
353444
2466133
163597
391434
2169016
188325
2533964
1227892
5368583
2065510
2484869
372741
983160
427308
159884
352832
352882
419452
2434198
4084908
209562
443942
2128982
112353
351337
445082
1251923
2076711
164926
427858
549062
2752124
5432551
3245934
428532
433593
2534170
373887
379836
3965440
215125
372852
448960
2161533
159620
163609
1237106
391521
112348
420386
3299567
2534209
187586
420396
391519
421879
3876071
622360
4568946
1483154
2731085
353239
1266699
2065560
2186755
2169043
2161526
391433
1250475
373920
444204
391979
3139045
5362792
5432421
2065278
266151
352875
421733
353323
5432444
421693
391513
2473590
419469
977240
443767
421139
443773
209770
373047
451756
1250472
420383
391522
2197556
391548
211119
2426496
1251988
419451
372742
1067645
443785
375176
420387
622387
141696
933534
2167890
419462
3772439
379814
1660598
353446
2176430
445066
2326808
749516
5368587
351433
421688
353303
1248744
2186775
3907289
2533890
421690
517516
879922
391444
434963
5610751
5607748
2752136
443770
1248753
2065178
2161160
877388
1181349
2176427
2465871
433606
1676139
2423781
573384
420388
421136
2422970
