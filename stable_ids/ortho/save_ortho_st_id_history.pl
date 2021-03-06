#!/usr/bin/perl -w
use common::sense;
use DBI;

use constant DB     => 'stable_identifiers';
use constant EXISTS_ST_ID => 'SELECT DB_ID,identifierVersion,instanceId FROM StableIdentifier WHERE identifier = ?';
use constant HISTORY => 'INSERT INTO History (ST_ID,name,class,ReactomeRelease,datetime) VALUES (?, ?, ?, ?,NOW())';
use constant EXISTS_NAME => 'SELECT DB_ID FROM Name WHERE name = ?'; 
use constant NAME    => 'INSERT INTO Name (ST_ID,name) VALUES (?, ?)';
use constant EXISTS_RELEASE => 'SELECT DB_ID FROM ReactomeRelease WHERE release_num = ?';
use constant RELEASE    => 'INSERT INTO ReactomeRelease (release_num,database_name) VALUES (?, ?)';
use constant CREATE => 'INSERT INTO StableIdentifier (identifier,identifierVersion,instanceId) VALUES (?, ?, ?)';
use constant VERSION => 'UPDATE StableIdentifier SET identifierVersion = ? WHERE DB_ID = ?';
use constant ASSOC => 'UPDATE StableIdentifier SET instanceId = ? WHERE DB_ID = ?';
use constant DEBUG => 0;

my $password = shift || die "Usage: $0 password\n";


my $dbh = DBI->connect(
    "dbi:mysql:".DB,
    'smckay',
    $password
    );

my $v = 1;

my $exists = $dbh->prepare(EXISTS_ST_ID);
my $change = $dbh->prepare(HISTORY);
my $create = $dbh->prepare(CREATE);
my $version = $dbh->prepare(VERSION);
my $assoc   = $dbh->prepare(ASSOC);
my $exists_name = $dbh->prepare(EXISTS_NAME);
my $save_name = $dbh->prepare(NAME);
my $exists_release = $dbh->prepare(EXISTS_RELEASE);
my $save_release = $dbh->prepare(RELEASE);

while (<>) {
    chomp;
    my ($release,$identifier,$db_id) = split;
    next unless $release == 53 || /UNAMBIG/;

    next unless $release && $identifier && $db_id;

    my $class = 'ORTHO';
    my $database = "test_reactome_$release";

    $exists->execute($identifier);

    my $stable_id_id;
    while (my $ary = $exists->fetchrow_arrayref) {
	my ($id,$identifier_version,$instance_id) = @$ary;
	
	say "$identifier exists in the database" if DEBUG;

	$stable_id_id = $id;

	if (!$instance_id) {
	    say "associating $identifier with $db_id" if DEBUG;
	    $assoc->execute($db_id,$id);
	}
    }
    unless ($stable_id_id) {
	say "I will create a record for $identifier" if DEBUG;
	$create->execute($identifier,$v,$db_id);
	$exists->execute($identifier);
	$stable_id_id = $exists->fetchrow_arrayref->[0];
    }

    
    my $name = "$identifier.1";
    $exists_name->execute($name);

    my $name_db_id = eval {$exists_name->fetchrow_arrayref->[0]};
    unless ($name_db_id) {
	say "Saving record of name $name" if DEBUG;
	$save_name->execute($stable_id_id,$name);
	$exists_name->execute($name);
	$name_db_id = eval {$exists_name->fetchrow_arrayref->[0]};
    }

    $exists_release->execute($release);

    my $release_db_id = eval {$exists_release->fetchrow_arrayref->[0]};
    unless ($release_db_id) {
        say "Saving record of release $release" if DEBUG;
        $save_release->execute($release,$database);
        $exists_release->execute($release);
        $release_db_id = eval {$exists_release->fetchrow_arrayref->[0]};
    }

    die "Incomplete" unless $stable_id_id && $name_db_id && $class && $release_db_id;
    $change->execute($stable_id_id,$name_db_id,lc($class),$release_db_id);
    say "saved state ($class) event for $name ($name_db_id), release $release";
}

