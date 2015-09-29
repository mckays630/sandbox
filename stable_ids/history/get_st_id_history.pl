#!/usr/bin/perl -w
use common::sense;
use Data::Dumper;
use DBI;

use lib '/usr/local/gkb/modules';
use GKB::DBAdaptor;

use constant USER => 'smckay';
use constant Q1   => 'select DB_ID from DatabaseObject where StableIdentifier IS NOT NULL';
use constant Q2   => 'select s.identifier,s.identifierVersion from DatabaseObject d, ' . 
                     'StableIdentifier s where d.DB_ID = ? AND s.DB_ID = d.StableIdentifier';

use constant REL  => 26..53;

my $password = shift || die "Usage: $0 password\n";

my %db_id;
my %id;
my %db;

my @releases = REL;
my %dbh = db_connect(@releases);

my %exists;

for (my $i = 0;$i < @releases;$i++) {
    my $release = $releases[$i];
    my $last_release = $releases[$i-1] if $i > 0;
    my @db_ids = get_db_ids($release);
    my $dbh = $dbh{$release} || die "NO DBH for $release";
    my $db_num = $release;
    $db_num =~ s/\D+//g;
    my $sth = $dbh->prepare(Q2);
    my $slice = $db{$release} =~ /slice/;

    for my $db_id (@db_ids) { 

	# if this is not a slice, ignore db_ids that have not been seen,
	# to avoid ortho-inferred events 
	next if !$slice && !$db_id{$db_id};

	$sth->execute($db_id);
	my ($st_id,$v);
	while (my $res = $sth->fetchrow_arrayref) {
	    ($st_id,$v) = @$res;
	    $id{$db_id}{$release}{identifier} = $st_id;
	    $id{$db_id}{$release}{version} = $v;
	    $exists{$st_id}++;
	}

	my $last_st_id = $id{$db_id}{$last_release}{identifier} if $last_release;
	my $last_v = $id{$db_id}{$last_release}{version} if $last_release;

	

	if ($last_release) {
	    if (!$last_st_id && !$exists{$st_id}) {
		say join("\t",$release,$db_id,$st_id,$v,'class=CREATED',$db{$release});
	    }
	    elsif ($st_id eq $last_st_id && $last_v == $v) {
		say join("\t",$release,$db_id,$st_id,$v,'class=EXISTS',$db{$release});
	    }
	    elsif ($last_st_id && $st_id ne $last_st_id) {
		say join("\t",$release,$db_id,$st_id,$v,'class=RENAMED',$db{$release});
	    } 
	    elsif ($v != $last_v) {
		say join("\t",$release,$db_id,$st_id,$v,'class=INCREMENTED',$db{$release});
	    }
	}
	else {
	    say join("\t",$release,$db_id,$st_id,$v,'class=EXISTS',$db{$release});
	}

	$db_id{$db_id}++;
    }
}

sub get_db_ids {
    my $db = shift;
    my $dbh = $dbh{$db} or die "NO DBH for $db";
    my $sth = $dbh->prepare(Q1);
    $sth->execute;
    my @db_ids;
    while (my @id = $sth->fetchrow_array) {
	push @db_ids, $id[0];
    }
    return @db_ids;
}

sub db_connect {
    my %dbh;
    for my $r (@releases) {
	
	DB: for my $db ("test_slice_$r", "test_slice_$r\_myisam", "test_reactome_$r") {
	    my $dsn = "dbi:mysql:$db";
	    $dbh{$r} = eval { DBI->connect($dsn, USER, $password, {'PrintError'=>0}) };
	    if ($dbh{$r}) {
		$db{$r} = $db;
		last DB;
	    }
	}

	$dbh{$r};
    }

    return %dbh;
}

