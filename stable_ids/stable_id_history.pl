#!/usr/bin/perl -w
use common::sense;
use Data::Dumper;
use DBI;

use lib '/usr/local/gkb/modules';
use GKB::DBAdaptor;

use constant USER => 'curator';
use constant PASS => 'r3@ct1v3';
use constant Q1   => 'select DB_ID from DatabaseObject where StableIdentifier IS NOT NULL';
use constant Q2   => 'select s.identifier,s.identifierVersion from DatabaseObject d, ' . 
                     'StableIdentifier s where d.DB_ID = ? AND s.DB_ID = d.StableIdentifier';

use constant REL  => map {"test_slice_${_}"} 26..32,34..36,38..52;

my %id;

my @releases = REL;
my %dbh = db_connect(@releases);

for (my $i = 0;$i < @releases;$i++) {
    my $release = $releases[$i];
    my $last_release = $releases[$i-1] if $i > 0;
    my @db_ids = get_db_ids($release);

    for my $db_id (@db_ids) { 
	my $dbh = $dbh{$release} || die "NO DBH for $release";
	my $db_num = $release;
	$db_num =~ s/\D+//g;
	my $sth = $dbh->prepare(Q2);
	$sth->execute($db_id);
	my ($st_id,$v);
	while (my $res = $sth->fetchrow_arrayref) {
	    ($st_id,$v) = @$res;
	    $id{$db_id}{$release}{identifier} = $st_id;
	    $id{$db_id}{$release}{version} = $v;
	}

	my $last_st_id = $id{$db_id}{$last_release}{identifier} if $last_release;
	my $last_v = $id{$db_id}{$last_release}{version} if $last_release;

	if ($last_release) {
	    if (!$last_st_id) {
		say join("\t",$release,$db_id,$st_id,$v,'CREATED');
	    }
	    elsif ($st_id eq $last_st_id && $last_v == $v) {
		say join("\t",$release,$db_id,$st_id,$v,'EXISTS');
	    }
	    elsif ($st_id ne $last_st_id) {
		say join("\t",$release,$db_id,$st_id,$v,'RENAMED');
		say join("\t",$release,$db_id,$last_st_id,$last_v,'DELETED');
	    } 
	    elsif ($v != $last_v) {
		say join("\t",$release,$db_id,$st_id,$v,'INCREMENTED');
	    }
	}
	else {
	    say join("\t",$release,$db_id,$st_id,$v,'EXISTS');
	}
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
    for my $db (@releases) {
	my $num = $db;
	$num =~ s/\D+//g;
	my $dsn = "dbi:mysql:$db";
	$dbh{$db} = DBI->connect($dsn, USER, PASS);
    }
    return %dbh;
}

