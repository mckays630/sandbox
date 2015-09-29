#!/usr/bin/perl
use common::sense;
use Data::Dumper;

use lib '/usr/local/gkb/modules';
use GKB::DBAdaptor;

@ARGV >= 3 or die "$0 user pass db";
my ($user, $pass, $db) = @ARGV;

my $dba = GKB::DBAdaptor->new(
    -dbname  => $db,
    -user    => $user,
    -pass    => $pass
    );

#select * from DatabaseObject where _class = 'DatabaseIdentifier' AND _DisplayName LIKE '%KEGG%'

#if (0) {
#my $instance = $dba->fetch_instance_by_db_id($db_id)->[0];
#my $xref = $instance->crossReference;

#for my $x (@$xref) {
#    say $x->db_id, "\t",$x->displayName;
#    my @rdb = $x->referenceDatabase->[0];
#    if (@rdb == 0) {#
#	say "NO RDB!!#!";
#    }
#    for my $rdb (@rdb) {
#	my @names = @{$rdb->name};
#	say "CLASS ", $rdb->class;
#	@names > 0 or die "NO NAME";
#	say join("\t",@names);
#    }
#}
#}
#exit;
#my $sth = $dba->prepare('SELECT DB_ID FROM DatabaseObject WHERE _class = "ReferenceDNASequence" AND _displayName LIKE "%KEGG%"');
my $sth = $dba->prepare('SELECT DB_ID FROM DatabaseObject WHERE _displayName LIKE "PRO:%"');
#my $sth = $dba->prepare('SELECT DB_ID FROM DatabaseObject WHERE _class = "ReferenceDNASequence" AND _displayName LIKE "%Orphanet%"');
$sth->execute;
my @instances;
while (my $ary = $sth->fetchrow_arrayref) {
    my $r = $dba->fetch_instance_by_db_id($ary->[0])->[0];
    my $name = $r->displayName;
    my $id = $r->identifier->[0];
    my $rdb = $r->referenceDatabase->[0];
    my $class = $r->class;
    say join("\t",$ary->[0],$name,$class,$id);
#    $r->inflate;
#    $dba2->store($r, 1);
#    unless ($rdb) {
#	say $r->db_id , " ", $name, " IS BAD" unless $rdb;
	$dba->delete($r);
#    }
}
