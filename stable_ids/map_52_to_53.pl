#!/usr/bin/perl -w
use lib "/usr/local/gkb/modules";
use GKB::StableIdentifierDatabase;
use GKB::DBAdaptor;

my $stable = GKB::StableIdentifierDatabase->new();
my ($user, $pass) = qw/curator r3@ct1v3/;
my $old = GKB::DBAdaptor->new(
            -dbname  => 'test_slice_52',
            -user    => $user,
            -pass    => $pass
    );
my $new = GKB::DBAdaptor->new(
            -dbname  => 'test_slice_53',
            -user    => $user,
            -pass    => $pass
    );

open IN, "uniprot_53.txt";
my %map;
while (<IN>) {
    chomp;
    my ($v,$k) = split;
    $map{$k} = $v;
}

my $sth = $old->prepare('SELECT DISTINCT identifier FROM StableIdentifier');
$sth->execute;
while (my $ar = $sth->fetchrow_arrayref) {
    my $st_id = $ar->[0];
#    print "$st_id\n";
    my $db_ids = $stable->db_ids_from_stable_id($st_id);
    my $db_id = $db_ids->[0];
    if (@$db_ids > 1) {
	print STDERR "Uh-Oh, multiple $st_id\n";
	next;
    }
#    print "DB_ID $db_id\n";
    my $stable_id;
    my $uniprot;
    if ($db_id) {
	my $instance = $new->fetch_instance_by_db_id($db_id)->[0];
	if ($instance) {
	    next if $instance->class ne 'EntityWithAccessionedSequence';
	    $stable_id = eval{$instance->attribute_value(stableIdentifier)->[0]->identifier->[0]};
	}
    }

    if ($stable_id) {
	$uniprot = $map{$stable_id} || 'NONE';
	print "$st_id\t$stable_id\t$uniprot\n";
	
    }
    else {
	print STDERR "$st_id\tunmapped\n";
    }
}
    
