#!/usr/bin/perl
use common::sense;
use Data::Dumper;

use lib '/usr/local/gkb/modules';
use GKB::DBAdaptor;

@ARGV >= 2 or die "$0 user pass";
my ($user, $pass, $db) = @ARGV;

my $dba = GKB::DBAdaptor->new(
    -dbname  => $db,
    -user    => $user,
    -pass    => $pass,
    -host    => $db eq 'gk_central' ? 'reactomecurator' : 'localhost'
    );

my $gkc = GKB::DBAdaptor->new(
    -dbname  => "gk_central",
    -user    => $user,
    -pass    => $pass,
    -host    => 'reactomecurator'
    );

# get all deleted instances
my $sth = $gkc->prepare('SELECT DB_ID FROM DatabaseObject WHERE _class = "_Deleted"');
$sth->execute;

my %stored;
while (my $ary = $sth->fetchrow_arrayref) {
    my $instance = $gkc->fetch_instance_by_db_id($ary->[0])->[0];
    $instance->inflate;
    say $instance->db_id;

    my @reasons = @{$instance->reason};
    for my $reason (@reasons) {
	$reason->inflate;
	next if $stored{$reason->db_id}++;
	eval{$dba->store($reason,1)};
    }

    eval{$dba->store($instance,1)};
    say "Stored ", $instance->db_id;
}

