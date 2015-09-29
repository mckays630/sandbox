#!/usr/bin/perl
use common::sense;
use Data::Dumper;
use JSON;

use lib '/usr/local/gkb/modules';
use GKB::DBAdaptor;

@ARGV >= 2 or die "$0 user pass";
my ($user, $pass, $db) = @ARGV;

my @releases = 49..54;
my %dba;
my %count;
for my $rel (@releases) {

    my $dba= GKB::DBAdaptor->new(
	-dbname  => "test_reactome_$rel",
	-user    => $user,
	-pass    => $pass,
	-host    => $db eq 'gk_central' ? 'reactomecurator' : 'localhost'
	);

    my $sth = $dba->prepare('select _class,count(*) count from DatabaseObject group by _class');
    $sth->execute;
    
    while (my $result = $sth->fetchrow_arrayref) {
	my ($class, $count) = @$result;
	$count{$rel}{$class} = $count;
    }
}

# Get ordered list of classes with count > 5000
my ($ultimate,$penultimate) = @releases[-1,-2];
my $current_counts = $count{$ultimate};
my @classes = keys %$current_counts;

@classes = sort {$count{$ultimate}{$b} <=> $count{$ultimate}{$a}} @classes;

my $classes = {
    release => \@releases,
    classes => \@classes,
    counts  => {}
};

for my $class (@classes) {
    my @cnt;
    for my $rel (@releases) {
	push @cnt, $count{$rel}{$class} || 0;
    }
    $classes->{counts}->{$class} = \@cnt;
}

say encode_json($classes);


