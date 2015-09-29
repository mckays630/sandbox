#!/usr/bin/perl
use common::sense;
use Data::Dumper;
use JSON;

use lib '/usr/local/gkb/modules';
use GKB::DBAdaptor;

@ARGV >= 2 or die "$0 user pass";
my ($user, $pass) = @ARGV;

my %classes = map {$_=>1} qw/
Pathway SimpleEntity OtherEntity DefinedSet Complex Depolymerisation EntitySet Polymerisation    
Reaction BlackBoxEvent PositiveRegulation CandidateSet NegativeRegulation OpenSet Requirement Polymer    
FailedReaction EntityWithAccessionedSequence GenomeEncodedEntity                                                                              
    /;

my @releases = 35..54;
my %dba;
my %count;
my %seen;
for my $rel (@releases) {
    next if $rel == 37;
    my $dba= GKB::DBAdaptor->new(
	-dbname  => "test_slice_$rel",
	-user    => $user,
	-pass    => $pass);

    my $sth = $dba->prepare('select DB_ID,_class from DatabaseObject');
    $sth->execute;
    
    while (my $result = $sth->fetchrow_arrayref) {
	my ($db_id,$class) = @$result;
	next unless $classes{$class};
	$seen{$db_id}{$rel}++;
    }
}

for my $db_id (sort keys %seen) {
    my $data = $seen{$db_id};
    unless ($data->{54}) {
	say join("\t",$db_id,sort keys %$data);
    }
}

