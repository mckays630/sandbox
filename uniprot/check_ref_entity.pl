#!/usr/bin/perl
use common::sense;
use Data::Dumper;

# Get rid if XML tags in geneName attribute

use lib '/usr/local/gkb/modules';
use GKB::DBAdaptor;

@ARGV >= 3 or die "$0 user pass db [class]";
my ($user, $pass, $db) = @ARGV;

my $dba = GKB::DBAdaptor->new(
    -dbname  => $db,
    -user    => $user,
    -pass    => $pass,
    -host    => $db eq 'gk_central' ? 'reactomecurator' : 'localhost'
    );

my $sth = $dba->prepare('SELECT DB_ID FROM DatabaseObject WHERE _class = ?');
$sth->execute($ARGV[3] || 'ReferenceGeneProduct');
my @db_ids;
while (my $ary = $sth->fetchrow_arrayref) {
    push @db_ids, $ary->[0];
}

for my $db_id (@db_ids) {
    my $instance = $dba->fetch_instance_by_db_id($db_id)->[0];
    my $gene_names = $instance->geneName;
    my @xml = grep {/<|\?|\>/} @$gene_names;
    if (@xml > 0) {
	$instance->inflate();
	say $instance->displayName, " has rubbish: ", join("\t",@xml);
	say "BEFORE:";
	say Dumper $gene_names;
	my @good_names = grep {!/<|\?|\>/} @$gene_names; 
	$instance->attribute_value('geneName',undef);
	$instance->attribute_value('geneName',@good_names);
	say "AFTER:";
	say Dumper $instance->geneName;
	say "storing change";
	$dba->update($instance);
    }
}


