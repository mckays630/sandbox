#!/usr/bin/perl
use common::sense;
use Data::Dumper;

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

 
my $sth = $dba->prepare('SELECT DB_ID FROM DatabaseObject WHERE _class = "pathway"');
$sth->execute;
my @db_ids;
while (my $ary = $sth->fetchrow_arrayref) {
    push @db_ids, $ary->[0];
}

say join("\t",qw/pathway_DB_ID stable_id old_stable_id pathway_name/);
for my $db_id (@db_ids) {
    my $instance = $dba->fetch_instance_by_db_id($db_id)->[0];
    my $st_id = $instance->stableIdentifier->[0]->identifier->[0];
    my $old_st_id =  $instance->stableIdentifier->[0]->oldIdentifier->[0] || '';
    next unless $st_id =~ /HSA/;
    say join("\t",$db_id,$st_id,$old_st_id,$instance->displayName);
}


