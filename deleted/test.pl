#!/usr/bin/perl
use common::sense;
use Data::Dumper;

use lib '/usr/local/gkb/modules';
use GKB::DBAdaptor;

@ARGV >= 3 or die "$0 user pass search_term";
my ($user, $pass, $term) = @ARGV;

my $dba = GKB::DBAdaptor->new(
    -dbname  => 'test_reactome_54',
    -user    => $user,
    -pass    => $pass
    );

my $res = $dba->fetch_instance( -CLASS => '_Deleted',
				-QUERY => [['deletedInstanceDB_ID',[$term]]]
    );
my $num = @$res;
say "I got $num";

