#!/usr/bin/perl
use common::sense;
use DBI;
use lib '/usr/local/gkb/modules';
use Data::Dumper;

my $password = shift || die "Usage: $0 password\n";

my $dbh = DBI->connect(
    "dbi:mysql:stable_identifiers",
    'smckay',
    $password
    );

my %db_id;
my $sth = $dbh->prepare('SELECT identifier,instanceId FROM StableIdentifier');
$sth->execute;
while (my $id = $sth->fetchrow_arrayref) {
    my ($st_id,$db_id) = @$id;
    $db_id{$db_id}{$st_id}++;
}


say "# Reactome stable IDs for release 53";
say join("\t",qw/Stable_ID old_identifier(s)/);
my @hsa_rows;
my @other_rows;
for my $db_id (sort {$a<=>$b} keys %db_id) {
    my @st_ids = sort keys %{$db_id{$db_id}};
    next if @st_ids < 2;
    my $array_to_push = $st_ids[0] =~ /R-HSA/ ? \@hsa_rows : \@other_rows;
    my $primary = shift @st_ids;
    $primary =~ /^R-/ or next;
    my $old = join(',',@st_ids);
    push @$array_to_push, join("\t",$primary,$old);
}

for my $row (sort @hsa_rows) {
    say $row;
}
for my $row (sort @other_rows) {
    say $row;
}

exit;


