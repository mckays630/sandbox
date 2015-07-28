#!/usr/bin/perl -w
use common::sense;
use lib "/usr/local/gkb/modules";
use GKB::DBAdaptor;
use DBI;


my $password = shift or die "Usage $0 password";

my $dba = GKB::DBAdaptor->new(
          -dbname  => 'gk_current',
          -user    => 'smckay',
          -pass    => '$password'
    );

my $dbh = DBI->connect(
    "dbi:mysql:stable_identifiers",
    'smckay',
    '$password'
    );


open DICT, "reactome_stable_ids.txt" or die $!;
my %id;
while (<DICT>) {
    chomp;
    next if /Stable/;
    my ($new,$old) = split;
    my @old = split(',',$old);
    $id{$new} = \@old;
}


my $sth = $dbh->prepare('SELECT name from Name where name LIKE ? ORDER BY name');

while (<*.owl>) {
    next if /old_ids/;
    next unless /_sapiens/;
    say $_;
    open OWL, $_ or die $!;
   
    chomp(my $base = `basename $_ .owl`);
    open OUT, ">$base\_old_ids.owl" or die $!;


    while (<OWL>) {
	unless (/(R-[A-Z]{3}-\d+(-\d+)?(\.\d+)?)/) {
	    print OUT;
	    next;
	}
	else {
	    my $st_id = $1;
	    (my $base_id = $st_id) =~ s/\.\d+$//;
	    (my $db_id = $base_id) =~ s/R-[A-Z]+-//;
	    my $instance = $dba->fetch_instance_by_db_id($db_id)->[0];
	    my $sobj = $instance->attribute_value('stableIdentifier')->[0];
	    my $old_id = $sobj->attribute_value('oldIdentifier')->[0];
	    my $old_v  = $sobj->attribute_value('oldIdentifierVersion')->[0];
	    my $map = $id{$base_id};
	    my ($mapped) = reverse sort @$map if $map;
	    #say "OLD $old_id.$old_v" if $old_id;


	    unless ($st_id =~ /R-HSA|R-ALL|R-NUL/) {
		my $class = $instance->class;
		say join("\t",'HUH',$st_id,$class);
	    }

	    if ($old_id) {
		$old_id .= ".$old_v";
		s/$st_id/$old_id/;
		say "OLD ID\t$old_id";
		print OUT $_;
		next;
	    }
	    elsif ($mapped && $base_id eq $st_id) {
		s/$base_id/$mapped/;
		say "OLD BASE ID\t$base_id";
		print OUT $_;
		next;
	    }
	    elsif ($mapped) {
		$sth->execute($base_id);
		my $name;
		while ($sth->fetchrow_arrayref) {
		    $name = $_->[0];
		    say "NAME!!! $name";
		}
		if ($name) {
		    say "OLD NAME $name";
		    s/$st_id/$name/;
		}
		print OUT $_;
		next;
	    }
	    else {
		print OUT $_;
		say "NO MAP $st_id";
		next;
	    }
	}
    }

}
