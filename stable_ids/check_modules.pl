#!/usr/bin/perl -w
use common::sense;
use Data::Dumper;
use DBI;

use lib '/usr/local/gkb/modules';
use GKB::DBAdaptor;

use constant USER => 'curator';
use constant PASS => 'r3@ct1v3';
use constant Q1   => 'select DB_ID from DatabaseObject where StableIdentifier IS NOT NULL';
use constant Q2   => 'select d._displayName,s.DB_ID,s.identifier,s.identifierVersion from DatabaseObject d, ' . 
                     'StableIdentifier s where d.DB_ID = ? AND s.DB_ID = d.StableIdentifier';
use constant Q3   => 'update StableIdentifier set identifierVersion = ? where DB_ID = ?';
use constant Q4   => 'update StableIdentifier set identifier = ? where DB_ID = ?';
use constant REL  => (map {"test_slice_${_}"} 
qw/
26
27
28
29
30
31
32
34
35
36
38
39
40
41
42
43
44
45
46
47
48
49
50
51
52
/);

my %id;
my %problem;
my @ids;

my @releases = REL;
my %dbh = db_connect(@releases);
my @db_ids = get_db_ids();

my $dba = GKB::DBAdaptor->new(
    -dbname => $releases[-1],
    -user   => USER,
    -pass   => PASS
    );

for my $db_id (@db_ids) { 
    push @ids, $db_id;
    for my $db (@releases) {
	my $dbh = $dbh{$db} || die "NO DBH for $db";
	my $db_num = $db;
	$db_num =~ s/\D+//g;
	my $sth = $dbh->prepare(Q2);
	$sth->execute($db_id);
	while (my $res = $sth->fetchrow_arrayref) {
	    my ($name,$st_db_id,$st_id,$v) = @$res;
	    $id{$db_id}{st_id} ||= $st_id;
	    $id{$db_id}{highest_version} ||= $v;
	    
	    if ($id{$db_id}{st_id} ne $st_id) {
		#say STDERR "Mismatch!\n";
		$problem{$db_id}{st_id}++;
	    }
	    
	    $id{$db_id}{previous_version} ||= 0;
	    if ($id{$db_id}{previous_version} > $v) {
		$problem{$db_id}{version}++;
		$id{$db_id}{$db_num}{corrected_version} = $id{$db_id}{previous_version};
	    }
	    elsif ($id{$db_id}{$db_num - 1}{corrected_version}) {
		if ($id{$db_id}{previous_version} == $v) {
		    $id{$db_id}{$db_num}{corrected_version} = $id{$db_id}{$db_num - 1}{corrected_version};
		}
		else {
		    $id{$db_id}{$db_num}{corrected_version} = $id{$db_id}{$db_num - 1}{corrected_version} + 1;
		}
	    }

	#    if ($id{$db_id}{$db_num}{corrected_version}) {
	#	say "$st_id $v corrected to $id{$db_id}{$db_num}{corrected_version}";
	#    }

	    $id{$db_id}{highest_version} = $v if $v > $id{$db_id}{highest_version};

	    $id{$db_id}{$db_num}{st_id}    = $st_id;
	    $id{$db_id}{$db_num}{version}  = $v;
	    $id{$db_id}{$db_num}{name}     = $name;
	    $id{$db_id}{$db_num}{st_db_id} = $st_db_id;
	    $id{$db_id}{previous_version}  = $v;
	}
    }
}

my $total = keys %id;
say "There were a total of $total events";exit; 


say join(",", qw/st_id/, grep {s/\D+//} @releases);
for my $db_id (sort {$a<=>$b} keys %problem) {
    my @versions;
    my @ids;
    my @corrected_versions;
    my @names;
    my $last_id  = '';
    my $st_id;
    my $proper_st_id;
    for my $db (@releases) {
	my $db_name = $db;
	#say $db_name;
	$db =~ s/\D+//g;
	my $version = $id{$db_id}{$db}{version} || '-';
	my $corrected_version = $id{$db_id}{$db}{corrected_version};
	my $name = $id{$db_id}{$db}{name};
	push @names, $name || '-';

        $proper_st_id ||= $id{$db_id}{st_id};

	my $st_db_id = $id{$db_id}{$db}{st_db_id};

	if ($st_db_id && $corrected_version && $version ne '-' && $version != $corrected_version) {
	    #say "$db, fixing version $version->$corrected_version";
	    #my $dbh = $dbh{"test_reactome_${db}_repaired"} or die "no dbh";
	    #my $sth = $dbh->prepare(Q3);
	    #$sth->execute($corrected_version,$st_db_id);
	    
	    #$dbh = $dbh{"test_slice_${db}_repaired"} or die "no dbh";
            #$sth = $dbh->prepare(Q3);
            #$sth->execute($corrected_version,$st_db_id);
	    push @versions, 'X';
	}
	else {
	    push @versions, $version eq '-' ? '-' : '.';
	}

	$st_id = $id{$db_id}{$db}{st_id} || '-';
	if ($st_db_id && $st_id ne '-' && $st_id ne $proper_st_id) {
	    #say "$db, fixing ST_ID $st_id -> $proper_st_id $st_db_id";
	    #my $dbh = $dbh{"test_reactome_${db}_repaired"} or die "no dbh";
	    #my $sth = $dbh->prepare(Q4);
	    #$sth->execute($proper_st_id,$st_db_id);
	    
	    #$dbh = $dbh{"test_slice_${db}_repaired"} or die "no dbh";
            #$sth = $dbh->prepare(Q4);
            #$sth->execute($proper_st_id,$st_db_id);
	    push @ids, 'X';
	}
	else {
	    push @ids, $st_id eq '-' ? '-' : '.';
	}

	$st_id = '' if $st_id eq $last_id;
	$last_id = $id{$db_id}{$db}{st_id} || '-';

	if ($st_id) {
	    $version = "$st_id.$version";
	    $corrected_version = "$st_id.$corrected_version" if $corrected_version;
	}

	push @corrected_versions, $corrected_version || '-';
    }
    my $problem = '';

    (my $last_release = @releases[-1]) =~ s/\D+//g;
    if ($problem{$db_id}{version}) {
	$problem = 'V';
    }
    if ($problem{$db_id}{st_id}) {
	$problem .= 'M';
    }

    my $species = get_species($db_id);
    #say join("\t",$proper_st_id,$db_id,$species);
    say join("\t",$species,$proper_st_id,'VER',@versions) if $problem =~ /V/;
    say join("\t",$species,$proper_st_id,'IDS',@ids) if $problem =~ /M/;;
#    say join(",",$db_id,@corrected_versions,$problem) if $problem =~ /V/;

    next;
    if ($problem =~ /M/) {
	my %names = map {$_ => 1} @names;
	delete $names{'-'};
	my $num = keys %names;
	print join("\n",@names),"\n" if $num > 1;
    }
}


sub get_db_ids {
    my $last_db = $releases[-1];
    my $dbh = $dbh{$last_db} or die "NO DBH";
    my $sth = $dbh->prepare(Q1);
    $sth->execute;
    my @db_ids;
    while (my @id = $sth->fetchrow_array) {
	push @db_ids, $id[0];
    }
    return @db_ids;
}

sub db_connect {
    my %dbh;
    for my $db (@releases) {
	my $num = $db;
	$num =~ s/\D+//g;
	my $dsn = "dbi:mysql:$db";
	$dbh{$db} = DBI->connect($dsn, USER, PASS);
	$dbh{$num} =  $dbh{$db};

	next;
	$dsn .= "_repaired" unless $dsn =~ /_repaired/;
	$dbh{"test_slice_${num}_repaired"} =  DBI->connect($dsn, USER, PASS);

	$dsn =~ s/slice/reactome/;
	$dbh{"test_reactome_${num}_repaired"} =  DBI->connect($dsn, USER, PASS);

    }
    return %dbh;
}


sub get_species {
    my $db_id = shift;
    my $instance = $dba->fetch_instance_by_db_id($db_id)->[0] || return 'UNKNOWN';
    my $species = $instance->attribute_value('species')->[0]  || return 'UNKNOWN';
    return $species->name->[0];
}
