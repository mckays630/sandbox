package GKB::StableIdentifierDatabase;

# Basic wrapper for stable id database
use common::sense;

use vars qw/@ISA/;
use Exporter();

use lib '/usr/local/gkb/modules';
use GKB::Secrets;
use GKB::DBAdaptor;
use Data::Dumper;

use constant DB  => 'stable_identifiers';
use constant Q1  => 'SELECT instanceId FROM StableIdentifier WHERE identifier = ?';
use constant Q2  => 'SELECT DB_ID,identifier,identifierVersion FROM StableIdentifier WHERE instanceId = ?';
use constant Q3  => '
SELECT h.class, r.release_num, n.name, r.database_name
FROM History h, Name n, ReactomeRelease r 
WHERE h.ST_ID = ?
AND h.name = n.DB_ID
AND r.DB_ID = h.ReactomeRelease
ORDER BY h.ReactomeRelease';
use constant ST_ID  => 'SELECT DB_ID FROM StableIdentifier WHERE identifier = ?';
use constant GET_NAME => 'SELECT DB_ID FROM Name WHERE name = ?';
use constant SET_NAME => 'INSERT INTO Name (ST_ID,name) VALUES (?,?)';
use constant GET_RELEASE => 'SELECT DB_ID FROM ReactomeRelease WHERE database_name = ?';
use constant SET_RELEASE => 'INSERT INTO ReactomeRelease (release_num,database_name) VALUES (?,?)';
use constant CREATE => 'INSERT INTO StableIdentifier VALUES (NULL, ?, ?, ?)';
use constant LOG_CHANGE  => 'INSERT INTO History VALUES (NULL,?,?,?,?,NOW())';
use constant UPDATE => 'UPDATE StableIdentifier SET instanceId = ? WHERE DB_ID = ?';
use constant ORTHO_V => 'UPDATE StableIdentifier SET identifierVersion = 1';
use constant HISTORY => 'SELECT COUNT(*) FROM History WHERE ST_ID = ? AND class = ?';

sub new {
    my $class = shift;
    my $self = {};
    if (@_) {
	my %hash = @_;
	for (keys %hash) {
	    $self->{$_} = $hash{$_};
	}
    }
    return bless $self, $class;
}

sub db_id_from_stable_id {
    my $self = shift;
    my $db_ids = $self->db_ids_from_stable_id(@_);
    if ($db_ids && @$db_ids > 0) {
	return $db_ids->[0];
    }
    return undef;
}

sub db_ids_from_stable_id {
    my $self = shift;
    my $stable_id = shift;

    $stable_id = uc($stable_id);
    $stable_id =~ /^REACT|^R-/ or die "$stable_id does not look like a stable ID to me";
    $stable_id =~ s/\.\d+$//;

    my $query = $self->dbh->prepare(Q1);
    $query->execute($stable_id);

    my $ids = [];
    while (my $id = $query->fetchrow_arrayref) {
        push @$ids, $id->[0];
    }
    return $ids;
}

sub stable_id_from_db_id {
    my $self = shift;
    my $db_id = shift || die("No DB_ID");

    my $query = $self->dbh->prepare(Q2);
    $query->execute($db_id);
    
    my @st_id;
    while (my $st_id = $query->fetchrow_arrayref) {
	push @st_id, [@$st_id]; #deref/ref weird recycle of reference otherwise
    }

    return @st_id;
}

sub get_history {
    my $self = shift;
    my $db_id = shift;
    my @stable_ids = $self->stable_id_from_db_id($db_id);

    my $query = $self->dbh->prepare(Q3);

    my @events;
    for my $st_id (@stable_ids) {
	my ($st_db_id,$identifier,$version) = @$st_id;
	$query->execute($st_db_id);

	while (my $event = $query->fetchrow_arrayref) {
	    my ($class,$release,$actual_name,$database) = @$event;
	    push @events, [$actual_name,$class,$release,$database];
	}
    }

    return @events;
}

sub dbh {
    my $self = shift;
    my $db = $self->{db} || DB;
    $self->{dbh} ||= DBI->connect(
	"dbi:mysql:$db",
	$GKB::Secrets::GK_DB_USER,
	$GKB::Secrets::GK_DB_PASS
	);
    return $self->{dbh};
}

sub get_dba {
    my $self = shift;
    my $db   = shift ||  $GKB::Secrets::GK_DB_NAME;
    $self->{dba}->{$db} ||= GKB::DBAdaptor->new(
	-dbname  => $db,
	-user    => $GKB::Secrets::GK_DB_USER,
	-pass    => $GKB::Secrets::GK_DB_PASS
	);
    return $self->{dba}->{$db};
}

sub has_change {
    my $self = shift;
    my $instance = shift;

    my $st_db_id = $self->has_history($instance);
    
    my $sth = $self->dbh->prepare(HISTORY);
    $sth->execute($st_db_id,'ortho');
    my $ary = $sth->fetchrow_arrayref;
    my $num = 1 if $ary && @$ary;
    return $num;
}

sub has_history {
    my $self = shift;
    my $instance = shift;
    my $do_not_add = shift;
    unless ($instance) {
	Bio::Root::RootI->throw("has_history: no stable id instance");
    }
    my $identifier = $instance->attribute_value('identifier')->[0];

    if ($self->{st_id}->{$identifier}) {
	return  $self->{st_id}->{$identifier};
    }

    my $dbh = $self->dbh();
    my $sth = $dbh->prepare(ST_ID);
    $sth->execute($identifier);
    my $ary = $sth->fetchrow_arrayref || [];
    my $db_id = $ary->[0];

    if ($db_id) {
	$self->{st_id}->{$identifier} = $db_id;
	return $db_id;
    }
    elsif (!$do_not_add) {
	return $self->add_stable_id_to_history($instance);
    }
    return undef;
}

sub update_ortho_parent {
    my $self = shift;
    my $st_id  = shift;
    my $parent = shift;
    my $st_db_id = $self->has_history($st_id);
    my $instance_id = $parent->db_id;
    my $sth = $self->dbh->prepare(UPDATE);

    say "Updating ortho parent for $instance_id, ".$parent->displayName; 
    $sth->execute($instance_id,$st_db_id);
    #$sth = $self->dbh->prepare(ORTHO_V);
    #$sth->execute;
}

sub add_stable_id_to_history {
    my $self = shift;
    my $instance  = shift or die "add_stable_id_to_history: I was expecting a stable id instance";
    my $parent = shift;

    my $ortho = $self->is_ortho();

    my $parent_id;
    if ($parent) {
	$parent_id = eval{$parent->db_id};
	unless ($parent_id) {
	    $parent_id = $parent;
	}
    }

    my $history_id = $self->has_history($instance,1);
    if ($history_id) {
	say "THIS ID EXISTS ".$instance->displayName;
	return $history_id;
    }

    say "THIS ID IS NEW ".$instance->displayName;
    my $dbh = $self->dbh();
    my $identifier = $instance->attribute_value('identifier')->[0];
    my $version = $instance->attribute_value('identifierVersion')->[0];

    # It is possible this stable ID has been used before.  If so,
    # we will pick up the last known version
    my $sth = $dbh->prepare(ST_ID);
    $sth->execute($identifier);
    my ($st_db_id) = eval{@{$sth->fetchrow_arrayref}};

    # This means we will revive the old stable ID
    if ($st_db_id) {
	if ($parent_id) {
	    $self->update_parent($st_db_id,$parent_id);
	}
	$self->log_reactivation($instance) unless $ortho;
	return $st_db_id;
    }
    else {
	$sth = $dbh->prepare(CREATE);
	$sth->execute($identifier,$version,$parent_id);

	$sth = $dbh->prepare(ST_ID);
	$sth->execute($identifier);

	my $db_id = eval{$sth->fetchrow_arrayref->[0]};

	if ($db_id) {
	    $self->{st_id}->{$identifier} = $db_id;
	    $self->log_creation($instance) unless $ortho;
	}
	else {
	    return undef;
	}
    }
}

sub log_renaming {
    my $self = shift;
    $self->add_change_to_history(@_,'renamed');
}

sub log_deletion {
    my $self = shift;
    $self->add_change_to_history(@_,'deleted');
}

sub log_creation {
    my $self = shift;
    $self->add_change_to_history(@_,'created');
}

sub log_incrementation {
    my $self = shift;
    $self->add_change_to_history(@_,'incremented');
}

sub log_reactivation {
    my $self = shift;
    $self->add_change_to_history(@_,'reactivated');
}

sub log_exists {
    my $self = shift;
    $self->add_change_to_history(@_,'exists');
}

sub log_ortho {
    my $self = shift;
    $self->add_change_to_history(@_,'ortho');
}

sub add_change_to_history {
    my $self = shift;
    my ($st_id,$change) = @_;
    $st_id or die "add_change_to_history: I was expecting a stable id instance";
    my $st_db_id = $self->has_history($st_id) || $self->add_stable_id_to_history($st_id);

    unless ($st_db_id) {
	Bio::Root::RootI->throw("No history found for ".$st_id->displayName);
    }

    my $dbh = $self->dbh();

    my $release = $self->reactome_release();
    my $name    = $self->stable_id_name($st_id);

    my $sth = $dbh->prepare(LOG_CHANGE);
    $sth->execute($st_db_id,$name,$change,$release);
    $self->{logged}->{$st_id}++;
}

sub release_num {
    my $self = shift;
    my $num = shift;
    $self->{release_num} = $num if $num;
    return $self->{release_num};
}

sub database_name {
    my $self = shift;
    my $name = shift;
    $self->{database_name} = $name if $name;
    return $self->{database_name};
}

sub is_ortho {
    my $self = shift;
    my $name = shift;
    $self->{is_ortho} = $name if $name;
    return $self->{is_ortho};
}

sub reactome_release {
    my $self = shift;
    my $db   = shift;
    my $num  = shift;

    if ($self->{release}) {
	return $self->{release};
    }

    say "DB = ".$self->{database_name};
    $db ||= $self->database_name();
    $db || die "No database name set";
    $num ||= $self->release_num(); 
    $num || die "No release number set";

    my $sth1 = $self->dbh->prepare(GET_RELEASE);
    my $sth2 = $self->dbh->prepare(SET_RELEASE);

    $sth1->execute($db);
    while (my $ref = $sth1->fetchrow_arrayref()) {
	$self->{release} = $ref->[0];
	return $self->{release} if $self->{release};
    }

    $sth2->execute($num,$db);
    $sth1->execute($db);
    while (my $ref = $sth1->fetchrow_arrayref()) {
        $self->{release} = $ref->[0];
        return $self->{release} if $self->{release};
    }
}

sub stable_id_name {
    my $self = shift;
    my $st_id = shift || die "stable_id_name: I was expecting a stable ID instance";
    my $name = $st_id->displayName;
    my $st_id_db_id = $self->has_history($st_id);

    if ($self->{name}{$name}) {
	return $self->{name}{$name};
    }

    my $sth1 = $self->dbh->prepare(GET_NAME);
    my $sth2 = $self->dbh->prepare(SET_NAME);
    
    $sth1->execute($name);
    while (my $ref = $sth1->fetchrow_arrayref()) {
        my $db_id = $ref->[0];
	$self->{name}{$name} = $db_id;
        return $db_id;
    }
    
    $sth2->execute($st_id_db_id,$name);
    $sth1->execute($name);
    while (my $ref = $sth1->fetchrow_arrayref()) {
        my $db_id = $ref->[0];
	$self->{name}{$name} = $db_id;
        return $db_id;
    }
}


1;
