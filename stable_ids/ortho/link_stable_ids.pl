#!/usr/bin/perl -w
use common::sense;
use Digest::MD5 'md5_hex';
my %db_id;

say "HELLO";
my @releases = reverse 49..53;
for my $rel (@releases) {
    say $rel;
    open TXT, "cat orthos_$rel*.txt |" or die "Could not open file: $!";
    while (<TXT>) {
	chomp;
	my ($st_id,$db_id,$class,$name,$spec,$name2,$parent) = split "\t";
	my $hex = md5_hex($class,$name,$spec,$parent);
	if ($rel == 53) {
	    $db_id{$spec}{$hex} = $db_id;
	    push @{$db_id{$spec}{$name}}, $db_id;
	    push @{$db_id{$spec}{$parent}}, $db_id;
	    push @{$db_id{$spec}{$hex}}, $db_id;

	    say join("\t",$rel,$st_id,$db_id);
	}
        elsif ($db_id{$spec}{$parent} && @{$db_id{$spec}{$parent}} == 1) {
            say join("\t",$rel,$st_id,$db_id{$spec}{$parent}->[0],'UNAMBIGUOUS:PARENT_ID');
        }
	elsif ($db_id{$spec}{$name} && @{$db_id{$spec}{$name}} == 1) {
	    say join("\t",$rel,$st_id,$db_id{$spec}{$name}->[0],'UNAMBIGUOUS:NAME');
	}
	elsif ($db_id{$spec}{$hex} && @{$db_id{$spec}{$hex}} == 1) {
            say join("\t",$rel,$st_id,$db_id{$spec}{$hex}->[0],'UNAMBIGUOUS:HEX');
        }
	else {
	    say join("\t",$rel,$st_id,'.','UNLINKED',$parent,$class,$spec,$name);
	}
    }
}
