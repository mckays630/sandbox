#!/usr/bin/perl -w
use common::sense;
use Data::Dumper;


my %seen;
my %line;
my $num;

while (<>) {
    chomp;
    my ($st_id,$db_id,$class,$name,$spec,$name2,$parent) = split "\t";

#    say "$spec\t$name";
    
    push @{$line{$spec}{parent}{$parent}}, $_;
    push @{$line{$spec}{name}{$name}}, $_;

    if ($seen{$spec}{parent}{$parent}++) {
	say "PARENTS";#say Dumper $line{$spec}{parent}{$parent};
    }

    if ($seen{$spec}{name}{$name}++) {
	say "NAMES";#, Dumper $line{$spec}{name}{$name};
    }
    $num++;
}

say "I looked at $num lines";
