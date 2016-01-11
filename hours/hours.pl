#!/usr/bin/perl -w
use strict;
use Data::Dumper;

my %work;

while (<>) {
    chomp;
    next if /^Date|^Reactome|^PanCancer/;
    my @data = split ",";
    $data[0] || next;
    $data[0] =~ s/^\s+|\s+$//g;
    $work{ucfirst($data[0])} += $data[1] || 0;
}

my $total_hr;
my $total_num;

print "Hours\n";
while (my ($k,$v) = each %work) {
    print join("\t",$k,$v,$v*52), "\n";
    $total_hr += $v;
    $total_num += $v*52;
}
print join("\t","Total",$total_hr,$total_num), "\n";


