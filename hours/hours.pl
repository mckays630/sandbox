#!/usr/bin/perl
use common::sense;
use Data::Dumper;

my %work;

while (<>) {
    chomp;
    next if /^Date|^Reactome|^PanCancer|^Mod/;
    my @data = split ",";
    $data[0] || next;
    $data[0] =~ s/^\s+|\s+$|\.?//g;
    $work{ucfirst($data[0])} += $data[1] || 0;
}

my $total_hr;
my $total_num;

say "Hours"
while (my ($k,$v) = each %work) {
    say join("\t",$k,$v,$v*52);
    $total_hr += $v;
    $total_num += $v*52;
}
say join("\t","Total",$total_hr,$total_num);


