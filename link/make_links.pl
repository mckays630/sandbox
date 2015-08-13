#!/usr/bin/perl
use common::sense;

my $eb_url = '/cgi-bin/eventbrowser?ID=';
my $eb_st_url     = '/cgi-bin/eventbrowser_st_id?ST_ID=';
my $query_url  = '/content/query?q=';
my %count;
my %seen;
while (<>) {
    chomp;

    my ($source) = /SOURCE=([^ \&\;]+)/;
    my ($id) = /ID=([^ \&\;]+)/;
    $id =~ s/\%20//g;

    next if $source =~ /[^_.+0-9a-zA-Z]+/;
    next if $id =~ /[^_.+0-9a-zA-Z]+/;

    next if ++$count{uc($source)} > 10;
    next unless $source && $id;

    say qq(<a target="linkout" href="$_">$_</a><br>);
}
