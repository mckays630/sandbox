#!/usr/bin/perl
use common::sense;
use Data::Dumper;
use List::Util 'shuffle';
use HTTP::Tiny;


my $query_url  = '/content/query?q=';
my %count;
my %links;

my $web = HTTP::Tiny->new();

my $log = get_log();

open OUT, ">/usr/local/gkb/website/html/test_links.html";

while (<$log>) {
    chomp;
    s/^.+\/cgi-bin/\/cgi-bin/;
    s/ HTTP.+$//;

    my ($source) = /SOURCE=([^ \&\;]+)/;
    my ($id) = /ID=([^ \&\;]+)/;
    $id =~ s/\%20//g;

    if ($id =~ /^REACT_|^R-/) {
	$source = 'REACTOME';
    }

    next if $source =~ /[^_.+0-9a-zA-Z]+/;
    next if $id =~ /[^_.+0-9a-zA-Z]+/;
    next unless $source && $id;

    $source = uc($source);
    next if $source eq 'REACTOME' && $id !~ /^R/;

    $count{$source}++;

    my $link = qq(<a target="linkout" href="http://reactomerelease.oicr.on.ca/$_">$_</a><br>);
    $links{$source}{$link}++;
}

my @sources = sort {$count{$b} <=> $count{$a}} keys %count;

say OUT "<h1>Common external link types</h1>";
for my $source ( grep {$count{$_} >= 50} @sources) {
    next if $source eq 'COMPOUND';

    say OUT "<h2>$source</h2>";
    my @links = shuffle(keys %{$links{$source}});

    my @working;
    my @broken;
    for my $link (@links) {
	last if @working == 2 && @broken == 2;
	say scalar(@working), " ", scalar(@broken), " <-";
	my $status = query_url($link);
	my $to_push = $status eq 'BAD' ? \@broken : \@working;
	next if @$to_push == 2;
	push @$to_push, $link;
    }

    say OUT '<h3>Sample links that work</h3>';
    say OUT join("\n",@working);
    say OUT '<h3>Sample links that find no result</h3>';
    say OUT join("\n",@broken);

    last if $count{$source} < 50;
    say join("\t",$count{$source},$source);
}

close OUT;

sub get_log {
    open my $in, "grep 'cgi-bin/link?' /usr/local/gkb/website/logs/transfer_log |" or die $!;
    return $in;
}


sub query_url {
    my $url = shift;
    $url =~ s!^.+href="([^"]+)".+$!$1!;

    my $response = $web->get($url);

    if ($response->{success}) {
        my $text = $response->{content};
	my $status = $text =~ /No matches for/ ? 'BAD' : 'GOOD';
	say "$url $status";
	return $status;
    }

    say "$url BAD";
    return 'BAD';
}
