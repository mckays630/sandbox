#!/usr/bin/perl 
use common::sense;
use HTTP::Tiny;

use constant HOST => 'http://reactomerelease.oicr.on.ca/';

die "Sorry, root permission required.\n" unless $> == 0;

my $host = shift || HOST;

my $url = HOST . "ReactomeRESTfulAPI/RESTfulWS/queryById/DatabaseObject/29358";
my $response = HTTP::Tiny->new->get($url);

my $stamp = timestamp();

if ($response->{success}) {

    my $content = $response->{content};
    my $OK = $content =~ /ATP/ && $content =~ /displayName/;

    my $stamp = timestamp();

    if ($OK) {
	say "OK $stamp";
    }
    else {
	say STDERR "Error, restarting $stamp";
	system "/etc/init.d/tomcat7 restart";
    }
}
else {
    say STDERR "Error, restarting $stamp";
    system "/etc/init.d/tomcat7 restart";
}

sub timestamp {
    my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst)=localtime(time);
    return sprintf ( "%04d-%02d-%02d:%02d:%02d:%02d",
                                   $year+1900,$mon+1,$mday,$hour,$min,$sec);
}
