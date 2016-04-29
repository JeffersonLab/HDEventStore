#!/usr/bin/env perl
$target = $ARGV[0];
$replacement = $ARGV[1];
$file = $ARGV[2];
if (!$file) {print_help(); exit 1;}
#print "gsl.pl: making backup for change: $target -> $replacement\n";
system("cp -pv $file $file~");
open(ORIGINAL, "$file~");
open(EDITTED, ">$file");
while (<ORIGINAL>) {
    s/$target/$replacement/g;
    print EDITTED;
}
exit;

sub print_help {
print "usage: gsr.pl <target-string> <replacement-string> <filename>\n";
}
