
$RESTDIR = "/w/halld-scifs1a/data_monitoring/RunPeriod-2014-10/ver12/REST";
$SAVEDIR = "/w/halld-scifs1a/data_monitoring/RunPeriod-2014-10/ver12/REST/badfiles";

system "mkdir -p $SAVEDIR";

@lines = `find $RESTDIR -name \*.hddm -exec ls -lh '{}' \\;`;
#@lines = split '\n',$output;

foreach $line (@lines) {
    chomp $line;
    @tokens = split / /,$line;
    #print "${tokens[9]}  ${tokens[4]}\n";

    if( $tokens[4] == "4.9K" ) {
	print "moving ${tokens[9]} ...\n";
	system "mv ${tokens[9]} /w/halld-scifs1a/data_monitoring/RunPeriod-2014-10/ver12/REST/badfiles";
    }
}
