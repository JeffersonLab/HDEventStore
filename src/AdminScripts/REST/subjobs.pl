#!/usr/bin/env perl

# job management parameters
$NUM_JOB_SUBMIT = 20.;    # number of jobs to submit at once
$NUM_JOBS_QUEUED = 50.;   # minimum number of jobs to keep running at once
                           # if the number of jobs dips below this number, submit more (if available) 

# other parameters
$ES_SCRIPT_DIR = "/home/gxproj3/EventStore/src/AdminScripts/REST";
$JOB_FILE = "inject.jsub";

####################################3

sub submit_job {
    my $run_number = @_[0];

    print "Submitting run $run_number\n";
    $TMP_JOB_FILE = "${JOB_FILE}.${run_number}";
    `cp ${JOB_FILE} ${TMP_JOB_FILE}`;
    `./gsr.pl '<run_number>' ${run_number} ${TMP_JOB_FILE}`;
    `./gsr.pl '<es_script_dir>' ${ES_SCRIPT_DIR} ${TMP_JOB_FILE}`;    
    #print "jsub ${TMP_JOB_FILE}\n";
    `jsub ${TMP_JOB_FILE}`;
    `rm ${TMP_JOB_FILE} ${TMP_JOB_FILE}~`;
}

####################################3

$run_file = shift or die "Must pass run file as argument\n";
open INF,$run_file or die "Could not open file ${run_file}: $!\n";

while(1) {
    for(my $njobs=0; $njobs<$NUM_JOB_SUBMIT; $njobs++) {
	# if we hit the end of the file, we're done submitting jobs!
	if(eof(INF)) {  
	    exit(0);
	}
	
	$run_number = <INF>;
	chomp $run_number;
	if(not $run_number =~ /^\d+$/) {
	    print "Invalid run = $line";
	}
	submit_job($run_number);
    }

    # see how many jobs we have in queue
    $njobs_in_queue = `jobstat -u gxproj3 | wc -l`;
    while( ($njobs_in_queue-1) > $NUM_JOBS_QUEUED ) {
	sleep(600);    # sleep for 5 minutes
	$njobs_in_queue = `jobstat -u gxproj3 | wc -l`;
    }
}
