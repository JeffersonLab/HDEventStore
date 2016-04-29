#!/usr/local/bin/perl
#
# This program is a quick hack to find the file names of all the immediate
# parent files of all the PDS files listed on the command line. The output
# is a single line, comprising a space-separated list of the parent files.

use warnings;
use strict;
use English;
use DBI;

# We will connect to the EventStore mysql db on lnx151.
# Modify this line to adjust for server movement...
my $dsn = "DBI:mysql:database=EventStore;host=lnx151.lns.cornell.edu";  ### UPDATE

my $contentCmd = "/nfs/cleo3/Offline/rel/development/src/GroupEventStoreToolkit/ESFileContent -v";


sub usage ()
{
    die "Usage: findParents.pl pds-file ...\n";
}


sub getSuffix ($)
{
    my $fileName = shift;
    return ($fileName =~ /.*(\.\w+)/)[0];
}


sub parsePDSSyncValue ($)
{
    my $line = shift;
    return ($line =~ m/.*: (\d+)\/\d+\/(\d+)/);
}


sub parseFileIdLine ($)
{
    # This parses the File Identifier: line from a location file.
    # It returns the list of file identifiers on that line.
    my $line = shift;
    $line =~ s/File identifiers: //;
    return split(" ", $line);
}

sub lookupFileName ($$)
{
    my $dbh = shift;
    my $fileId = shift;
    my $sth = $dbh->prepare("SELECT fileName FROM FileID WHERE fileid=$fileId");
    $sth->execute();
## TODO: at this point we should assert that there was at most one match
##       and handle the case of no match. For now, we trust mysql to keep
##       its primary keys unique.
    my $ref = $sth->fetchrow_hashref();
    $sth->finish();
    return $ref->{'fileName'};
}


sub displayParentsFromLocation($$$)
{
    # Given a location file id, we get its file name (there should only be
    # one!) and look inside it to see which run/uniqueId pairs it contains.
    # Then we find the corresponding file ids for those run/uniqueId pairs
    # and print the file names.
    my $dbh = shift;
    my $locationFileId = shift;
    my $inputFileSuffix = shift;

    my $locationFileName = lookupFileName($dbh, $locationFileId);
    die "File id $locationFileId not found in FileID database\n"
    	unless (defined($locationFileName));

    # Look inside the location file for the parent file identifiers.
    my @lines = `$contentCmd $locationFileName | grep 'File identifiers: '`;
    # For each file id on each line, print out the matching file name
    for my $line (@lines)
    {
	my @fileIds = parseFileIdLine($line);
	for my $fileId (@fileIds)
	{
	    my $fileName = lookupFileName($dbh, $fileId);
	    die "File id $fileId not found in FileID database\n"
		unless (defined($fileName));
	    # Only print out files with same suffix as the input file
	    if ($inputFileSuffix eq getSuffix($fileName))
	    {
		print "$fileName ";
	    }
	}
    }
}


sub displayParentInfo ($$$$)
{
    my $dbh = shift; # Database handle
    my $runNum = shift;
    my $uniqueId = shift;
    my $inputFileSuffix = shift;

    my $sth = $dbh->prepare("SELECT locationFileId FROM Location WHERE " .
    			    "run=$runNum AND uid=$uniqueId");
    $sth->execute();
    my $locationFileId = -1;
    while (my $ref = $sth->fetchrow_hashref())
    {
	my $newLocationFileId = $ref->{'locationFileId'};
	if ($locationFileId != $newLocationFileId)
	{
	    $locationFileId = $newLocationFileId;
	    displayParentsFromLocation($dbh, $locationFileId, $inputFileSuffix);
	}
    }
    $sth->finish();
}


########
# MAIN #
########

# Make sure that we have at least one argument
usage() unless $#ARGV >= 0;

# Connect to the database.
# connect prints an explanation and dies if there is an error.
my $dbh = DBI->connect($dsn, "cleo", "cleoc", {'RaiseError' => 1});

while ($#ARGV >= 0)
{
    my $inputFile = shift;
    my $fileSuffix = getSuffix($inputFile);

    die "Input file $inputFile doesn't have a suffix\n"
	unless defined($fileSuffix);

    my @lines = `$contentCmd $inputFile | grep 'SyncValue          : '`;
    die "No sync value lines in $inputFile\n" unless $#lines >= 0;

    # For each line, if it has a different run number/uniqueid pair from
    # the previous line, then we need to add it to our output.
    my $runNum = -1;
    my $uniqueId = -1;
    for my $line (@lines)
    {
	my ($newRunNum, $newUniqueId) = parsePDSSyncValue($line);
	if ($newRunNum != $runNum || $newUniqueId != $uniqueId)
	{
	    displayParentInfo($dbh, $newRunNum, $newUniqueId, $fileSuffix);
	    $runNum = $newRunNum;
	    $uniqueId = $newUniqueId;
	}
    }
}
print "\n";

# Drop the connection to the db
$dbh->disconnect();

exit 0;
