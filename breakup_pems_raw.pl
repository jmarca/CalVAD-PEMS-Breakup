#!/usr/bin/perl -w
use strict;
use warnings;
use Carp;
use Data::Dumper;
use Getopt::Long;
use Pod::Usage;
use Text::CSV;
use IO::File;
use IO::Uncompress::Gunzip qw(gunzip $GunzipError);

use CalVAD::PEMS::Breakup;

use English qw(-no_match_vars);

use version; our $VERSION = qv('0.2.0');

# this script breaks up daily pems files into annual by vdsid

#some global variables for bulk doc calls
my @trackdocs  = ();
my @big_update = ();

#### This is the part where options are set

my $year;
my $district;
my $path;
my $help;
my $shrink   = 1;
my $pretty   = 0;
my $outdir   = q{.};

my $user   = $ENV{PSQL_USER} || q{};
my $pass   = q{}; # never use a postgres password, use config file or .pgpass
my $host   = $ENV{PSQL_HOST} || '127.0.0.1';
my $dbname = $ENV{PSQL_DB}   || 'spatialvds';
my $port   = $ENV{PSQL_PORT} || 5432;

my $cdb_user   = $ENV{COUCHDB_USER} || q{};
my $cdb_pass   = $ENV{COUCHDB_PASS} || q{}; # need a config file for this
my $cdb_host   = $ENV{COUCHDB_HOST} || '127.0.0.1';
my $cdb_dbname = $ENV{COUCHDB_DB}   || 'pems_brokenup';
my $cdb_port   = $ENV{COUCHDB_PORT} || '5984';

my $reparse;
my $uniquebit;

my $result = GetOptions(
    'username:s'  => \$user,
    'host:s'      => \$host,
    'db:s'        => \$dbname,
    'port:i'      => \$port,
    'cusername:s' => \$cdb_user,
    'chost:s'     => \$cdb_host,
    'cdb:s'       => \$cdb_dbname,
    'cport:i'     => \$cdb_port,
    'year=i'      => \$year,
    'district=i'  => \$district,
    'path=s'      => \$path,
    'reparse'     => \$reparse,
    'outdir=s'    => \$outdir,
    'help|?'      => \$help
);

if ( !$result || $help ) {
    pod2usage(1);
}

# options dictate files to parse, db to use/create

my $rs;    # where to put db responses
if ( !$district ) {
    croak 'a district is required!';
}

my $pattern = join q{_}, $district < 10 ? "d0$district" : "d$district",
  qw{ text station raw }, $year;
$pattern = join q{}, $pattern, '.*gz';

carp "directory path is $path, pattern is $pattern";
my @files = ();

sub loadfiles {
    if (-f) {
        push @files, grep { /$pattern/sxm } $File::Find::name;
    }
    return;
}
File::Find::find( \&loadfiles, $path );

@files = sort { $a cmp $b } @files;
carp 'going to process ', scalar @files, ' files';

# make sure the outdir is a directory that is writable
if ( not( -d $outdir && -w $outdir ) ) {
    carp
"Output directory: [$outdir] does not exist.  It will be created unless you abort now";
    sleep 2;
}
carp "creating the parser";

my $parser = CalVAD::PEMS::Breakup->new(

    # first the sql role
    'host_psql'     => $host,
    'port_psql'     => $port,
    'dbname_psql'   => $dbname,
    'username_psql' => $user,
    'password_psql' => $pass, # never use sql password, use .pgpass

    # now the couchdb role
    'host_couchdb'     => $cdb_host,
    'port_couchdb'     => $cdb_port,
    'dbname_couchdb'   => $cdb_dbname,
    'username_couchdb' => $cdb_user,
    'password_couchdb' => $cdb_pass,
    'create'           => 1,

    'output_dir'        => $outdir,
    'year'              => $year,
    'district'          => $district,
);

# first some useful subroutines
$parser->create_db;
$parser->fetch_vds_metadata;

# main program loop

while ( my $input = shift @files ) {

    # pick off the actual filename for tracking the file
    my $currentfile;
    if ( $input =~ /.*\/(.*gz)$/sxm ) {
        $currentfile = $1;
    }
    else {
        croak 'regular expressions fail again';
    }
    my $seekpos = $parser->track( 'id' => $currentfile, );
    if ( !$reparse && $seekpos < 0 ) {
        carp "skipping $currentfile, already done according to parser ";
        next;    # skip this document, go onto the next one
    }
    else {
        carp "process $currentfile";
    }

    # okay let's process the file, break it to bits
    my $z = IO::Uncompress::Gunzip->new($input)
      or croak "IO::Uncompress::Gunzip failed: $GunzipError\n";
    my $err = eval { $parser->copy_in($z); };
    if ( $err || $EVAL_ERROR ) {
        $parser->track(
            'id'        => $currentfile,
            'otherdata' => { 'broken_parse' => $EVAL_ERROR },
        );
    }
    else {
        $parser->track(
            'id'  => $currentfile,
            'row' => $z->input_line_number(),
        );
    }
    $z->close();

    $parser->breakup;

    $parser->track(
        'id'        => $currentfile,
        'processed' => 1,
    );

}

# all done
# compress the files

my @processedfiles = ();

$pattern = "$year\.txt\$";

my $d =
    $district < 10
    ? 'D0' . $district
    : 'D' . $district;

my $searchpath = join q{/}, $outdir, $d;

sub checkfiles {
    if (-f) {
        push @processedfiles, grep { /$pattern/sxm } $File::Find::name;
    }
    return;
}
find( \&checkfiles, $searchpath );

for my $bf (@processedfiles){
    my @sargs = ("xz", $bf);
    carp join q{ },@sargs;
    system(@sargs) == 0  or die "system @sargs failed: $?";
}


1;

__END__

=head1 NAME

    breakup_pems_raw - breakup those pesky daily pems files

=head1 USAGE

    perl -w breakup_pems_raw.pl --path /data/pems/downloaded/raw/data --district 3 --out /data/pems/breakup --year 2010 --reparse > bpr_03.txt 2>&1 &


=head1 REQUIRED ARGUMENTS

       -district the district number (1 through 12) you are trying to process
       -month    the month of the year (1 through 12) you are trying to process
       -year     the year (for example, 2007) of the month you are trying to process
       -path     the directory in which the target raw PeMS data files reside

   (the district, month, and year will be combined to create a couchdb db name that will accept the data)


=head1 OPTIONS

       -district the district number (1 through 12) you are trying to process
       -year     the year (for example, 2007) of the month you are trying to process
       -path     the directory in which the target raw PeMS data files reside
       -reparse  probably not used.  If a file failed to be read, it will be reread
       -outdir   the directory to which the split up files will be written

       -help     brief help message

       -username optional, username for the pg database
       -host     optional, host to use for postgres
       -db       optional, database to use for postgres, defaults to spatialvds
       -port     optional, defaults to pg standard

       -cusername  optional,  couchdb user
       -chost      optional,  couchdb host, default localhost
       -cdb        optional,  couchdb dbname, default pemsrawdocs
       -cport      optional,  couchdb port, default couchdb-standard 5984

     the database options can also be read from the following
     environment variables:

     $ENV{PSQL_USER} || q{};
     $ENV{PSQL_PASS} || q{};  # leave as is, use .pgpass file instead
     $ENV{PSQL_HOST} || '127.0.0.1';
     $ENV{PSQL_DB}   || 'spatialvds';
     $ENV{PSQL_PORT} || 5432;

     $ENV{COUCHDB_USER} || q{};
     $ENV{COUCHDB_PASS} || q{};
     $ENV{COUCHDB_HOST} || '127.0.0.1';
     $ENV{COUCHDB_DB}   || 'pems_brokenup';
     $ENV{COUCHDB_PORT} || '5984';



=head1 DIAGNOSTICS

=head1 EXIT STATUS

1

=head1 CONFIGURATION AND ENVIRONMENT

   I'm for the environment!


=head1 LICENSE AND COPYRIGHT

This program is free software, (c) 2015 James E Marca under the same terms as Perl itself.

=head1 DESCRIPTION

    B<This program> will read the given input file(s) and save the
    broken up data to various per-detector files at the output
    directory, and track the files processed in the specified couchdb
    as documents.
