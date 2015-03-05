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

use Config::Any; # config db credentials with config.json

use CalVAD::PEMS::Breakup;

use English qw(-no_match_vars);

use version; our $VERSION = qv('0.2.0');

# this script breaks up daily pems files into annual by vdsid

#### This is the part where options are set

##################################################
# read the config file
##################################################
my $config_file = './config.json';
my $cfg = {};

# check if right permissions on file, if so, use it
if( -e $config_file){
    my @mode = (stat($config_file));
    my $str_mode = sprintf "%04o", $mode[2];
    if( $str_mode == 100600 ){

        $cfg = Config::Any->load_files({files => [$config_file],
                                        flatten_to_hash=>1,
                                        use_ext => 1,
                                       });
        # simplify the hashref down to just the one file
        $cfg = $cfg->{$config_file};
    }else{
        croak "permissions for $config_file are $str_mode.  Set permissions to 0600 (only the user can read or write)";
    }
}
else{
  # if no config file, then just note that and move on
    carp "no config file $config_file found";
}

##################################################
# translate config file into variables, for command line override
##################################################

my $year     = $cfg->{'year'};
my $district = $cfg->{'district'};
my $path     = $cfg->{'path'};
my $help;
my $outdir = $cfg->{'outdir'} || q{};

my $user = $cfg->{'postgresql'}->{'username'} || $ENV{PSQL_USER} || q{};
my $pass = $cfg->{'postgresql'}->{'password'}
  || q{};    # never use a postgres password, use config file or .pgpass
my $host = $cfg->{'postgresql'}->{'host'} || $ENV{PSQL_HOST} || '127.0.0.1';
my $dbname =
     $cfg->{'postgresql'}->{'breakup_pems_raw_db'}
  || $ENV{PSQL_DB}
  || 'spatialvds';
my $port = $cfg->{'postgresql'}->{'port'} || $ENV{PSQL_PORT} || 5432;

my $cdb_user =
  $cfg->{'couchdb'}->{'auth'}->{'username'} || $ENV{COUCHDB_USER} || q{};
my $cdb_pass = $cfg->{'couchdb'}->{'auth'}->{'password'}
  || q{};
my $cdb_host = $cfg->{'couchdb'}->{'host'} || $ENV{COUCHDB_HOST} || '127.0.0.1';
my $cdb_dbname =
     $cfg->{'couchdb'}->{'breakup_pems_raw_db'}
  || $ENV{COUCHDB_DB}
  || 'pems_brokenup';
my $cdb_port = $cfg->{'couchdb'}->{'port'} || $ENV{COUCHDB_PORT} || '5984';

my $reparse = $cfg->{'reparse'} || q{};

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
    carp 'a district is required!';
    pod2usage(1);
}
if ( !$year ) {
    carp 'a year is required!';
    pod2usage(1);
}
if ( !$path ) {
    carp 'a path is required!';
    pod2usage(1);
}
if ( !$outdir ) {
    carp 'an output directory (-outdir) is required!';
    pod2usage(1);
}

# make sure the outdir is a directory that is writable
if ( not( -d $outdir && -w $outdir ) ) {
    carp
"Output directory: [$outdir] does not exist.  It will be created unless you abort now";
    sleep 2;
}

my $pattern = join q{_}, $district < 10 ? "d0$district" : "d$district",
  qw{ text station raw }, $year;
$pattern = join q{}, $pattern, '.*gz';

carp "using $pattern to search for files below the directory $path";

my @files = ();
sub loadfiles {
    if (-f) {
        push @files, grep { /$pattern/sxm } $File::Find::name;
    }
    return;
}
File::Find::find( \&loadfiles, $path );

# sorting the files makes it easier to follow along progress by
# reading log messages
@files = sort { $a cmp $b } @files;
carp 'going to process ', scalar @files, ' files';

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
carp "checking that couchdb $cdb_dbname exists and creating if not";
$parser->create_db;

carp "getting vds metadata from postgresql database $dbname";
my $check;
eval{$parser->fetch_vds_metadata;};
if($@){
    carp " there was a problem accessing the database.  Check to make sure that the username, password, and host are correct.  The error message (probably unhelpful) is\n$@";
}

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
File::Find::find( \&checkfiles, $searchpath );

@processedfiles = sort { $a cmp $b } @processedfiles;

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


=head1 CONFIGURATION FILE

All options can be set in a configuration file called "config.json"
placed in the same directory as this program.  This file should be
chmod 0600, or your passwords will exposed to others.  Therefore, if
this file is not made readable and writable only by the file user,
then this file will not be used.

An example config.json file.  Note that this must be JSON, and so all
quotes must be double quotes, all commas must come at the end of
the line, no comments, and no dangling commas.

{
    "couchdb": {
        "host": "192.168.0.1",
        "port":5984,
        "breakup_pems_raw_db": "pems_brokenup",
        "auth":{"username":"james",
                "password":"admin party mode oh my"
               }
    },
    "postgresql":{
        "host":"192.168.0.1",
        "port":5432,
        "username":"james",
        "password":"super secret postgresql password",
        "breakup_pems_raw_db":"spatialvds"
    }
}

Note that for postgresql, it is not necessary to add the password if
you are using the .pgpass file that postgresql recommends.  Also note
that due to historical accident, there is a difference in how I parse
the auth stuff for postgres and couchdb.  If you must know, it is
because couchdb is very much like a web client, and so in javascript I
can plonk that config hash right in the request params and it will set
the auth for the request properly.

The command line options, listed below, will override these config file values.

There is no command line option for passwords.  This is deliberate, so
that you don't expose usernames and passwords to any local user's
invocation of 'ps'.

=head1 REQUIRED ARGUMENTS

These need to be set either in the config file or on the command line.
The command line will override the config file.

       -district the district number (1 through 12) you are trying to process
       -year     the year (for example, 2007) you are trying to process
       -path     the directory in which the target raw PeMS data files reside

   The district and year are used to match the source PeMS files, and to help name the output files.


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
