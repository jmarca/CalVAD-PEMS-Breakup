use Test::Modern; # see done_testing()
use Carp;
use Data::Dumper;

use File::Temp qw/tempdir /;
use English qw(-no_match_vars);

use IO::File;
use IO::Uncompress::Gunzip qw(gunzip $GunzipError);
#use File::Spec;

use CalVAD::PEMS::Breakup;


my $host =  '127.0.0.1';
my $port = $ENV{PGPORT} || 5432;
my $db = $ENV{PGDATABASE} || 'testdb';
my $user = $ENV{PGTESTUSER} || $ENV{PGUSER} || 'postgres';
my $pass = $ENV{PGTESTPASS} || '';

my $chost = $ENV{COUCHDB_HOST} || '127.0.0.1';
my $cport = $ENV{COUCHDB_PORT} || 5984;
my $cdb = $ENV{COUCHDB_TESTDATABASE} || 'test_calvad_db';
my $cuser = $ENV{COUCHDB_USER} || 'admin';
my $cpass = $ENV{COUCHDB_PASS} || '';

my $outdir = tempdir( CLEANUP => 1 );

my $year = 2012;
my $district = '12';

isnt($port,undef,'need a valid port defined in env PGPORT');
isnt($user,undef,'need a valid user defined in env PGUSER');
isnt($db,undef,'need a valid db defined in env PGDATABASE');
isnt($host,undef,'need a valid host defined in env PGHOST');

isnt($cport,undef,'need a valid port defined in env COUCHDB_PORT');
isnt($cpass,undef,'need a valid password defined in env COUCHDB_PASS');
isnt($cuser,undef,'need a valid user defined in env COUCHDB_USER');
isnt($cdb,undef,'need a valid db defined in env COUCHDB_TESTDATABASE');
isnt($chost,undef,'need a valid host defined in env COUCHDB_HOST');


my $obj;
my $warnings;

$warnings = [warnings{
    $obj = CalVAD::PEMS::Breakup->new(
        # first the sql role
        'host_psql'     => $host,
        'port_psql'     => $port,
        'dbname_psql'   => $db,
        'username_psql' => $user,
        'password_psql' => $pass, # never use sql password, use .pgpass

        # now the couchdb role
        'host_couchdb'     => $chost,
        'port_couchdb'     => $cport,
        'dbname_couchdb'   => $cdb,
        'username_couchdb' => $cuser,
        'password_couchdb' => $cpass,
        'create'           => 1,

        'output_dir'        => $outdir,
        'year'              => $year,
        'district'          => $district,
        );
             }];

is(scalar @{$warnings},0,"no warnings on object creation");

my $vds_info = $obj->vds_info;
is_deeply($vds_info,{},'without live db, get empty vds hash');

my $file;
my @files;
my $z;
my $w;
my $store;
my $sumlines;
my $detectors = {};

$file = File::Spec->rel2abs('./t/files/d12_text_station_raw_2012_10_01.txt.gz');
$z = IO::Uncompress::Gunzip->new($file)
    or croak "IO::Uncompress::Gunzip failed: $GunzipError\n";
$w = [warnings{ $obj->copy_in($z); }];
$z->close();
is(scalar @{$w},0,'no problems parsing file');
$store = $obj->store;
is(scalar keys %{$store},2013,'got the right number of detectors');
$sumlines = 0;
for(keys %{$store}){
    like($_,qr/^12\d+$/,'detector id looks like a detector id');
    $detectors->{$_}=1;
    my $cnt = scalar @{$store->{$_}};
    $sumlines+=$cnt;
    # cmp_ok($cnt,'<=',2880,"there are 2880 or less obs for $_");
}
is($sumlines,5603866,'read in every line');


$obj->breakup;

# side effect only, it should have written out to the filesystem
@files=();
File::Find::find( \&loadfiles, $outdir );

is(scalar @files,scalar keys %{$detectors},'wrote one file per detector');

##################################################
# another file
##################################################
$file = File::Spec->rel2abs('./t/files/d12_text_station_raw_2012_10_02.txt.gz');
$z = IO::Uncompress::Gunzip->new($file)
    or croak "IO::Uncompress::Gunzip failed: $GunzipError\n";
$w = [warnings{ $obj->copy_in($z); }];
$z->close();
is(scalar @{$w},0,'no problems parsing file');
$store = $obj->store;
is(scalar keys %{$store},2035,'got the right number of detectors');
$sumlines = 0;
for(keys %{$store}){
    like($_,qr/^12\d+$/,'detector id looks like a detector id');
    $detectors->{$_}=1;
    my $cnt = scalar @{$store->{$_}};
    $sumlines+=$cnt;
    # cmp_ok($cnt,'<=',2880,"there are 2880 or less obs for $_");
}
is($sumlines,5550455,'read in every line');


$obj->breakup;

# side effect only, it should have written out to the filesystem
@files=();
sub loadfiles {
    if (-f) {
        push @files, grep { /\.txt$/sxm } $File::Find::name;
    }
    return;
}
File::Find::find( \&loadfiles, $outdir );

is(scalar @files,scalar keys %{$detectors},'wrote one file per detector');


done_testing();
