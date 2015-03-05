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

$warnings = [warnings{$obj->create_db;}];
is(scalar @{$warnings},0,"no warnings on couchdb temp db creation");

# test the tracking bits and bobs, even though that should work
# because of the couchdb lib's tests, but hey, sue me

$obj->track( 'id' => './files/d12_text_station_raw_2012_10_01.txt.gz',
             'otherdata' => { 'broken_parse' => 'choked on vomit' },
    );

my $testdoc = $obj->get_doc('./files/d12_text_station_raw_2012_10_01.txt.gz');
is($testdoc->{'broken_parse'},'choked on vomit','parser can also track via couchdb');


# delete the test db
my $rs;

$warnings = [warnings{$rs = $obj->delete_db;}];
is(scalar @{$warnings},0,"no warnings on couchdb temp db deletion");

isa_ok($rs,'DB::CouchDB::Result','response to delete is correct class');
is( $rs->err, undef , 'database deletion should pass here');



done_testing();
