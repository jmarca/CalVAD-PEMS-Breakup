use Test::Modern; # see done_testing()
use Carp;
use Data::Dumper;

use File::Path qw/remove_tree/;
use File::Temp qw/tempdir /;

use CalVAD::PEMS::Breakup;


my $host =  '192.168.0.1';
my $port = $ENV{PGPORT} || 5432;
my $db = $ENV{PGDATABASE} || 'spatialvds';
my $user = $ENV{PGTESTUSER} || $ENV{PGUSER} || 'slash';
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

my $connect;
eval {
  $connect = $obj->_connection_psql;
};
if($@) {
  warn $@;
}

isnt($connect, undef, 'db connection should be possible');
like($connect,qr/Testbed::Spatial::VDS::Schema/,'it is okay');

$warnings = [warnings{
    $obj->fetch_vds_metadata();
             }];
is(scalar @{$warnings},0,"no warnings fetching vds metadata");

my $vds_info = $obj->vds_info;
is(scalar keys %{$vds_info},17231,'got all the vds sites');

done_testing();
