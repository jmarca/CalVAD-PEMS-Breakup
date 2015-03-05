use Test::Modern; # see done_testing()
use Carp;
use Data::Dumper;

use File::Temp qw/tempdir /;
use English qw(-no_match_vars);

use IO::File;
use IO::Uncompress::Gunzip qw(gunzip $GunzipError);
#use File::Spec;

use Config::Any; # config db credentials with config.json
use UUID;

use CalVAD::PEMS::Breakup;



##################################################
# read the config file
##################################################
my $config_file = './test.config.json';
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

my $path     = $cfg->{'path'};
my $help;

my $user = $cfg->{'postgresql'}->{'username'} || $ENV{PGUSER} || q{};
my $pass = $cfg->{'postgresql'}->{'password'}
  || q{};    # never use a postgres password, use config file or .pgpass
my $host = $cfg->{'postgresql'}->{'host'} || $ENV{PGHOST} || '127.0.0.1';
my $dbname =
     $cfg->{'postgresql'}->{'breakup_pems_raw_db'}
  || $ENV{PGDATABASE}
  || 'test_calvad_db';
my $port = $cfg->{'postgresql'}->{'port'} || $ENV{PGPORT} || 5432;

my $cdb_user =
  $cfg->{'couchdb'}->{'auth'}->{'username'} || $ENV{COUCHDB_USER} || q{};
my $cdb_pass = $cfg->{'couchdb'}->{'auth'}->{'password'}
  || q{};
my $cdb_host = $cfg->{'couchdb'}->{'host'} || $ENV{COUCHDB_HOST} || '127.0.0.1';
my $cdb_dbname =
     $cfg->{'couchdb'}->{'breakup_pems_raw_db'}
  || $ENV{COUCHDB_TESTDB}
  || 'test_calvad_pems_brokenup';
my $cdb_port = $cfg->{'couchdb'}->{'port'} || $ENV{COUCHDB_PORT} || '5984';

my $reparse = $cfg->{'reparse'} || q{};

my $outdir = tempdir( CLEANUP => 1 );
my $year = 2012;
my $district = '12';

isnt($port,undef,'need a valid port defined in env PGPORT');
isnt($user,undef,'need a valid user defined in env PGUSER');
isnt($dbname,undef,'need a valid db defined in env PGDATABASE');
isnt($host,undef,'need a valid host defined in env PGHOST');

isnt($cdb_port,undef,'need a valid port defined in env COUCHDB_PORT');
isnt($cdb_pass,undef,'need a valid password defined in env COUCHDB_PASS');
isnt($cdb_user,undef,'need a valid user defined in env COUCHDB_USER');
isnt($cdb_dbname,undef,'need a valid db defined in env COUCHDB_TESTDATABASE');
isnt($cdb_host,undef,'need a valid host defined in env COUCHDB_HOST');

# make a unique couchdb name so you don't clobber stuff
$cdb_dbname = $cdb_dbname . UUID::uuid();

my $obj;
my $warnings;

##################################################
# first with a fake username that should not be able to create dbs
##################################################

$warnings = [warnings{
    $obj = CalVAD::PEMS::Breakup->new(
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
        'username_couchdb' => 'asgasg',
        'password_couchdb' => 'agasgasdg',
        'create'           => 1,

        'output_dir'        => $outdir,
        'year'              => $year,
        'district'          => $district,
        );
             }];

is(scalar @{$warnings},0,"no warnings on object creation");

like(
    exception { $obj->create_db; },
    qr/cannot find or create couchdb database/,
    'db creation died as expected',
);



############################################################
# now with the specified user, pass
############################################################

$warnings = [warnings{
    $obj = CalVAD::PEMS::Breakup->new(
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
             }];

is(scalar @{$warnings},0,"no warnings on object creation");

$warnings = [warnings{$obj->create_db;}];
is(scalar @{$warnings},0,"no warnings on couchdb temp db creation");

$warnings = [warnings{$obj->create_db;}];
is(scalar @{$warnings},0,"will silently ignore failure to create a db that already exists");

# test the tracking bits and bobs, even though that should work
# because of the couchdb lib's tests, but hey, sue me

$obj->track( 'id' => './files/d12_text_station_raw_2012_10_01.txt.gz',
             'processed' => 1,
             'row' => 100,
             'otherdata' => { 'broken_parse' => 'choked on vomit' },
    );

my $testdoc = $obj->get_doc('./files/d12_text_station_raw_2012_10_01.txt.gz');
is($testdoc->{'broken_parse'},'choked on vomit','parser can also track via couchdb');

# check that "track" functionality works okay
my $done = $obj->track('id'=>'./files/d12_text_station_raw_2012_10_01.txt.gz');
is($done,-1,'setting processed in track object works okay');


# delete the test db
my $rs;

$warnings = [warnings{$rs = $obj->delete_db;}];
is(scalar @{$warnings},0,"no warnings on couchdb temp db deletion");

isa_ok($rs,'DB::CouchDB::Result','response to delete is correct class');
is( $rs->err, undef , 'database deletion should pass here');



done_testing();
