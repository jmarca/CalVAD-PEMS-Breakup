use Test::Modern; # see done_testing()
use Carp;
use Data::Dumper;
use Config::Any; # config db credentials with config.json

use File::Temp qw/tempdir /;
use English qw(-no_match_vars);

use IO::File;
use IO::Uncompress::Gunzip qw(gunzip $GunzipError);
#use File::Spec;

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


my $obj;
my $warnings;

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
        'create'           => 0,

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
