use Test::Modern; # see done_testing()
use Carp;
use Data::Dumper;

use File::Temp qw/tempdir /;
use Config::Any; # config db credentials with config.json

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

# override variables that might be in config.json with test.config.json ones
my $outdir = 't/files';
my @sargs = ('perl', '-w', 'fix_filenames.pl',
             '-district' ,3,
             '-outdir',$outdir,
             '-year',2012,
             '-db',$cfg->{'postgresql'}->{'breakup_pems_raw_db'},
             '-host',$cfg->{'postgresql'}->{'host'},
             '-username',$cfg->{'postgresql'}->{'username'},
             '-port',$cfg->{'postgresql'}->{'port'},
    );
#carp join q{ },@sargs;
my $warnings;
$warnings = [warnings{
    system(@sargs) == 0  or die "system @sargs failed: $?";
             }];

is(scalar @{$warnings},0,"no warnings on object creation");

# it should have moved one file
# side effect only, it should have written out to the filesystem
my @files=();
sub loadfiles {
    if (-f) {
        push @files, grep { /_ML_\d{4}\.txt(.xz)?$/sxm } $File::Find::name;
    }
    return;
}
File::Find::find( \&loadfiles, $outdir . '/D03/' );
is(scalar @files,1,'moved one file in 2012');
is($files[0],'t/files/D03/80/W/JWO_ENTERPRISE_BL/317033_ML_2012.txt','fixed the path and detector type');


@sargs = ('perl', '-w', 'fix_filenames.pl',
             '-district' ,3,
             '-outdir',$outdir,
             '-year',2010,
             '-db',$cfg->{'postgresql'}->{'breakup_pems_raw_db'},
             '-host',$cfg->{'postgresql'}->{'host'},
             '-username',$cfg->{'postgresql'}->{'username'},
             '-port',$cfg->{'postgresql'}->{'port'},
    );

$warnings = [warnings{
    system(@sargs) == 0  or die "system @sargs failed: $?";
             }];

is(scalar @{$warnings},0,"no warnings on object creation");
@files=();
File::Find::find( \&loadfiles, $outdir . '/D03/' );
is(scalar @files,2,'moved another file in 2010');
is($files[0],'t/files/D03/80/W/JWO_ENTERPRISE_BL/317033_ML_2010.txt.xz','fixed the path and detector type');
is($files[1],'t/files/D03/80/W/JWO_ENTERPRISE_BL/317033_ML_2012.txt','fixed the path and detector type');

done_testing();

use File::Copy qw(move);
use File::Path qw(rmtree);

move 't/files/D03/80/W/JWO_ENTERPRISE_BL/317033_ML_2012.txt', 't/files/D03/_name_/317033__2012.txt' or croak $!;

move 't/files/D03/80/W/JWO_ENTERPRISE_BL/317033_ML_2010.txt.xz', 't/files/D03/_name_/317033__2010.txt.xz' or croak $!;

rmtree 't/files/D03/80';
