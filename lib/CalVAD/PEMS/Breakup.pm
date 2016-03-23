
package CalVAD::PEMS;

use Moops;
# ABSTRACT: Breaks up the daily all-vds-per-district files into yearly per vds files

class Breakup using Moose : ro {

    use Carp;
    use Data::Dumper;
    use File::Path qw(make_path);
    use Testbed::Spatial::VDS::Schema;
    with 'CouchDB::Trackable';

    my $param = 'psql';

    has 'inner_loop_method' =>
        ( is => 'ro',
          isa => 'CodeRef',
          init_arg => undef,
          builder => '_build_inner_loop_method',);

    # PeMS documentation:
    # CSV (ASCII) - Station Raw
    # Raw detector data as reported by the district. Each line contains
    # sample time, and station id followed by flow, occupancy and speed for
    # each lane. Note that occupancy and/or speed may be empty depending on
    # the measurement capabilities of the detectors.
    #
    # Column 	Units 	Description
    #
    # Timestamp: : Sample time as reported by the field element as
    #              MM/DD/YYYY HH24:MI:SS.
    #
    # Station: :Unique station identifier. Use this value to
    #           cross-reference with Metadata files.
    #

    sub _build_inner_loop_method {

        my $slurpcode = sub {
            my ( $z, $store ) = @_;
            while ( my $line = $z->getline() ) {

                # get the date, time, and vdsid, using grep
                if ( $line =~
                    /^(\d{2}\/\d{2}\/\d{4}\s+\d{2}:\d{2}:\d{2})\s*,\s*(\d+),/sxm
                  )
                {
                    my $id = $2;
                    if ( !defined( $store->{$id} ) ) {
                        $store->{$id} = [];
                    }
                    push @{ $store->{$id} }, $line;
                }
                else {
                    croak "regex failed on $line";
                }
            }
            return;
        };
        return $slurpcode;
    }


    method _build__connection_psql {
        my ( $host, $port, $dbname, $username, $password ) =
          map { $self->$_ }
          map { join q{_}, $_, $param }
          qw/ host port dbname username password /;
        my $vdb = Testbed::Spatial::VDS::Schema->connect(
            "dbi:Pg:dbname=$dbname;host=$host;port=$port",
            $username, $password, {}, { 'disable_sth_caching' => 1 } );
        return $vdb;
    }

    with 'DB::Connection' => {
        'name'                  => 'psql',
        'connection_type'       => 'Testbed::Spatial::VDS::Schema',
        'connection_delegation' => qr/^(.*)/sxm,
    };

    has 'output_dir' =>
      ( 'is' => 'rw', 'isa' => 'Str', 'default' => './downloads' );
    has 'year'     => ( 'is' => 'rw', 'isa' => 'Int', 'required' => 1 );
    has 'district' => ( 'is' => 'rw', 'isa' => 'Str', 'required' => 1 );

    has '_stmt' => ( 'is' => 'ro', 'isa' => 'Str', 'lazy_build' => 1 );

    has 'store'    => ( 'is' => 'ro', 'isa' => 'HashRef', 'lazy_build' => 1 );
    has 'handles'    => ( 'is' => 'ro', 'isa' => 'HashRef', 'lazy_build' => 1 );
    has 'vds_info' => ( 'is' => 'ro', 'isa' => 'HashRef', 'lazy_build' => 1 );
    method _build_store {
        my $hashref = {};
        return $hashref;
    }
    method _build_handles {
        my $hashref = {};
        return $hashref;
    }
    method _build_vds_info {
        my $hashref = {};
        return $hashref;
    }

    method _build__stmt {

        my $stmt = <<'FINIS';
    SELECT v.id, v.name, v.cal_pm, v.abs_pm, v.latitude, v.longitude,
           vv.lanes, vv.segment_length, vv.version, vf.freeway_id,
           vf.freeway_dir, vt.type_id AS vdstype, vd.district_id AS district,
           ST_AsEWKT(g.geom) as geom,
    regexp_replace(v.cal_pm,E'[RMNCDGHTSL]?(.*?)\\.*$','\1','g')::numeric as cal_pm_numeric
    FROM vds_id_all v
    JOIN (
        SELECT vds_versioned.* from vds_versioned join (select id,max(version) as version from vds_versioned group by id )vmax USING (id,version)
    ) vv USING (id)
    left outer JOIN vds_points_4326  ON v.id = vds_points_4326.vds_id
    left outer JOIN vds_vdstype vt USING (vds_id)
    left outer JOIN vds_district vd USING (vds_id)
    left outer JOIN vds_freeway vf USING (vds_id)
    left outer JOIN geom_points_4326 g USING (gid)
FINIS
        $stmt =~ s/\s+/ /sxgm;
        return $stmt;
    }

    method BUILD {

        if ( !-e $self->output_dir ) {
            carp 'Going to create a destination directory ', $self->output_dir;
            make_path( $self->output_dir );
        }
        elsif ( !-d $self->output_dir ) {
            confess 'Need to pass a directory to output_dir.  ',
              $self->output_dir,
              ' exists but is not a directory.';
        }

    }

    method _populate_vdsinfo( ArrayRef $vals ) {
        $self->vds_info->{ $vals->[0] } = {
            'id'             => $vals->[0],
            'name'           => $vals->[1],
            'cal_pm'         => $vals->[2],
            'abs_pm'         => $vals->[3],
            'latitude'       => $vals->[4],
            'longitude'      => $vals->[5],
            'lanes_fromPeMS' => $vals->[6],
            'segment_length' => $vals->[7],
            'version'        => $vals->[8],
            'freeway_id'     => $vals->[9],
            'freeway_dir'    => $vals->[10],
            'vdstype'        => $vals->[11],
            'district'       => $vals->[12],
            'geom'           => $vals->[13],
            'cal_pm_numeric' => $vals->[14],

        };
          return;
      }

      method _fetch_and_strip( Str $url) {

        my $doc = $self->get_doc($url);
          if ( $doc->err ) {
            return;
        }
        delete $doc->{'_id'};
          delete $doc->{'_rev'};
          delete $doc->{'row'};
          return $doc;
      }

      method copy_in($fh) {
        $self->clear_store;
          $self->inner_loop_method->( $fh, $self->store );
      }

      method fetch_vds_metadata(  ) {
        $self->clear_vds_info;    # clear the decks first
          my $stmt    = $self->_stmt;
          my $storage = $self->storage();
          my $dbh     = $storage->dbh();

          my $sth = $dbh->prepare($stmt);

          # $sth->execute(@bind);
          $sth->execute();
          while ( my $vals = $sth->fetchrow_arrayref ) {
            $self->_populate_vdsinfo($vals);
        }
        return;

      }

      method sanitize_name( Int $id) {

        if (  !$self->vds_info->{$id}->{'sanitized_name'}
            && $self->vds_info->{$id}->{'name'} )
        {
            my $sanitized_name = $self->vds_info->{$id}->{'name'};

            # change 'n/o' to 'n of', etc
            $sanitized_name =~ s/(n|s|e|w)\/o/$1 of/ixm;

            # change slashes to dashes
            $sanitized_name =~ s/\//-/sxm;

            # strip quotes
            $sanitized_name =~ s/"//sxm;

            # most single quotes are feet references
            $sanitized_name =~ s/' /ft /xm;
            $sanitized_name =~ s/'//sxm;

            # make @ at
            $sanitized_name =~ s/@/ at /xm;

            # make * +
            $sanitized_name =~ s/\*/+/xm;

            # convert to upper case
            $sanitized_name = uc $sanitized_name;

            # # regular use of Mi/MI/M, etc
            # $sanitized_name =~ s/(\dM)I(\s+N|S|E|W)/$1 /xm;

            # make spaces underscores
            $sanitized_name =~ s/\s+/_/gsxm;

            $self->vds_info->{$id}->{'sanitized_name'} = $sanitized_name;
        }
        return $self->vds_info->{$id}->{'sanitized_name'};
      }

      method breakup {
        my $store = $self->store;
        for my $id ( keys %{$store} ) {

            # make path from vds metadata
            # pattern:  district/freeway/direction/name/vdsid_vdstype_year.txt

            my $absname = $self->handles->{$id};
            if(! defined $absname){
                # make it for next time

                my $info           = $self->vds_info->{$id};
                my $sanitized_name = $self->sanitize_name($id);
                if ( !$sanitized_name ) {
                    $sanitized_name = '_name_';
                }
                my $d =
                    $self->district < 10
                    ? 'D0' . $self->district
                    : 'D' . $self->district;
                my $path = $self->output_dir;
                for (  $d, $info->{'freeway_id'}, $info->{'freeway_dir'}, $sanitized_name){
                    if($_){
                        $path .=  q{/} . $_;
                    }
                }
                if ( !-e $path ) {
                    make_path($path);
                }
                my $filename = join q{_}, $id, $info->{'vdstype'}, $self->year;
                $filename .= '.txt';
                $absname = join q{/}, $path, $filename;
                $self->handles->{$id}=$absname;
            }
            #open for appending
            my $fh = IO::File->new( $absname, '>>' );
            if ( defined $fh ) {
                for my $line ( @{ $store->{$id} } ) {
                    my $p_res = print {$fh} $line;
                }
            }else{
                carp "issues with $id";
                carp 'already have handles for: ', scalar keys %{$self->handles};
                croak "could not get filehandle for $absname";
            }
        }
        $self->clear_store;    # aaand the next
    }

}


1;

__END__

=head1 SYNOPSIS

Perl module to help breakup the PeMS daily, by-district, raw station data.

PeMS distributes its raw data on a daily basis.  For CalVAD, it makes better sense to store data on a per-detector basis.  This module can read in the zipped data file from PeMS, will split out each detector's information, and then will write that data to a single file for the detector.

The output files are stashed according to the metadata that is known about the detector.  At a minimum they will be written in a subdirectory named after the detector's district (the district is given in the first two numbers of the detector id).  Then if the metadata can be loaded, the detectors will be further sorted by the freeway/highway name, and then the direction, and then the name of the detector.  It is often the case that several types of detectors will share a single name---for example, on ramps, off ramps, mainline, and HOV detectors at a single location.  If none of these things are known, then the detector data will be written to a subdirectory under the district called "_name_".

So if you *know* that the database connection is good and that you have all ofthe metadata properly loaded, *and* you keep seeing detectors pop up under the directory '_name_', then perhaps you've found a detector without any metadata in the database.  This isn't good, so go fix that...check the database, ask Caltrans, etc etc.

Pretty much you should use the script 'breakup_pems_raw.pl' to run this code, but just for future reference, here are the major methods you might need to know about.

The methods are documented in the order in which they are used in the script breakup_pems_raw.pl.  There are lots of other methods inherited from the two database access roles that this class inherits, but those are ignored if they aren't used directly.


=method new

How to create a new parser object.  The new method takes several parameters for creating the couchdb and postgresql database connections, as well as three that relate to this module.

my $parser = CalVAD::PEMS::Breakup->new(

    # first the sql role
    'host_psql'     => $host,
    'port_psql'     => $port,
    'dbname_psql'   => $dbname,
    'username_psql' => $user,
    'password_psql' => $pass, # be careful to either leave this blank and use
                              # .pgpass, or else use a config file that is
                              # chmod 0600

    # now the couchdb role
    'host_couchdb'     => $cdb_host,
    'port_couchdb'     => $cdb_port,
    'dbname_couchdb'   => $cdb_dbname,
    'username_couchdb' => $cdb_user,
    'password_couchdb' => $cdb_pass,  # use a config file or env vars, I guess
    'create'           => 1,          # unset this if you want to fail on db
                                      # creation

    'output_dir'        => $outdir,  # root directory of results
    'year'              => $year,    # the year you're processing
    'district'          => $district,# the district you're processing
);

Note that year and district really should be defined by the parsing step, but I am too lazy.  These are used to construct the file name.  If there is a mismatch, you are screwed down the road, so don't mess this up.

On the other hand, the way these are used in the breakup_pems_raw.pl program is that they are used to find the input zip files.  Therefore in that case everything is consistent.  You set the district and year which dictates which files are loaded up for processing, and then the parser is also set with exactly the same district and year.

The output_dir is the root of the file tree where output files will be written.  All necessary directories will be created below this point, and existing directories will be reused if necessary.

=method create_db

This method is from the 'CouchDB::Trackable' role.  It will create the couchdb database named in the constructor if it needs to be created.  If it doesn't need to be created, then this is a no-op. (well, it will check but failures that look like "this database already exists" are ignored...other failures will slip through.

If a real failure to create the database crops up (for example from a bad password or username), then the program will die.


=method fetch_vds_metadata

This method will hit the postgresql databse and get the metadata for each detector.  This loads everything needed in order to create the right path in the output directory.  It will not be run automatically, and if the calling program does not run this code, then the detector metadata will be an empty hashref.  This is okay.  That means you can run this code with a fake database connection or without metadata and it will work.


=method track

This method is from the 'CouchDB::Trackable' role. This method will do a number of things.  It is called with an 'id' argument, typically the name of the file being parsed.  If this can't be found in the database, then it will create that document, get ready to parse, and return a "line number" which will be zero.  If it *can* find the document in the database, then it will return either the last line number seen in that document, or else "-1" to indicate that processing was finished successfully the last time this document was touched.

This means you can safely re-process files without worrying about doubling up on observations and processing time.

Of course, you can override this in your program.  In the breakup_pems_raw.pl program, I do:

        my $seekpos = $parser->track( 'id' => $currentfile, );
        if ( !$reparse && $seekpos < 0 ) {
            carp "skipping $currentfile, already done according to parser ";
            next;    # skip this document, go onto the next one
        }
        else {
            carp "process $currentfile";
        }

where $reparse is a command line argument.

The track method can also be used to set various things.  For example:

        # when done parsing a file, set the row count
        $parser->track(
            'id'  => $currentfile,
            'row' => $z->input_line_number(),
        );
        # when you're sure everything was finshed, say so
        $parser->track(
            'id'        => $currentfile,
            'processed' => 1,
        );
        # if something went wrong, you can add arbitrary notes
        # using "otherdata" and a hashref.
        $parser->track(
            'id'        => $currentfile,
            'otherdata' => { 'broken_parse' => $EVAL_ERROR },
        );


=method copy_in

This method takes a file handle as a parameter, then reads in data.  For example:

    my $z = IO::Uncompress::Gunzip->new($file)
         or croak "IO::Uncompress::Gunzip failed: $GunzipError\n";
    $parser->copy_in($z);

The data is stored internally in a hash.  You can access it with the next method documented

Side effect:  When you call this, any existing data in the internal data store will get deleted, so you probably should have already called "breakup"

=method store

This method accesses the data store.  This data store is reset to an empty hash at the beginning of the "copy_in" method.  It is also reset to the empty hash at the end of the "breakup" method.  It is a hash, with the key being the detector id, and the values being a list of all of the rows read from the input file, in order.


=method breakup

This method will write out the contents of the internal store object to the file system, one file per detector.

Side effect:  When this method is finished writing out to the file system, it will delete the contents of the store object.



=head1 SEE ALSO

This uses some other packages that are not on CPAN because they are poorly namespaced and internal to CalVAD.  They are:

=for :list
* L<Testbed::Spatial::VDS::Schema>
* L<CouchDB::Trackable>
* L<DB::Connection>
