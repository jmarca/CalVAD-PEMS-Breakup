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


    sub _build__connection_psql {
        my $self = shift;
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
    has 'vds_info' => ( 'is' => 'ro', 'isa' => 'HashRef', 'lazy_build' => 1 );
    method _build_store {
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
   regexp_replace(v.cal_pm,E'[^[:digit:]^\\.]','','g')::numeric as cal_pm_numeric
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
            my $info           = $self->vds_info->{$id};
            my $sanitized_name = $self->sanitize_name($id);
            if ( !$sanitized_name ) {
                $sanitized_name = '_name_';
            }
            my $d =
              $self->district < 10
              ? 'D0' . $self->district
              : 'D' . $self->district;
            my $path = join q{/}, $self->output_dir, $d,
              $info->{'freeway_id'}, $info->{'freeway_dir'},
              $sanitized_name;
            if ( !-e $path ) {
                make_path($path);
            }
            my $filename = join q{_}, $id, $info->{'vdstype'}, $self->year;
            $filename .= '.txt';
            my $absname = join q{/}, $path, $filename;

            #open for appending
            my $fh = IO::File->new( $absname, '>>' );
            if ( defined $fh ) {
                for my $line ( @{ $store->{$id} } ) {
                    my $p_res = print {$fh} $line;
                }
            }
        }
        $self->clear_store;    # aaand the next
    }

}


1;

__END__
