# Breakup PeMS raw detector data

PeMS distributes its raw data on a daily basis.  For CalVAD, it makes better sense to store data on a per-detector basis.  This module can read in the zipped data file from PeMS, will split out each detector's information, and then will write that data to a single file for the detector.

The output files are stashed according to the metadata that is known about the detector.  At a minimum they will be written in a subdirectory named after the detector's district (the district is given in the first two numbers of the detector id).  Then if the metadata can be loaded, the detectors will be further sorted by the freeway/highway name, and then the direction, and then the name of the detector.  It is often the case that several types of detectors will share a single name---for example, on ramps, off ramps, mainline, and HOV detectors at a single location.  If none of these things are known, then the detector data will be written to a subdirectory under the district called "_name_".

One important caveat: the metadata in the database must be current
relative to the raw data you are going to process.  If you have data
from 2019 and metadata from 2017 in the database, then some detectors
will be fine, but any new detectors will not be picked up properly.
So, **BEFORE** running the code in this repo, hop over to the
`CalVAD-PEMS-StationsParse` repository, download the recent metadata
from PeMS, and use *that* repo to update the database.

So if you *know* that the database connection is good and that you have all ofthe metadata properly loaded, *and* you keep seeing detectors pop up under the directory '_name_', then perhaps you've found a detector without any metadata in the database.  This isn't good, so go fix that...check the database, ask Caltrans, etc etc.

Pretty much you should use the script 'breakup_pems_raw.pl' to run this code, but just for future reference, here are the major methods you might need to know about.

The methods are documented in the order in which they are used in the script breakup_pems_raw.pl.  There are lots of other methods inherited from the two database access roles that this class inherits, but those are ignored if they aren't used directly.


# Installation

To install, use Dist::Zilla.

## prereqs

First install Dist::Zilla using cpan or cpanm

```
cpanm --sudo Dist::Zilla
```

Next install the Dist::Zilla plugins needed.

```
dzil authordeps --missing | cpanm --sudo
```

Next install the package dependencies, which are probably the
Spreadsheet parsing modules.

```
dzil listdeps --missing | cpanm --sudo
```

## Moops, Kavorka, and Devel::CallParser

As of this writing (November 2017), `Devel::CallParser` has a bug that
causes Kavorka and Moops to fail installation.  The problem is known,
but the maintainer of Devel::CallParser is MIA.

The fix is as follows.

### Download Devel::CallParser

Get Devel::CallParser from
https://cpan.metacpan.org/authors/id/Z/ZE/ZEFRAM/Devel-CallParser-0.002.tar.gz

Download the most recent patch from this bug thread:
https://rt.cpan.org/Public/Bug/Display.html?id=110623

Alternately, just use the copy included in this repository.

Unzip the Devel-CallParser file

```
tar xvf Devel-CallParser-0.002.tar.gz
```

Change into the directory and apply the patch

```
cd Devel-CallParser-0.002
patch -p 1 < ../0002-Fix-a-pad-problem-with-Perl-5.24.1-on-unthreaded-build.patch
```

(Note that the patch command needs the patch.  I put it in the
directory above the Devel-CallParser code, but wherever it is, you
need to put in the correct path to the patch.)

The patch should apply cleanly.  If it doesn't check the bug thread
linked above.  Then make and install the code.

```
perl Build.PL
./Build
./Build test

... Result: PASS

sudo ./Build install
```

After that patched version of Devel-CallParser is installed, Moops
(and Kavorka) should install cleanly

```
cpanm --sudo Moops Kavorka
```

And with that, all of the dependencies required for this package
should be good to go (assuming you also manually installed the package
`spatialvds_schema` (from https://github.com/jmarca/spatialvds_schema)

```
dzil listdeps --missing | cpanm --sudo
```


## Testing

Configuration of the tests is done using the file `test.config.json.
This file controls options to access databases.  An example is:

```javascript
{
    "couchdb": {
        "host": "127.0.0.1",
        "port":5984,
        "breakup_pems_raw_db": "test_calvad_pems_brokenup",
        "auth":{"username":"james",
                "password":"this is my couchdb passwrod"
               }
    },
    "postgresql":{
        "host":"192.168.0.1",
        "port":5432,
        "username":"james",
        "password":"my secret stapler horse",
        "breakup_pems_raw_db":"spatialvds"
    }
}
```

To run the tests, you can also use dzil

```
dzil test
```

If the tests don't pass, read the failing messages, and maybe try to
run each test individually using prove, like so:

```
prove -l t/04_couch_tracking.t
```

(The -l flag on prove will add the packages under the 'lib' directory
to the Perl path.)

## Install

Once the prerequisites are installed and the tests pass, you can
install.  This will again run the tests.

Two ways to do this.  First is to use sudo -E

```
sudo -E dzil install
```

The second is to use cpanm as the install command.

```
dzil install --install-command "cpanm --sudo ."
```

I prefer the second way.  You have to be sudo to install the module
in the global perl library, but there is no need to be sudo to run the
tests.  This second way uses the "sudo" flag for cpanm only when
installing, not for testing.

# Running the script to breakup and transpose the data

To actually run the program, do

```
perl -w breakup_pems_raw.pl
```

If you just run this, it will dump out a hopefully helpful
documentation of the command line options.

You can configure either with command line switches, or with a file
named `config.json`, or both.

The `config.json` can be like the test config file, except you can
also set command line switches too, like the year and the district.

Don't forget to set the config.json files to be mode 0600.  Not sure
what that means in windows, so you should probably run this on mac or
Linux.  Don't worry if you don't set that mode...the program will
crash and remind you in a Pavlovian punishment scheme.
