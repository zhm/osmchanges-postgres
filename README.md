# OpenStreetMap Changeset Sync for PostgreSQL

This script imports `changesets-latest.osm` [file](http://planet.osm.org/planet/) into a Postgres database and also provides a way to keep the database up-to-date with the [minute diffs](http://planet.osm.org/replication/changesets/).

I wrote this little script to power the [pushpinapp stats](http://pushpinosm.org/stats/) and others might find it useful.

The changeset data is stored in the database in the following structure:

```sql
CREATE TABLE changes
(
  id serial NOT NULL,
  osm_id integer NOT NULL,
  uid integer NOT NULL,
  username character varying(255),
  num_changes integer,
  open boolean,
  closed_at timestamp without time zone,
  created_at timestamp without time zone,
  min_lat numeric(30,20),
  min_lon numeric(30,20),
  max_lat numeric(30,20),
  max_lon numeric(30,20),
  comment text,
  comment_index tsvector,
  created_by text,
  version character varying(255),
  build character varying(255),
  tags hstore,
  CONSTRAINT changes_pkey PRIMARY KEY (id)
)
```

# Requirements

  * PostgreSQL (tested with 9.x)
  * Ruby 1.9
  * Bundler `gem install bundler`
  * Download the `changesets-latest.osm.bz2` file from [here](http://planet.osm.org/planet/) and extract it somewhere for the first time import.

# Usage

First you have to create a new postgres database (this script does not create databases)

    $ createdb osmchanges

Then use bundler to install the gems and start the import

    $ bundle --path .bundle
    $ ./osmchanges.rb setup -d <databasename>
    $ ./osmchanges.rb import -d <databasename> -f /path/to/changesets-latest.osm  # this will take a while

  The import process uses bulk inserts to speed up processing. For maximum performance, the import process doesn't perform checks for existing records. Be sure to start with empty tables otherwise duplicate entries might occur. Package processing can be controlled by specifying a package size via parameter '-s'. As a default setting a package size of 5000 changesets will be assumed. This should be a reasonable default for most situations.

  Now you will need to find a sequence number that will work for the time you did the import. A safe bet is to look at the date on the changesets-latest.osm file you downloaded and go back half a day or so. Start [here](http://planet.osm.org/replication/changesets/000/) and drill all the way down to a specific file. You don't need to download the file, you just need to get the number. You will need the full sequence number. e.g. The sequence number for http://planet.osm.org/replication/changesets/000/020/999.osm.gz is 000020999. You will use this sequence number for the next command so the importer knows how to "catch up" and bootstrap the diff process. I would like to improve this, but since you only need to do this once, it's not a huge deal for now.

    $ ./osmchanges.rb sync -s 000020999  # use your sequence number

  Once this completes, your database will be up-to-date and ready to work with the minute diffs. The last state is now stored in the database and you only need to run one command at any time to sync the database with the diffs. To keep the database always up-to-date, just add the following command to a cronjob.

    $ ./osmchanges.rb sync

  Here is the crontab line I use

    */5 * * * * cd /apps/osmchanges/ && /usr/local/lib/ry/current/bin/ruby osmchanges.rb sync

  If you have trouble getting it running from a cronjob, it's most likely due to a ruby version or gem issue. Feel free to submit an issue.

# Making use of the data

There's a script in the repo that uses the changeset data to compute some basic edit statistics for Pushpin iOS. You can see how to query the data and make use of it to do something much cooler with your own script :)

You can run the pushpin stats script yourself to output a stats.json file:

    $ ./pushpin.rb stats
