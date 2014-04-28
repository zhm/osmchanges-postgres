#!/usr/bin/env ruby

require 'rubygems'
require 'bundler/setup'
require 'thor'
require 'nokogiri'
require 'yaml'
require 'zlib'
require 'open-uri'
require 'active_support/all'
require 'pg'
require 'sequel'

Sequel.extension(:pg_hstore)

class OsmChanges < Thor
  desc "setup", "Setup the database"
  method_option :host,     aliases: "-h", desc: "Postgres hostname"
  method_option :database, aliases: "-d", desc: "Database name"
  def setup
    setup_database
  end

  desc "import", "Import changeset file for the first time"
  method_option :file,     aliases: "-f", desc: "Input file", required: true
  method_option :host,     aliases: "-h", desc: "Postgres hostname"
  method_option :database, aliases: "-d", desc: "Database name"
  def import
    parse_changesets(File.open(options[:file])) do |changeset|
      if !changeset_exists(changeset['osm_id'])
        insert_changeset(changeset)
        puts "Creating changeset #{changeset['osm_id']}"
      end
    end
  end

  desc "sync", "Sync changesets from planet.osm.org"
  method_option :sequence, aliases: "-s", desc: "Sequence number to start at", required: false
  method_option :host,     aliases: "-h", desc: "Postgres hostname"
  method_option :database, aliases: "-d", desc: "Database name"
  def sync
    current_state = get_changeset_state

    raise "no sync state found. 'sequence' argument required for first sync" if options[:sequence].nil? and current_state.nil?

    local_state = options[:sequence] || current_state[:sequence]

    server_state = YAML.load(`curl -s http://planet.openstreetmap.org/replication/changesets/state.yaml`)['sequence']

    (local_state.to_i .. server_state.to_i).each do |seq|
      sync_sequence(seq)
    end
  end

  desc "autosync", "Run the sync continuously"
  def autosync
    # this is mostly for testing/debugging
    while true
      puts `ruby osmchanges.rb sync`
      sleep(70)
    end
  end

  no_tasks do
    def database
      @db ||= Sequel.connect(adapter: 'postgres',
                             host: options[:host] || 'localhost',
                             database: options[:database] || 'osmchanges')
    end

    def sync_sequence(seq)
      puts "Processing sequence #{seq}"
      padded = "%09d" % seq.to_i

      source = open("http://planet.openstreetmap.org/replication/changesets/#{padded[0..2]}/#{padded[3..5]}/#{padded[6..8]}.osm.gz")

      parse_changesets(Zlib::GzipReader.new(source)) do |changeset|
        next if changeset['open']

        if !changeset_exists(changeset['osm_id'])
          insert_changeset(changeset)
        end
      end

      state = get_changeset_state || {}
      state[:sequence] = seq.to_i
      save_changeset_state(state)
    end

    def changeset_exists(id)
      database["SELECT COUNT(1) AS count FROM changes WHERE osm_id = #{id}"].all.first[:count] > 0
    end

    def insert_changeset(changeset)
      database[:changes].insert(changeset)
    end

    def get_changeset_state
      database[:state].all.first
    end

    def save_changeset_state(state)
      if state[:id]
        database[:state].update(state)
      else
        database[:state].insert(state)
      end
    end

    def parse_changesets(xml)
      current_record = nil

      Nokogiri::XML::Reader(xml).each_with_index do |node, index|
        if node.name == 'changeset'
          if node.node_type == Nokogiri::XML::Reader::TYPE_ELEMENT
            current_record = node.attributes.merge('tags' => {})
            current_record['osm_id'] = current_record['id'].to_i
            current_record['uid'] = current_record['uid'].to_i
            current_record['num_changes'] = current_record['num_changes'].to_i
            current_record['open'] = current_record['open'] == 'false' ? false : true
            current_record['closed_at'] = Time.parse(current_record['closed_at']) if current_record['closed_at']
            current_record['created_at'] = Time.parse(current_record['created_at']) if current_record['created_at']
            current_record['min_lat'] = current_record['min_lat'].to_f
            current_record['min_lon'] = current_record['min_lon'].to_f
            current_record['max_lat'] = current_record['max_lat'].to_f
            current_record['max_lon'] = current_record['max_lon'].to_f
            current_record['username'] = current_record.delete('user')
            current_record.delete('id')
          end

          if node.node_type == Nokogiri::XML::Reader::TYPE_END_ELEMENT || node.self_closing?
            %w(comment created_by version build).each {|attr| current_record[attr] = current_record['tags'][attr] }
            current_record['tags'] = Sequel.hstore(current_record['tags'] || {})
            yield(current_record)
          end
        end

        if node.name == 'tag'
          current_record['tags'][node.attributes['k'].gsub('.', '-')] = node.attributes['v']
        end
      end
    end

    def setup_database
      database.run 'CREATE EXTENSION "hstore"' rescue nil
      database.run create_tables_statement
      database.run create_triggers_statement
      database.run create_indexes_statement
    end

    def create_indexes_statement
      <<-SQL
        CREATE UNIQUE INDEX index_changes_on_osm_id ON changes USING btree (osm_id);
        CREATE INDEX index_changes_on_username ON changes USING btree (username);
        CREATE INDEX index_changes_on_created_at ON changes USING btree (created_at);
        CREATE INDEX index_changes_on_closed_at ON changes USING btree (closed_at);
        CREATE INDEX index_changes_on_created_by ON changes USING btree (created_by);
        CREATE INDEX index_records_on_comment_index ON changes USING gin (comment_index);
        CREATE INDEX index_records_on_created_by_index ON changes USING gin (created_by_index);
SQL
    end

    def create_triggers_statement
      <<-SQL
        CREATE TRIGGER comment_index_trigger BEFORE INSERT OR UPDATE ON changes
        FOR EACH ROW EXECUTE PROCEDURE tsvector_update_trigger('comment_index', 'pg_catalog.english', 'comment');
        CREATE TRIGGER created_by_index_trigger BEFORE INSERT OR UPDATE ON changes
        FOR EACH ROW EXECUTE PROCEDURE tsvector_update_trigger('created_by_index', 'pg_catalog.english', 'created_by');
SQL
    end

    def create_tables_statement
      <<-SQL
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
          created_by_index tsvector,
          version character varying(255),
          build character varying(255),
          tags hstore,
          CONSTRAINT changes_pkey PRIMARY KEY (id)
        )
        WITH (
          OIDS=FALSE
        );

        CREATE TABLE state
        (
          id serial NOT NULL,
          sequence integer NOT NULL,
          CONSTRAINT state_pkey PRIMARY KEY (id)
        )
        WITH (
          OIDS=FALSE
        );
SQL
    end
  end
end

OsmChanges.start
