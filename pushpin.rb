#!/usr/bin/env ruby

require 'rubygems'
require 'bundler/setup'
require 'thor'
require 'yaml'
require 'active_support/all'
require 'aws/s3'
require 'pg'
require 'sequel'

Sequel.extension(:pg_hstore)

if File.exist?('config.json')
  config = JSON.parse(File.read('config.json'))

  S3_ACCESS_KEY = config['s3_access_key']
  S3_SECRET     = config['s3_secret']
  S3_BUCKET     = config['s3_bucket']
end

class Pushpin < Thor

  desc "setup", "Setup the database"
  method_option :host,     aliases: "-h", desc: "Postgres hostname"
  method_option :database, aliases: "-d", desc: "Database name"
  def setup
    setup_database
  end

  desc "stats", "Compute edit stats for @pushpinapp"
  method_option :s3,       aliases: "-s", desc: "Store stats json on S3", required: false
  method_option :host,     aliases: "-h", desc: "Postgres hostname"
  method_option :database, aliases: "-d", desc: "Database name"
  def stats
    compute_stats
    store_stats_on_s3 if options[:s3]
  end

  no_tasks do
    def database
      @db ||= Sequel.connect(adapter: 'postgres',
                             host: options[:host] || 'localhost',
                             database: options[:database] || 'osmchanges')
    end

    def stats_collection
      @stats_collection ||= mongo_database.collection("pushpin_users")
    end

    def compute_stats
      delete_pushpin_users
      insert_pushpin_users

      json = {
        top_users:    top_users,
        recent_edits: recent_edits,
        total_edits:  total_edits,
        edits_by_day: edits_by_day
      }

      File.open('stats.json', 'w') {|f| f.write(json.to_json)}
    end

    def store_stats_on_s3
      AWS::S3::Base.establish_connection!(access_key_id: S3_ACCESS_KEY, secret_access_key: S3_SECRET)

      default_options = { cache_control: 'no-cache, no-store, max-age=0, must-revalidate',
                          expires:       'Fri, 01 Jan 1990 00:00:00 GMT',
                          pragma:        'no-cache' }

      AWS::S3::S3Object.store(
        'stats.json',
        open('stats.json'),
        S3_BUCKET,
        { :content_type => 'application/json', :access => :public_read }.merge(default_options)
      )
    end

    def delete_pushpin_users
      database.run "DELETE FROM pushpin_users;"
    end

    def insert_pushpin_users
      database.run <<-SQL
      INSERT INTO pushpin_users (username, edits)
      SELECT username, COUNT(1) FROM changes WHERE created_by_index @@ to_tsquery('Pushpin')
      GROUP BY username;
SQL
    end

    def top_users
      database["SELECT * FROM pushpin_users ORDER BY edits DESC"].all.to_a.map do |user|
        { name: user[:username], edits: user[:edits].to_i }
      end
    end

    def recent_edits
      database["SELECT * FROM changes WHERE created_by_index @@ to_tsquery('Pushpin') ORDER BY created_at DESC LIMIT 500"].all.to_a.map do |edit|
	created_at = Time.parse(edit[:created_at].strftime('%Y-%m-%dT%H:%M:%SZ')).utc
        { name: edit[:username], tags: Sequel.hstore(edit[:tags]).to_hash, date: created_at, id: edit[:osm_id] }
      end
    end

    def total_edits
      database["SELECT COUNT(1) AS count FROM changes WHERE created_by_index @@ to_tsquery('Pushpin')"].all.first[:count]
    end

    def edits_by_day
      database["SELECT date_trunc('day', created_at) AS day, COUNT(1) as count FROM changes WHERE created_by_index @@ to_tsquery('Pushpin') GROUP BY date_trunc('day', created_at) ORDER BY date_trunc('day', created_at);"].all.map {|e| {day: e[:day].strftime('%F'), count: e[:count]}}
    end

    def setup_database
      database.run create_tables_statement
    end

    def create_tables_statement
      <<-SQL
        CREATE TABLE pushpin_users
        (
          id serial NOT NULL,
          username character varying(255),
          edits integer,
          CONSTRAINT pushpin_users_pkey PRIMARY KEY (id)
        )
        WITH (
          OIDS=FALSE
        );
SQL
    end
  end
end

Pushpin.start
