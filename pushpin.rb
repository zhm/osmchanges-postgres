#!/usr/bin/env ruby

require 'rubygems'
require 'bundler/setup'
require 'thor'
require 'yaml'
require 'active_support/all'
require 'aws/s3'

S3_ACCESS_KEY = ""
S3_SECRET     = ""
S3_BUCKET     = ""

include Mongo

class Pushpin < Thor

  desc "stats", "Compute edit stats for @pushpinapp"
  method_option :s3, :aliases => "-s", :desc => "Store stats json on S3", :required => false
  def stats
    compute_stats
    store_stats_on_s3 if options[:s3]
  end

  no_tasks do
    def mongo_client
      @client ||= MongoClient.new("localhost", 27017)
    end

    def mongo_database
      @db ||= mongo_client.db("osm_changesets")
    end

    def changesets_collection
      @changesets ||= mongo_database.collection("changesets")
    end

    def state_collection
      @state_collection ||= mongo_database.collection("state")
    end

    def stats_collection
      @stats_collection ||= mongo_database.collection("pushpin_users")
    end

    def compute_stats
      map = %Q{
        function() {
          emit({ user: this.user}, { edits: 1 });
        }
      }

      reduce = %Q{
        function(key, values) {
          var result = { edits: 0 };
          values.forEach(function(value) {
            result.edits += value.edits;
          });
          return result;
        }
      }

      options = {
        query: {
          "tags.created_by" => /Pushpin/
        },
        out: { :replace => 'pushpin_users' }
      }

      changesets_collection.map_reduce(map, reduce, options)

      json = {
        top_users:    top_users,
        recent_edits: recent_edits,
        total_edits:  total_edits
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

    def top_users
      stats_collection.find.sort([['value.edits', :desc]]).to_a.map do |user|
        { name: user['_id']['user'], edits: user['value']['edits'].to_i }
      end
    end

    def recent_edits
      changesets_collection.find({'tags.created_by' => /Pushpin/}).sort([['created_at', :desc]]).limit(500).map do |edit|
        { name: edit['user'], tags: edit['tags'], date: edit['created_at'], id: edit['id']}
      end
    end

    def total_edits
      changesets_collection.find({'tags.created_by' => /Pushpin/}).count.to_i
    end
  end
end

Pushpin.start
