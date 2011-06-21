#!/usr/bin/env ruby
#
# Downloads Friendster friend lists, to investigate the Friendster social network.
#
# This script will contact a central tracker to get an id range (of 10,000 Friendster
# ids at a time). It will then download the friends lists for these users and parse
# them to extract the user ids of the friends. The list of connections for each user
# will be saved to a local file and submitted back to the tracker.
# This process takes between 200 to 300 seconds for each range of 10,000 ids. The
# html pages it downloads are fairly large, but the resulting list of friends is
# much smaller.
#
# To run this script you need the typhoeus gem:
#
#   gem install typhoeus
#
# Then run
#
#   ruby bff-graph-client.rb
#
# If you press any key, the script will complete the current range and exit.
#
#
# Version 1, 21 June 2011.
#

require "rubygems"
require "typhoeus"
require "fileutils"
require "thread"
require "zlib"
require "stringio"
require "net/http"


USER_AGENT = "Googlebot/2.1 (+http://www.googlebot.com/bot.html)"

def profile_id_to_file(profile_id)
  profile_id_s = "%09d" % profile_id.to_i
  "data/#{ profile_id_s[0,3] }/#{ profile_id_s[0,5] }____.txt"
end

def get_friends_list(profile_id)
  friends = []
  result = get_friends_pages(profile_id) do |html|
    html.scan(/friendDetailsTab".+?profiles\.friendster\.com\/([0-9]+)">([^<]+)</m).each do |friend|
      friends << friend[0].to_i
    end
  end
  case result
  when :private
    "#{ profile_id }:private"
  when :not_found
    "#{ profile_id }:notfound"
  when :ok
    "#{ profile_id }:" + friends.sort.uniq.join(",")
  end
end



class BFF
  def initialize
    @hydra = Typhoeus::Hydra.new(:max_concurrency => 100)
    @hydra.disable_memoization
    class << @hydra.instance_variable_get(:@queue)
      def shift
        self.pop
      end
    end

    @results = []
    @results_mutex = Mutex.new
  end

  def get_profile(profile_id)
    get_friends_page(profile_id, 0)
  end

  def run
    @hydra.run
  end

  def save_result(result)
    @results_mutex.synchronize {
      @results << result
    }
  end

  def results
    res = {}
    @results.each do |profile_id, result|
      case result
      when Array
        res[profile_id] ||= []
        res[profile_id] += result
      when :not_found, :private
        res[profile_id] = result
      else
        # $stderr.puts "Error: #{ profile_id }"
        $stderr.print "E"
      end
    end
    res
  end

  def get_friends_page(profile_id, page)
    url = "http://www.friendster.com/friends/#{ profile_id }/#{ page }?r=#{ rand }"
    request = Typhoeus::Request.new(url, :user_agent=>USER_AGENT, :follow_location=>false, :timeout=>60000) #, :headers=>{"Accept-Encoding"=>"gzip"})
    request.on_complete do |response|
      $stderr.print "."
      case response.code
      when 302
        save_result([ profile_id, :private ])
      when 200
        response_str = response.body.to_s
#       if response_str.start_with?("\037\213\b")
#         gz = Zlib::GzipReader.new(StringIO.new(response_str))
#         response_str = gz.read
#         gz.close
#       end

        if response_str=~/<\/html>\s*\Z/m
          answer = process_response(response_str, profile_id, page)
          if answer==:not_found
            save_result([ profile_id, :not_found ])
          else
            get_friends_page(profile_id, page + 1) if answer[:has_next_page]
            save_result([ profile_id, answer[:friends] ])
          end
        else
          $stderr.print "X#{profile_id}"
          get_friends_page(profile_id, page)
          save_result([ profile_id, false ])
        end
      else
        $stderr.print "X#{profile_id}-#{response.code}"
        get_friends_page(profile_id, page)
        save_result([ profile_id, false ])
      end
    end
    queue(request)
  end

  def process_response(response, profile_id, page)
    if response=~/Invalid User ID/
      :not_found
    else
      friends = []
      response.scan(/friendDetailsTab".+?profiles\.friendster\.com\/([0-9]+)">([^<]+)</m).each do |friend|
        friends << friend[0].to_i
      end

      max_page = page
      response.scan(/\/friends\/#{ profile_id }\/([0-9]+)/) do |match|
        last_page = match[0].to_i
        max_page = last_page if last_page > max_page
      end

      { :friends=>friends, :has_next_page=>( max_page > page ) }
    end
  end

  private

  def queue(request)
    @hydra.queue(request)
  end
end


interrupted = false

Thread.new do
  $stdin.getc
  puts
  puts "Exiting..."
  puts
  interrupted = true
end



until interrupted

  puts "Press any key to exit gracefully."

  # ask for an id range
  req = Net::HTTP::Post.new("/request")
  res = Net::HTTP.start("friendster-tracker.heroku.com", 80) { |http| http.request(req) }

  if res.body.to_s.strip.empty?
    puts "Waiting for id range..."
    10.times do
      sleep 1
      exit if interrupted
    end

  else
    id = res.body.strip.to_i

    first_profile_id = id * 10000

    filename = profile_id_to_file(first_profile_id)
    exit if File.exists?(filename)

    profile_id_range = Range.new(first_profile_id, first_profile_id + 9999)
    puts "Downloading #{ profile_id_range }"
    start_time = Time.now

    lines = []

    bff = BFF.new

    profile_id_range.each do |profile_id|
      bff.get_profile(profile_id)
    end

    bff.run

    lines += bff.results.sort_by{|k,v|k}.map do |profile_id, result|
      case result
      when :private
        "#{ profile_id }:private"
      when :not_found
        "#{ profile_id }:notfound"
      when Array
        "#{ profile_id }:" + result.sort.uniq.join(",")
      else
        raise "#{ profile_id } #{ result }"
      end
    end

    end_time = Time.now

    puts
    puts "#{ end_time.to_i - start_time.to_i } seconds"

    result = lines.join("\n")

    FileUtils.mkdir_p(File.dirname(filename))
    File.open(filename, "w") do |f|
      f.puts(result)
    end

    # compress result
    compressed_result = Zlib::Deflate.deflate(result)
    
    # post result
    puts "Submitting results... (#{ compressed_result.size / 1024 } kB / #{ result.size / 1024 } kB uncompressed)"
    req = Net::HTTP::Post.new("/done/#{ id }")
    req.add_field("Content-Type", "application/octet-stream")
    req.body = compressed_result
    res = Net::HTTP.start("friendster-tracker.heroku.com", 80) { |http| http.request(req) }
    raise "HTTP Error: #{ res }" unless res.is_a?(Net::HTTPSuccess)
    
  end

end

