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
# Version 3, 23 June 2011. Added trickery to run two ranges at te same time
#                          to keep the request pipe filled.
# Version 2, 21 June 2011. Made 'max_concurrency' a constant.
# Version 1, 21 June 2011.
#


#
# This is the number of concurrent requests. Set this to something smaller
# if you want the script to go slower.
#
MAX_CONCURRENCY = (ARGV[0] || "100").to_i



require "rubygems"
require "typhoeus"
require "fileutils"
require "thread"
require "zlib"
require "stringio"
require "net/http"


USER_AGENT = "Googlebot/2.1 (+http://www.googlebot.com/bot.html)"


class RequestQueue
  def initialize
    @queues = { :low=>[], :normal=>[], :high=>[] }
  end

  def clear
    @queues.each do |prio, queue|
      queue.clear
    end
  end

  def each
    @queues.each do |prio, queue|
      queues.each(&block)
    end
  end

  def <<(req)
    prio = (req.respond_to?(:priority) ? req.priority : :normal)
    queue = @queues[prio] || @queues[:normal]
    queue << req
    self
  end

  def shift
    @queues[:high].shift || @queues[:normal].shift || @queues[:low].shift
  end

  def pop
    @queues[:high].pop || @queues[:normal].pop || @queues[:low].pop
  end

  def empty?
    @queues.all? do |prio, queue|
      queue.empty?
    end
  end
end



$hydra = Typhoeus::Hydra.new(:max_concurrency => MAX_CONCURRENCY)
$hydra.disable_memoization
class << $hydra.instance_variable_get(:@queued_requests)
  def <<(req)
    if req.respond_to?(:priority) && req.priority == :high
      unshift(req)
    else
      push(req)
    end
  end
end
$hydra.instance_variable_set(:@queued_requests, RequestQueue.new)

class Typhoeus::Hydra
  def running_requests
    @running_requests
  end
end
class Typhoeus::Request
  attr_accessor :priority
end



class BFF
  attr_reader :profile_id_range

  def initialize(profile_id_range)
    @profile_id_range = profile_id_range
    @results = []
    @running_results = 0
    @results_mutex = Mutex.new

    @start_time = Time.now

    profile_id_range.each do |profile_id|
      get_profile(profile_id)
    end
  end

  def done?
    @results_mutex.synchronize {
      not @results.empty? and @running_results==0
    }
  end
  def running_results
    @results_mutex.synchronize {
      @running_results
    }
  end
  def done
    @results_mutex.synchronize {
      @results.size
    }
  end

  def process_and_submit_results
    number_of_edges = 0

    lines = []
    lines += results.sort_by{|k,v|k}.map do |profile_id, result|
      case result
      when :private
        "#{ profile_id }:private"
      when :not_found
        "#{ profile_id }:notfound"
      when Array
        friends = result.sort.uniq
        number_of_edges += friends.length
        "#{ profile_id }:" + friends.join(",")
      else
        raise "#{ profile_id } #{ result }"
      end
    end

    end_time = Time.now

    puts "#{ @profile_id_range }: #{ end_time.to_i - @start_time.to_i } seconds || Found #{ number_of_edges } friends!"

    result = lines.join("\n")

    FileUtils.mkdir_p(File.dirname(filename))
    File.open(filename, "w") do |f|
      f.puts(result)
    end

    # compress result
    compressed_result = Zlib::Deflate.deflate(result)
    
    # post result
    puts "#{ @profile_id_range }: Submitting results... (#{ compressed_result.size / 1024 } kB / #{ result.size / 1024 } kB uncompressed)"
    req = Net::HTTP::Post.new("/done/#{ @profile_id_range.first }")
    req.add_field("Content-Type", "application/octet-stream")
    req.body = compressed_result
    res = Net::HTTP.start("friendster-tracker.heroku.com", 80) { |http| http.request(req) }
    raise "HTTP Error: #{ res }" unless res.is_a?(Net::HTTPSuccess)
  rescue
    puts $!
    raise $!
  end

  private

  def filename
    profile_id_s = "%09d" % @profile_id_range.first
    "data/#{ profile_id_s[0,3] }/#{ profile_id_s[0,5] }____.txt"
  end

  def get_profile(profile_id)
    get_friends_page(profile_id, 0)
  end

  def queue(request)
    @results_mutex.synchronize {
      @running_results += 1
    }
    $hydra.queue(request)
  end

  def save_result(result)
    @results_mutex.synchronize {
      @running_results -= 1
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
        # $stderr.print "E"
      end
    end
    res
  end

  def get_friends_page(profile_id, page, priority=:low)
    url = "http://www.friendster.com/friends/#{ profile_id }/#{ page }?r=#{ rand }"
    request = Typhoeus::Request.new(url, :user_agent=>USER_AGENT, :follow_location=>false, :timeout=>60000)
    request.priority = priority
    request.on_complete do |response|
      # $stderr.print "."
      case response.code
      when 302
        save_result([ profile_id, :private ])
      when 200
        response_str = response.body.to_s

        if response_str=~/<\/html>\s*\Z/m
          answer = process_response(response_str, profile_id, page)
          if answer==:not_found
            save_result([ profile_id, :not_found ])
          else
            get_friends_page(profile_id, page + 1, :normal) if answer[:has_next_page]
            save_result([ profile_id, answer[:friends] ])
          end
        else
          # $stderr.print "X#{profile_id}"
          get_friends_page(profile_id, page, :high)
          save_result([ profile_id, false ])
        end
      else
        # $stderr.print "X#{profile_id}-#{response.code}"
        get_friends_page(profile_id, page, :high)
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
end


$interrupted = false

Thread.new do
  $stdin.getc
  puts "Exiting..."
  $interrupted = true
end

Thread.new do
  begin
  $bff_queue = []

  def print_status
    if $interrupted
      print "Exiting || "
    else
      print "Running || "
    end
    puts($bff_queue.map do |bff|
      todo = bff.running_results
      done = bff.done
      "#{bff.profile_id_range}: %6d open, %6d done" % [ bff.running_results, bff.done ]
    end.join(" | "))
  end

  until $interrupted

    until $bff_queue.size < 2
      first_bff = $bff_queue.first
      until first_bff.done?
        print_status
        sleep 5
      end
      $bff_queue.shift
      first_bff.process_and_submit_results
    end

    while not $interrupted and $hydra.running_requests >= MAX_CONCURRENCY * 0.9
      print_status
      sleep 5
    end

    break if $interrupted

    puts "#  Press any key to exit gracefully."

    # ask for an id range
    req = Net::HTTP::Post.new("/request")
    res = Net::HTTP.start("friendster-tracker.heroku.com", 80) { |http| http.request(req) }

    if res.body.to_s.strip.empty?
      puts "Waiting for id range..."
      10.times do
        sleep 1
        break if $interrupted
      end
    else
      first_profile_id = res.body.strip.to_i * 10000

      profile_id_range = Range.new(first_profile_id, first_profile_id + 9999)
      puts "Starting download of #{ profile_id_range }"
      $bff_queue << BFF.new(profile_id_range)
    end

  end

  until $bff_queue.empty?
    first_bff = $bff_queue.first
    until first_bff.done?
      print_status
      sleep 5
    end
    $bff_queue.shift
    first_bff.process_and_submit_results
  end

  rescue
  p $!
  end
end

until $interrupted
  $hydra.run
  sleep 1
end

