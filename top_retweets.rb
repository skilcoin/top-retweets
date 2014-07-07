# Run by calling "ruby top_retweets.rb <n>" where n is the rolling window
# in which to aggregate retweet texts
# Tested with ruby 2.1.1

require 'twitter'
require 'yaml'

# Non-60 values for debugging
BUCKET_SECONDS = 60

DEBUG_STREAM = false
DEBUG_PRINTING = false
n = ARGV[0].to_i
puts "Printing top 10 tweets for last #{n} minutes every minute"
currentMin = (Time.now.to_f * 1000).to_i / (BUCKET_SECONDS * 1000)
lastMin = currentMin
tweetStore = Hash.new

tweetStore[currentMin] = Hash.new
properties = YAML.load_file("properties")
client = Twitter::Streaming::Client.new do |config|
	config.consumer_key        = properties["consumer_key"]
	config.consumer_secret     = properties["consumer_secret"]
	config.access_token        = properties["access_token"]
	config.access_token_secret = properties["access_token_secret"]
end

def print_top_10 currentMin, tweetStore, n
	totalCounts = Hash.new
	puts "-------------- Retweets from minute #{currentMin - n} - #{currentMin} -------------"
	tweetStore.each do |minute, tweetCounts|
		if minute >= (currentMin - n)
			tweetCounts.each do |text, count|
				puts "#{minute} #{count} #{text}" if DEBUG_PRINTING
				totalCounts[text] = 0 if !totalCounts.has_key? text
				totalCounts[text] += count
			end
		end
	end
	statusesPrinted = 0
	totalCounts.sort_by {|k,v| v}.reverse.each do |text, count|
		break if statusesPrinted >= 10
		puts "#{count} #{text}"
		statusesPrinted += 1
	end
end

client.sample do |object|
  if object.is_a?(Twitter::Tweet)
	currentMin = (Time.now.to_f * 1000).to_i / (BUCKET_SECONDS * 1000)
	print_top_10(currentMin, tweetStore, n) if lastMin != currentMin
	lastMin = currentMin
  	if /^RT/.match object.text
		retweetText = object.text
		retweetText = retweetText.split(/^RT @[a-zA-Z_0-9]+:/)[1] if /^RT @[a-zA-Z_0-9]+:/.match object.text
		retweetText = retweetText.split(/^RT /)[1] if /^RT http/.match retweetText
		tweetStore[currentMin] = Hash.new if !tweetStore.has_key? currentMin
		tweetStore[currentMin][retweetText] = 0 if !tweetStore[currentMin].has_key? retweetText
		tweetStore[currentMin][retweetText] += 1
		puts "#{tweetStore[currentMin][retweetText]} Retweet #{retweetText}" if DEBUG_STREAM
	end
	object.text
  end
end
