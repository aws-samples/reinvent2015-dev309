#!/usr/bin/env ruby

# Rule 1a: Input comes line by line through STDIN. In Ruby, ARGF is one standin
# for STDIN. So, this block is "reading" line by line from Hadoop Streaming.
ARGF.each do |line|
  line = line.chomp

  # Format: host session user useragent date time request status bytes
  split = line.split(" ")

  # This is a heuristic to skip incomplete entries. Your logic is likely going
  # to be more involved. For example, if you are only looking for particular
  # entries in a log that co-locates different log entry types, this would be
  # a detailed section in your actual mapper.
  next if split.size < 10

  # If you wanted even more surety or a lack of a need for order, you could use
  # a key-value log format.
  #
  # Now we are collecting the fields we care about. User agents can span many
  # space-delimited fields, so we use Ruby array properties to choose the right
  # split entries for each field.
  #
  # One case where you might do this significantly differently would be if you
  # have labeled fields. In that case, you'd likely want to strip those here,
  # and move to a convention-based format. In this case, the log format is
  # already done by convention.
  session = split[1]
  user = split[2]
  agent = split.slice(3..-7).join(" ")
  date = split[-6]
  request = split[-4, 2].join(" ")
  status = split[-2]
  
  # Note that this is a 1:1 and in-order mapping to our create table statement.
  # This is absolutely by design. Not only will you likely want to do the same
  # thing, but you'll even want to add "empty" entries for null fields.
  output = [user, session, agent, date, request, status].join("|")

  # Rule 1b: Output is printed line by line to STDOUT. It's also important that
  # the "value" is tab separated from the remainder of the item, the "key".
  # Before the reducer, Hadoop will 
  puts "#{output}\t1"
end
