#!/usr/bin/env ruby

curr_key = nil
curr_count = nil

# Pulled this out into a function for readability. Simply compare, and print/reset
# if different.
def reduce_item(curr_key, curr_count, key, value)
  if curr_key != key && curr_count > 0
    # Rule 1b: Output goes to STDOUT, line by line.
    puts "#{curr_key}\t#{curr_count.to_s}"
    return [key, value]
  else
    curr_count += value
    return [curr_key, curr_count]
  end
end

# Remember Rule 1a: Input comes from STDIN. And that ARGV is a stand-in for
# STDIN here.
ARGF.each do |line|
  line = line.chomp
  key, value = line.split(/\t/)

  # Redshift ingestion will expect tab-delimited, format now.
  key = key.split("|").join("\t")

  # Special case first value.
  if curr_key == nil
    curr_key = key
    curr_count = value.to_i
    next
  end

  # There's a trick to this, and that's the fact that Hadoop is piping us
  # already sorted line items. So, if we see two identical keys in a row, we can
  # combine them. We can also expect that we will not need to concern ourselves
  # with non-consecutive but identical keys. No gigantic hash tables needed.
  curr_key, curr_count = reduce_item(curr_key, curr_count, key, value.to_i)
end

# Make sure we print the final values.
puts "#{curr_key}\t#{curr_count.to_s}" unless curr_count < 1
