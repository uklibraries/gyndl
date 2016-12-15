#!/usr/bin/env ruby

require 'resque'
require 'jobs'

file = ARGV[0]
File.readlines(file).each do |line|
  size, id = line.split /\s+/
  puts id
  Resque.enqueue(ValidateProdDip, id)
end
