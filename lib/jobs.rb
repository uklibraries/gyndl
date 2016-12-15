require 'bagit'
require 'filelock'
require 'fileutils'
require 'open3'
require 'time'

def log(message)
  message_ext = "#{Time.now} #{message}"
  puts message_ext
  Filelock 'tmp/lock.txt' do
    File.open('log.txt', 'a') do |f|
      f.puts message_ext
    end
  end
end

class GyndlError < Exception
end

class ValidateProdDip
  @queue = :gyndl

  def self.perform(id)
    node = '/opt/shares/library_dips_1'
    dir = File.join(
      node,
      'pairtree_root',
      id.gsub(/(..)/, '\1/'),
      id
    )

    bag = BagIt::Bag.new dir
    if bag.valid?
      log("ValidateProdDip ok #{id}")
      Resque.enqueue(PushDipProdToTest, id)
    else
      log("ValidateProdDip not ok #{id} invalid")
      raise GyndlError
    end
  end
end

class PushDipProdToTest
  @queue = :gyndl

  def self.perform(id)
    source_node = '/opt/shares/library_dips_1'
    source_dir = File.join(
      source_node,
      'pairtree_root',
      id.gsub(/(..)/, '\1/'),
      id
    )

    target_node = '/opt/shares/library_dips_2/test_dips'
    target_dir = File.join(
      target_node,
      'pairtree_root',
      id.gsub(/(..)/, '\1/'),
      id
    )

    FileUtils.mkdir_p target_dir
    rsync_command = '/usr/bin/rsync'
    rsync_options = '-avPOK'
    if system(rsync_command,
              rsync_options,
              source_dir + '/',
              target_dir)
      log("PushDipProdToTest ok #{id}")
      Resque.enqueue(CompareDipSignatures, id)
    else
      log("PushDipProdToTest not ok #{id}")
      raise GyndlError
    end
  end
end

class CompareDipSignatures
  @queue = :gyndl

  def self.perform(id)
    cmd = 'quick-dip-compare'
    good = false
    Open3.popen2e(cmd, id) do |stdin, stdout_stderr, wait_thr|
      stdout_stderr.each do |line|
        if line =~ /^ok/
          good = true
          log("CompareDipSignatures ok #{id}")
          Resque.enqueue(ValidateTestDip, id)
          break
        end
      end
    end
    unless good
      log("CompareDipSignatures not ok #{id}")
      raise GyndlError
    end
  end
end

class ValidateTestDip
  @queue = :gyndl

  def self.perform(id)
    node = '/opt/shares/library_dips_2/test_dips'
    dir = File.join(
      node,
      'pairtree_root',
      id.gsub(/(..)/, '\1/'),
      id
    )

    bag = BagIt::Bag.new dir
    if bag.valid?
      log("ValidateTestDip ok #{id}")
      Resque.enqueue(ClearJsonFromTest, id)
    else
      log("ValidateTestDip not ok #{id} invalid")
      raise GyndlError
    end
  end
end

class ClearJsonFromTest
  @queue = :gyndl_solr

  def self.perform(id)
    cmd = 'desolr-test'
    good = false
    Open3.popen2e(cmd, id) do |stdin, stdout_stderr, wait_thr|
      stdout_stderr.each do |line|
        if line =~ /"status"=>0/
          good = true
          log("ClearJsonFromTest ok #{id}")
          Resque.enqueue(CopyJsonToTest, id)
          break
        end
      end
    end
    unless good
      log("ClearJsonFromTest not ok #{id}")
      raise GyndlError
    end
  end
end

class CopyJsonToTest
  @queue = :gyndl

  def self.perform(id)
    cmd = 'eris-pull-test-json-from-helios'
    good = false
    Open3.popen2e(cmd, id) do |stdin, stdout_stderr, wait_thr|
      stdout_stderr.each do |line|
        if line =~ /^ok/
          good = true
          log("CopyJsonToTest ok #{id}")
          Resque.enqueue(CheckFormat, id)
          break
        end
      end
    end
    unless good
      log("CopyJsonToTest not ok #{id}")
      raise GyndlError
    end
  end
end

class CheckFormat
  @queue = :gyndl
  
  def self.perform(id)
    cmd = 'dipformat'
    Open3.popen2e(cmd, id) do |stdin, stdout_stderr, wait_thr|
      stdout_stderr.each do |line|
        if line =~ /#{id}:(\w+)/
          format = $1
          case format
          when 'collections'
            log("CheckFormat #{id} - #{format} - jester required")
            Resque.enqueue(RunJesterTest, id)
          else
            log("CheckFormat #{id} - #{format}")
            Resque.enqueue(IndexJsonIntoTest, id)
          end
        end
      end
    end
  end
end

## XXX: Goal: build up jester queue, then run
## Resque.info[:pending] <-- number of jobs
class RunJesterTest
  @queue = :gyndl_solr
  
  def self.perform(id)
    cmd = 'jester-test-queue'
    system(cmd, id)
    log("RunJesterTest ok (queued) #{id}")

    cmd = 'jester-test-count'
    queue_size = 0
    Open3.popen2e(cmd) do |stdin, stdout_stderr, wait_thr|
      stdout_stderr.each do |line|
        queue_size = line.to_i
      end
    end

    pending = Resque.info[:pending].to_i

    wants_batch = false
    if queue_size >= 10 or pending == 0
      wants_batch = true
    else
      q = "/opt/pdp/services/jester/tmp/queue/#{id}"
      if (Time.now - File.stat(q).mtime) > 60
        wants_batch = true
      end
    end

    if wants_batch
      ids = []
      Bundler.with_clean_env do
        cmd = 'jester-test-batch'
        Open3.popen2e(cmd) do |stdin, stdout_stderr, wait_thr|
          stdout_stderr.each do |line|
            if line =~ /ok index ready (\w+)/
              log("RunJesterTest ok (processed) #{id}")
              ids << id
            end
          end
        end
      end
      ids.each do |id|
        Resque.enqueue(IndexJsonIntoTest, id)
      end
    else
      dir = '/opt/pdp/services/jester'
      if File.exist?("#{dir}/success/#{id}") or
         File.exist?("#{dir}/failure/#{id}")
        cmd = 'jester-test-dequeue'
        system(cmd, id)
      else
        sleep 5
        Resque.enqueue(RunJesterTest, id)
      end
    end
  end
end

class IndexJsonIntoTest
  @queue = :gyndl_solr
  
  def self.perform(id)
    cmd = 'eris-index-test'
    good = false
    Open3.popen2e(cmd, id) do |stdin, stdout_stderr, wait_thr|
      stdout_stderr.each do |line|
        print line
        if line =~ /^ok #{id}/
          good = true
          log("IndexJsonIntoTest ok #{id}")
          break
        end
      end
    end
    unless good
      log("IndexJsonIntoTest not ok #{id}")
      raise GyndlError
    end
  end
end
