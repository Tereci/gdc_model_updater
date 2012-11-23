#!/usr/bin/env ruby

require 'rubygems'
require 'gdc_mover'
require 'gooddata'

class Updater

  def initialize(login, password, done, logger)
    @viewer = GdcMover::Viewer.new(:login => login, :password => password)
    @done = done
    @logger = logger
  end
  
  def use_pid(pid)
  	@pid = pid
  end
  
  def move(sdataset, tdataset, object, pid=@pid)
  	fail "pid must be specified" if pid.nil?
    key = "#{pid}\t#{sdataset}\t#{tdataset}\t#{object}"
    return if @done[key]
    @viewer.load_dataset_structure(pid, sdataset)
    @viewer.move_object(tdataset, object)
    @logger.puts key
  end
  
  def execute_maql(filename, pid=@pid)
  	fail "pid must be specified" if pid.nil?
    key = "#{pid}\t#{filename}"
    return if @done[key]
    File.open(filename, "r") do |file|
      maql = file.read
      GoodData.post("/gdc/md/#{pid}/ldm/manage", { 'manage' => { 'maql' => maql } })
    end
    @logger.puts key
  end
  
  def synchronize_datasets(datasets="all", pid=@pid)
  	fail "pid must be specified" if pid.nil?
  	key = "#{pid}\tsync_dt\t#{datasets.to_s}"
    return if @done[key]
    @viewer.synchronize_datasets(pid, datasets)
    @logger.puts key
  end
  
end 

login = ARGV[0]
password = ARGV[1]
pid = ARGV[2]
previous_log = ARGV[3]

raise "Usage: #{$0} login password pid [previous_log]" unless (login and password and pid)

done = {}
log  = (Class.new { def puts(a); end; def close; end }).new # do nothing on puts or close
if previous_log then
  File.foreach(previous_log) { |i| done[i.chomp] = true } rescue puts("Cannot read #{previous_log}")
  log = File.open(previous_log, 'a')
end

begin
  # login to GD
  updater = Updater.new(login, password, done, log)
  updater.use_pid(pid)
	
  ################################################
  # EDIT ONLY FOLLOWING PART OF THE CODE         #
  ################################################
  
  # example of execute_maql
	# updater.execute_maql(pid, "drop_references.maql")
  
  # examples of synchronize datasets
	# updater.synchronize_datasets() // sync all datasets
  # updater.synchronize_datasets(["opportunity, account"]) // sync only given datasets
  
  # example of move(source_dataset,target_dataset,object) for both attribute or fact. Attribute will be moved together with its labels.
  # updater.move("opportunity","account","staff")

ensure
  log.close
end
