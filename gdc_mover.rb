require 'rubygems'
require 'gooddata'
require 'logger'

module GdcMover

  
  def self.gooddata_login(login, password) 
    GoodData.logger = Logger.new(STDOUT)
    gd_server = "https://secure.gooddata.com"
    gd_webdav = "https://secure-di.gooddata.com"
    begin
      GoodData.connect login, password, gd_server, {
        :timeout       => 60,
        :webdav_server => gd_webdav
      }
    rescue RestClient::BadRequest => e
      fail "Login to GoodData Failed"
      exit 1
    end
  end

  class Viewer

    attr_reader :fact, :att, :dataset_json, :dataset, :pid

   def initialize(options)
     GdcMover::gooddata_login(options[:login],options[:password])
     @fact = Hash.new()
     @att = Hash.new()
   end


    def show_all_projects
       json = GoodData.get GoodData.profile.projects
      puts "You have this project available:"
      json["projects"].map do |project|
        pid = project["project"]["links"]["roles"].to_s
        puts "Project name: #{project["project"]["meta"]["title"].bright} Project PID: #{pid.match("[^\/]{32}").to_s.bright}"
      end
    end


    def show_all_datasets(pid)
    GoodData.use pid
    puts "Project has this datasets:"
     GoodData.project.datasets.each do |dataset|
       puts "Dataset name: #{dataset.title.bright} Dataset identifier: #{dataset.identifier.bright}"
     end
    end


    def find_dataset(dataset)
      GoodData.project.datasets.each do |d|
        if d.identifier.to_s == "dataset.#{dataset}"
          return d
        end
      end
      fail "Cannot find dataset #{dataset}"

    end

    def load_dataset_structure(pid,dataset)
      @pid = pid
      GoodData.use @pid
      choosen_dataset = find_dataset(dataset)
      @dataset = GoodData::MdObject.new((GoodData.get choosen_dataset.uri)['dataSet'])

      #Load all atrribute info
      @dataset.content['attributes'].map do |e|
        att_id = e.match("[0-9]*$").to_s
        @att[att_id] = Attribute.new((GoodData.get e)['attribute'])
      end

      #Load all fact info
      @dataset.content['facts'].map do |e|
        fact_id = e.match("[0-9]*$").to_s
        @fact[fact_id] = Fact.new((GoodData.get e)['fact'])
      end
    end


    def print_attributes
      @att.each_value do |att|
        puts att.to_s
      end
    end

		def print_fact
			@fact.each_value do |fact|
				puts fact.to_s
      end
    end
     
    def find_attribute(object)
			@att.each_pair do |key,value|
	  		if value.identifier.split('.').last == object 
	    		return key
	  		end
    	end
			return nil
    end
     
    def find_fact(object)
    	@fact.each do |key,value|
	  		if value.identifier.split('.').last == object 
	      	return key
	  		end
			end
			return nil
    end
    
    def synchronize_datasets(pid, datasets="all", exclude_datasets=[])
   		GoodData.use pid
    	maql = ""
      
    	if datasets.kind_of?(Array) 
    		datasets.each do |dataset|
    			maql = maql + (dataset.instance_of?(String) ? "SYNCHRONIZE {#{find_dataset(dataset).identifier}};" : "SYNCHRONIZE {#{dataset.identifier}};")
    		end
        else 
    		GoodData.project.datasets.each do |dataset|
          maql = maql + "SYNCHRONIZE {#{dataset.identifier}};" unless (dataset.identifier.include?(".dt") or dataset.identifier.include?(".time.") or exclude_datasets.include?(dataset.identifier.gsub("dataset.","")))
    		end
    	end
      
    	GoodData.post("/gdc/md/#{pid}/ldm/manage", { 'manage' => { 'maql' => maql } })
    end

    def move_object(tdataset,identifier)
			object_id = nil
			object = nil
			if (find_attribute(identifier).nil?)
	  		if (find_fact(identifier).nil?)
	    		fail "Cannot find object id (Most likily you provided wrong identifier)"
	  		else 
	    		object_id = find_fact(identifier)
	    		object = @fact[object_id]
	  		end
			else
	    	object_id = find_attribute(identifier)
	    	object = @att[object_id]
			end
  
      object.move(@dataset,
                  GoodData::MdObject.new((GoodData.get find_dataset(tdataset).uri)['dataSet']),
                  @pid)
      
      synchronize_datasets(@pid, [@dataset, GoodData::MdObject.new((GoodData.get find_dataset(tdataset).uri)['dataSet'])])
    end
	
  end


  class Attribute < GoodData::MdObject

    attr_reader :fk, :labels, :main_label


    def type
      "attribute"
    end

    def to_s
      "Attribute identifier: #{identifier.bright} Attribute name: #{title.bright}"
    end

    def refresh
	@json = (GoodData.get uri)['attribute']
     end
    
    def load_fk
      @fk = Hash.new()
       content['fk'].map do |e|
        fk_id = e['data'].match("[0-9]*$").to_s
        @fk[fk_id] = GoodData.get e['data']
       end

      fail "Cannot choose right FK" if @fk.count > 1

    end
    
    
    def load_labels
      @labels = Hash.new()
      content['displayForms'].map do |e|
	if (e['meta']['identifier'].split(".").count == 3)
	  @main_label = GoodData.get e['meta']['uri']
	else
	  label_id = e['meta']['uri'].match("[0-9]*$").to_s
	  @labels[label_id] = GoodData.get e['meta']['uri']
	end
      end
    end

    def fk_identifier
      @fk.values.first['column']['meta']['identifier']
    end

    def move(s_dataset,t_dataset,pid)
      move_maql(s_dataset,t_dataset,pid)
      change_identifier(t_dataset)
      change_label_identifiers(s_dataset,t_dataset)
    end
    
    def move_maql(s_dataset,t_dataset,pid)
      puts "Posting change maql to GoodData"
      load_fk
      s_target_key = "f_#{t_dataset.identifier.split('.').last}.#{fk_identifier.split('.').last}"
      maql = "ALTER ATTRIBUTE {#{identifier}} DROP KEYS {f_#{s_dataset.identifier.split('.').last}.#{fk_identifier.split(".").last}};" \
      "ALTER ATTRIBUTE {#{identifier}} ADD KEYS {#{s_target_key}};" \
      "ALTER DATASET {#{s_dataset.identifier}} DROP {#{identifier}};" \
      "ALTER DATASET {#{t_dataset.identifier}} ADD {#{identifier}};"
      
      GoodData.post("/gdc/md/#{pid}/ldm/manage", { 'manage' => { 'maql' => maql } })
      
    end
    
    def change_identifier(t_dataset)
      refresh
      puts "Updating attribute #{identifier} identifier"
      last = identifier.split('.').last
      meta['identifier'] = "attr.#{t_dataset.identifier.split('.').last}.#{last}" 
      GoodData.post(uri,{ 'attribute' => @json })
    end
    
    def change_label_identifiers(s_dataset,t_dataset)
      puts "Updating labels..."
      load_labels
      fail "Cannot find main label" if @main_label.nil?
      puts "Updating label: #{@main_label['attributeDisplayForm']['meta']['identifier']}"
      # First we need to update main label
      @main_label['attributeDisplayForm']['meta']['identifier'].sub!(s_dataset.identifier.split('.').last,t_dataset.identifier.split('.').last)
      GoodData.post(@main_label['attributeDisplayForm']['meta']['uri'],@main_label)
      @labels.each_value do |l|
	  puts "Updating label: #{l['attributeDisplayForm']['meta']['identifier']}"
	  l['attributeDisplayForm']['meta']['identifier'].sub!(s_dataset.identifier.split('.').last,t_dataset.identifier.split('.').last)
	  GoodData.post(l['attributeDisplayForm']['meta']['uri'],l)
      end
    end 

  end


  class Fact < GoodData::MdObject
    attr_reader :expr

    def type
       "fact"
     end

     def to_s
       "Fact identifier: #{identifier.bright} Fact name: #{title.bright}"
     end
     
     def refresh
	@json = (GoodData.get uri)['fact']
     end

     def load_expr
       @expr = Hash.new()
       content['expr'].map do |e|
         expr_id = e['data'].match("[0-9]*$").to_s
         @expr[expr_id] = GoodData.get e['data']
       end

       fail "Cannot choose right EXPR" if @expr.count > 1

     end

     def expr_identifier
       @expr.values.first['column']['meta']['identifier']
     end

     def move(s_dataset,t_dataset,pid)
       load_expr
       move_to_maql(s_dataset,t_dataset,pid)
       change_identifier(t_dataset)
     end

     def move_to_maql(s_dataset,t_dataset,pid)
       puts "Generating fact maql..."
       s_target_key = "f_#{t_dataset.identifier.split('.').last}.#{expr_identifier.split('.').last}"
       maql = "ALTER FACT {#{identifier}} DROP {f_#{s_dataset.identifier.split('.').last}.#{expr_identifier.split(".").last}};" \
       "ALTER FACT {#{identifier}} ADD {#{s_target_key}};" \
       "ALTER DATASET {#{s_dataset.identifier}} DROP {#{identifier}};" \
       "ALTER DATASET {#{t_dataset.identifier}} ADD {#{identifier}};"
       GoodData.post("/gdc/md/#{pid}/ldm/manage", { 'manage' => { 'maql' => maql } })
     end
     
     def change_identifier(t_dataset)
      refresh
      puts "Updating fact #{identifier} identifier"
      last = identifier.split('.').last
      meta['identifier'] = "fact.#{t_dataset.identifier.split('.').last}.#{last}" 
      GoodData.post(uri,{ 'fact' => @json })
    end

  end

end