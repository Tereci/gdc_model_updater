# GDC Model Updater
Simple wrapper for maql model modifications.

## Prerequisitions
rubygems, gooddata and logger ruby gems installed

## Usage
* Download the two ruby files into a directory.   
* Add maql files into the directory.  
* Open update_mode.rb  
* Add updater.execute_maql, updater.move and updater.synchronize calls as desired.  
* Run the update_model.rb from command line with arguments as:  
* * 	./update_model.rb login@gooddata.com password pid_of_the_project [log_file]
* Log file parameter is not required, but if you provide it all performed steps will be logged and in case that the updater failes on timeout (e.g. while performing cascade drops) you can afterwards restart it and it will pick up where it stopped.  

NOTE: the script will output a lot of info on console while running. If it fails see the log to figure out why.   
