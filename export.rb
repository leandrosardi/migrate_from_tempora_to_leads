# load gem and connect database
require 'blackstack_core'
require 'tiny_tds'
require 'sequel'

connection_descriptor = {
    :adapter => 'tinytds',
    :dataserver => '127.0.0.1', # IP or hostname
    :port => 1433, # Required when using other that 1433 (default)
    :database => 'euler', # connect the master to create the central database
    #:user => PARSER.value('db_user'), # comment to connect using Windows Authentication
    #:password => PARSER.value('db_password'), # comment to connect using Windows Authentication
    :timeout => 60,
}  

DB = Sequel.connect(connection_descriptor)