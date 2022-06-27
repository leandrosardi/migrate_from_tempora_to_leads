# load gem and connect database
require 'blackstack-core'
require 'tiny_tds'
require 'sequel'

connection_descriptor = {
    :adapter => 'tinytds',
    :dataserver => 'localhost', # IP or hostname
    :port => 1433, # Required when using other that 1433 (default)
    :database => 'master', # connect the master to create the central database
    :user => 'sa', #PARSER.value('db_user'), # comment to connect using Windows Authentication
    :password => 'Amazonia2020', #PARSER.value('db_password'), # comment to connect using Windows Authentication
    :timeout => 6,
}  


DB = Sequel.odbc('euler') #connect(connection_descriptor)
puts DB["select db_name() as dbname"].first[:dbname]



