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


def profile_descriptor(pid)
    ret = {}

    p = DB["SELECT name, headline FROM [profile] WHERE [id]='#{pid}'"].first
    ret[:name] = p[:name]
    ret[:headline] = p[:headline]

    DB["
        SELECT name, 
        FROM [append] 
        WHERE export_end_time IS NULL
    "].all { |row|
        :name => 'Leandro Sardi',
        :position => 'Founder and CEO',
        :company => {
            :name => "ConnectionSphere",
            :url => "https://connectionsphere.com",
        },
        :industry => "Internet",
        :location => "Argentina",
        :datas => [
            {
                :type => 10,
                :value => "+54 9 11 5555-5555",
            },
            {
                :type => 20,
                :value => "support@expandedventure.com",
            },
        ],
end # def profile_descriptor

DB["SELECT TOP 1 id, id_profile FROM [append] WITH (NOLOCK) WHERE export_end_time IS NULL"].all { |row|
    print "#{row[:id]}... "
    
    puts 'done'
}
