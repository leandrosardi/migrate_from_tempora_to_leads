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

    # get profile fields
    p = DB["SELECT name, headline, location, industry, id_company_from_headline FROM [profile] WHERE [id]='#{pid}'"].first

    # extract the company name from the headline
    cname = nil
    if p[:headline] =~ / at /
        cname = p[:headline].split(/ at /).last
        p[:headline] = p[:headline].split(/ at /).first
    end

    # add profile fields to the descriptor
    ret[:name] = p[:name]
    ret[:headline] = p[:headline]
    ret[:location] = p[:location]
    ret[:industry] = p[:industry]

    # if the profile is not linked to a company record
    ret[:company] = nil
    ret[:id_company_from_headline] = nil
    if !ret[:id_company_from_headline].nil?
        cid = ret[:id_company_from_headline]
        ret[:id_company_from_headline] = cid
        c = DB["SELECT name, website FROM [company] WHERE [id]='#{cid}'"].first
        ret[:company] = {
            :name => c[:name],
            :url => c[:website],
        }
    elsif !cname.nil?
        ret[:company] = {
            :name => cname,
            :url => nil,
        }
    end 

    # iterate data
    datas = []
    DB["
        SELECT [type], [email]
        FROM [append] WITH (NOLOCK)
        WHERE id_profile='#{pid}'
    "].all { |row|
        datas << {
            :type => row[:type],
            :value => row[:email],
        }
    }
    ret[:datas] = datas

    # return
    ret
end # def profile_descriptor

DB["SELECT TOP 100 id as aid, id_profile as pid FROM [append] WITH (NOLOCK) WHERE isnull(type,20) in (10,20) and export_end_time IS NULL"].all { |row|
    aid = row[:aid]
    pid = row[:pid]

    print "#{row[:pid]}... "
    h = profile_descriptor(pid)
    puts 'done'

    puts h[:id_company_from_headline].to_s
    puts h[:company].to_s
    puts 

    #
    GC.start
    DB.disconnect
}
