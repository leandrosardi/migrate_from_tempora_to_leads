=begin
s = "Co-Founder & CEO of fayVen ® we book Vendors who need a place to sell at Venues with some space for sale??USAF Veteran?? Action Zone Director of Ecosystem Development"
puts s
puts
puts s.force_encoding("utf-8")
puts
puts s.encode("iso-8859-1").force_encoding("utf-8")
exit(0)
=end

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
    ret[:position] = p[:headline]
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

    # convert to utf-8 compatible string
    ret[:name].to_s.encode!("utf-8", :undef=>:replace, :invalid=>:replace, :replace=>'?')
    ret[:position].to_s.encode!("utf-8", :undef=>:replace, :invalid=>:replace, :replace=>'?')
    ret[:company][:name].to_s.encode!("utf-8", :undef=>:replace, :invalid=>:replace, :replace=>'?')

    # iterate data
    datas = []
    DB["
        SELECT [type], [email]
        FROM [append] WITH (NOLOCK)
        WHERE id_profile='#{pid}'
    "].all { |row|
        datas << {
            :type => row[:type].to_i,
            :value => row[:email],
        }
    }
    ret[:datas] = datas

    # return
    ret
end # def profile_descriptor

DB["
    SELECT TOP 100 id as aid, id_profile as pid 
    FROM [append] WITH (NOLOCK) 
    WHERE type is not null 
    AND isnull(type,20) in (20) 
    AND export_end_time IS NULL
"].all { |row|

    aid = row[:aid]
    pid = row[:pid]
    params = profile_descriptor(pid)
    print "#{row[:pid]} - #{params[:name]}... "

    print '.'
    DB.execute("UPDATE [append] SET export_start_time=GETDATE() WHERE [id]='#{aid}'")

    #puts "cid: "+params[:id_company_from_headline].to_s
    #puts "position: "+params[:position].to_s
    #puts "company: "+params[:company].to_s
    #puts
    #puts params.to_s 
    #puts
    params[:api_key] = 'e5facc62-5ad0-4902-8830-b3c020be03e4'
    begin
        print '.'
        url = 'https://connectionsphere.com/api1.0/leads/merge.json'
        res = BlackStack::Netting::call_post(url, params)
        parsed = JSON.parse(res.body)
        raise parsed['status'] if parsed['status']!='success'
        puts parsed.to_s

        print '.'
        DB.execute("UPDATE [append] SET export_end_time=GETDATE(), export_success=1 WHERE [id]='#{aid}'")
    rescue Errno::ECONNREFUSED => e
        raise "Errno::ECONNREFUSED:" + e.message
        print '.'
        DB.execute("UPDATE [append] SET export_error_description='#{e.message.gsub(/'/, "''")}', export_success=0 WHERE [id]='#{aid}'")
    rescue => e2
        raise "Exception:" + e2.message
        print '.'
        DB.execute("UPDATE [append] SET export_error_description='#{e.message.gsub(/'/, "''")}', export_success=0 WHERE [id]='#{aid}'")
    end
    puts 'done'

    #
    GC.start
    DB.disconnect
}
