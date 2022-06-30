# load gem and connect database
require 'blackstack-core'
require 'tiny_tds'
require 'sequel'
require_relative './lib'

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
    print "#{row[:pid]} - #{params['name']}... "

    print '.'
    DB.execute("UPDATE [append] SET export_start_time=GETDATE() WHERE [id]='#{aid}'")
    params['api_key'] = 'e5facc62-5ad0-4902-8830-b3c020be03e4'
    #puts "cid: "+params['id_company_from_headline'].to_s
    #puts "position: "+params['position'].to_s
    puts
    puts "name: "+params['name'].to_s+"... "
    puts "company: "+params['company'].to_s+"... "
    puts
    puts params.to_s 
    puts
    begin
        print '.'
        url = 'https://connectionsphere.com/api1.0/leads/merge.json'
        res = BlackStack::Netting::call_post(url, params)
        parsed = JSON.parse(res.body)
        raise parsed['status'] if parsed['status']!='success'
        print '.'
        DB.execute("UPDATE [append] SET export_end_time=GETDATE(), export_success=1 WHERE [id]='#{aid}'")
        puts parsed.to_s
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
