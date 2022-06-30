# load gem and connect database
require 'blackstack-core'
require 'tiny_tds'
require 'sequel'
require_relative './lib'

API_KEY = 'e5facc62-5ad0-4902-8830-b3c020be03e4'

n = 1
while n > 0
    rows = DB["
        SELECT TOP 1 id as aid, id_profile as pid 
        FROM [append] WITH (NOLOCK) 
        WHERE type is not null 
        AND isnull(type,20) in (20) 
        AND export_end_time IS NULL
    "].all
    n = rows.size

    print "#{n.to_s} batch... "
    leads = []
    rows.each { |row|
        aid = row[:aid]
        pid = row[:pid]
        params = profile_descriptor(pid)
        leads << params
    }

    print '.'
    idsin = "'"+rows.map { |row| row[:aid] }.join("', '")+"'"

    print '.'
    DB.execute("
        UPDATE [append] 
        SET export_start_time=GETDATE() 
        WHERE [id] in (#{idsin})
    ")

    params = {}
    params['api_key'] = API_KEY
    params['leads'] = leads
puts 
puts params.to_s
    begin
        print '.'
        url = 'https://connectionsphere.com/api1.0/leads/merge_many.json'
        res = BlackStack::Netting::call_post(url, params)
        parsed = JSON.parse(res.body)
        raise parsed['status'] if parsed['status']!='success'
        print '.'
        DB.execute("UPDATE [append] SET export_end_time=GETDATE(), export_success=1 WHERE [id] in (#{idsin})")
        puts parsed.to_s
    rescue Errno::ECONNREFUSED => e
        raise "Errno::ECONNREFUSED:" + e.message
        print '.'
        DB.execute("UPDATE [append] SET export_error_description='#{e.message.gsub(/'/, "''")}', export_success=0 WHERE [id] in (#{idsin})")
    rescue => e2
        raise "Exception:" + e2.message
        print '.'
        DB.execute("UPDATE [append] SET export_error_description='#{e.message.gsub(/'/, "''")}', export_success=0 WHERE [id] in (#{idsin})")
    end
    puts 'done'

    #
    GC.start
    DB.disconnect
end # while n > 0

