# load gem and connect database
require 'blackstack-core'
require 'tiny_tds'
require 'sequel'
require_relative './lib'

API_KEY = 'e5facc62-5ad0-4902-8830-b3c020be03e4'

n = 1
while n > 0
    rows = DB["
        SELECT TOP 100 id as aid, id_profile as pid 
        FROM [append] WITH (NOLOCK) 
        WHERE type is not null 
        AND isnull(type,20) in (20) 
        AND export_start_time IS NULL -- choose records who have never tried before
    "].all
    n = rows.size

    print "#{n.to_s} batch... "

    print '.'
    idsin = "'"+rows.map { |row| row[:aid] }.join("', '")+"'"

    print '.'
    leads = []
    rows.each { |row|
        aid = row[:aid]
        pid = row[:pid]
        params = profile_descriptor(pid)
#puts
#puts
#puts params.to_s
        # if the lead has a valid email address
        if params['datas'].select { |d| d['type'].to_i == 20 }.size > 0
            leads << params
#puts 'yes'
        else
#puts 'no'
        end
    }
    print "(#{leads.size} effective leads)"
#exit(0)
    print '.'
    DB.execute("
        UPDATE [append] 
        SET export_start_time=GETDATE() 
        WHERE [id] in (#{idsin})
    ")

    params = {}
    params['api_key'] = API_KEY
    params['leads'] = leads
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
        #raise "Errno::ECONNREFUSED:" + e.to_console
        print 'error'
        DB.execute("UPDATE [append] SET export_error_description='#{e.to_console.to_sql}', export_success=0 WHERE [id] in (#{idsin})")
    rescue => e2
        #raise "Exception:" + e2.to_console
        print 'error'
        DB.execute("UPDATE [append] SET export_error_description='#{e2.to_console.to_sql}', export_success=0 WHERE [id] in (#{idsin})")
    end
    puts 'done'

    #
    GC.start
    DB.disconnect
end # while n > 0

