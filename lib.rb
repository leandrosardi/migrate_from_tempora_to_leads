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
    ret['name'] = p[:name]
    ret['position'] = p[:headline]
    ret['location'] = p[:location]
    ret['industry'] = p[:industry]

    # if the profile is not linked to a company record
    ret['company'] = nil
    ret['id_company_from_headline'] = nil
    if !ret['id_company_from_headline'].nil?
        cid = ret['id_company_from_headline']
        ret['id_company_from_headline'] = cid
        c = DB["SELECT name, website FROM [company] WHERE [id]='#{cid}'"].first
        ret['company'] = {
            'name' => c[:name],
            'url' => c[:website],
        }
    elsif !cname.nil?
        ret['company'] = {
            'name' => cname,
            'url' => nil,
        }
    end 

    # convert to utf-8 compatible string
    ret['name'].to_s.encode!("utf-8", :undef=>:replace, :invalid=>:replace, :replace=>'?')
    ret['position'].to_s.encode!("utf-8", :undef=>:replace, :invalid=>:replace, :replace=>'?')
    if !ret['company'].nil?
        ret['company']['name'].to_s.encode!("utf-8", :undef=>:replace, :invalid=>:replace, :replace=>'?')
    end
    ret['location'].to_s.encode!("utf-8", :undef=>:replace, :invalid=>:replace, :replace=>'?')

    # iterate data
    datas = []
    DB["
        SELECT ISNULL([type], 20) AS [type], [email]
        FROM [append] WITH (NOLOCK)
        WHERE id_profile='#{pid}'
        AND ISNULL([type], 20) IN (10,20,90)
        AND (
            ISNULL([type], 20) <> 90
            OR
            ( 
                -- these are patterns of sales navigator URLs, who are not public profiles URLs
                email like '%linkedin.com/in/%'
                and
                len(email)<>63 
                and
                email not like '%NAME_SEARCH%'
            )
        )
        AND (
            ISNULL([type], 20) <> 20
            OR
            ( 
                -- emails with wrong format
                email not like 'http%'
                and
                email like '%@%'
            )
        )

    "].all { |row|
        # remove emails with wrong format
        # remove linkedin URLs with wrong format
        if (
            row[:type].to_i==10 || ( row[:type].to_i==20 && row[:email].to_s.email? ) || ( row[:type].to_i==90 && row[:email].to_s =~ /((https?:\/\/)?(www\.)?linkedin\.com\/in\/)(([-A-Za-z0-9\%](\/?))+$)/ )
        )
            datas << {
                'type' => row[:type].to_i,
                'value' => row[:email],
            }
        end
    }
    ret['datas'] = datas

    # return
    ret
end # def profile_descriptor