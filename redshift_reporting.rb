require 'pg'

class RedshiftReporting

  # This is very similar to the connection setup we're using in the Redshift
  # Operations class. Admittedly, we're copying code such that each file is
  # independent. I would suggest, in production, that reporting uses a dedicated
  # read-only account. Lowest possible impact radius if the account is
  # compromised.
  def initialize(
    dbname: ENV["redshift_dbname"],
    host: ENV["redshift_host"],
    user: ENV["redshift_user"],
    password: ENV["redshift_password"],
    port: 5439
  )
    @conn = PG.connect(
      dbname: dbname,
      host: host,
      user: user,
      password: password,
      port: port,
      sslmode: 'require'
    )
  end

  def run_query(query)
    result = @conn.exec(query)
    print_query_result(result)
    result # Return the result hash.
  end

  private
  # This is more oriented towards pretty printing, but it's also a great way to
  # CSV format a query result. Just join with ',' instead of tabs, and build a
  # string instead of printing.
  def print_query_result(result)
    # First row is the key names.
    puts result.first.keys.join("\t")
    result.each do |row|
      puts row.values.join("\t")
    end
  end
end

# Added bonus - runnable queries.
class ExampleQueries
  def initialize
    @reporter = RedshiftReporting.new
  end

  def user_count
    statement = "SELECT COUNT(DISTINCT USERNAME) FROM FACT_DAILY_REQUESTS"
    puts "Query:\n#{statement}\n"
    @reporter.run_query(statement)
  end

  def requests_by_date
    statement = "SELECT END_DATE, SUM(REQUEST_COUNT)
      FROM FACT_DAILY_REQUESTS
      WHERE END_DATE BETWEEN '2015-10-01' AND '2015-10-07'
      GROUP BY END_DATE
      ORDER BY END_DATE ASC"
    puts "Query:\n#{statement}\n"
    @reporter.run_query(statement)
  end

  def top_product_views_last_week
    statement = "SELECT REQUEST, SUM(REQUEST_COUNT) AS TOTAL
      FROM FACT_DAILY_REQUESTS
      WHERE REQUEST ILIKE 'GET /products/%'
      AND END_DATE BETWEEN '2015-10-01' AND '2015-10-07'
      GROUP BY REQUEST
      ORDER BY TOTAL DESC
      LIMIT 10"
    puts "Query:\n#{statement}\n"
    @reporter.run_query(statement)
  end
end
