require 'json'
require 'pg'

class RedshiftOperations

  # When building a connection, remember: It's just SQL. There is absolutely no
  # "special" Redshift adapter involved. By default, we will source some of
  # these values from environment variables to avoid hard-coding.
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

  # 1:1 from the slide deck. Just execute the statement and you're done. Note
  # especially that we can and do have multiple sortkeys to improve query
  # performance.
  def create_table
    statement = "CREATE TABLE FACT_DAILY_REQUESTS(
       USERNAME VARCHAR(30) NOT NULL DISTKEY,
       SESSION_ID VARCHAR(10),
       USER_AGENT VARCHAR(256) NOT NULL,
       END_DATE DATE NOT NULL,
       REQUEST VARCHAR(128) NOT NULL,
       RESPONSE_CODE INTEGER NOT NULL,
       REQUEST_COUNT INTEGER NOT NULL
     )
     INTERLEAVED SORTKEY(END_DATE,REQUEST,RESPONSE_CODE)"
    @conn.exec(statement)
  end

  # To copy from S3, Redshift currently requires AWS credentials. Because you
  # should NEVER hard code credentials, we're fetching our credentials from the
  # IAM role attached to our EC2 instances. These temporary credentials are a
  # great way to avoid putting credentials anywhere in your source. The rest is
  # 1:1 from the slides.
  def ingest_data(bucket: nil, prefix: nil, iam_role: nil)
    credentials = JSON.parse(
      `curl http://169.254.169.254/latest/meta-data/iam/security-credentials/#{iam_role}`
    )
    statement = "COPY FACT_DAILY_REQUESTS
      FROM 's3://#{bucket}/#{prefix}'
      DATEFORMAT AS 'DD/MON/YYYY'
      DELIMITER '\\t'
      CREDENTIALS 'aws_access_key_id=#{credentials["AccessKeyId"]};"\
      "aws_secret_access_key=#{credentials["SecretAccessKey"]};"\
      "token=#{credentials["Token"]}'"
    @conn.exec(statement)
  end
end
