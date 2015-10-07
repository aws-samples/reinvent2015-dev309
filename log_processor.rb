require 'aws-sdk-core'

# This is one way you could implement a log processing control plane. This
# example is meant to be concise while still supporting batching within a
# single cluster.
class LogProcessor

  AMI_VERSION = "3.9.0"
  HADOOP_STREAMING_JAR = "/home/hadoop/contrib/streaming/hadoop-streaming.jar"

  # These roles won't exist if you've never launched an EMR cluster before.
  # See the EMR docs for instructions on how to generate these.
  DEFAULT_JOB_FLOW_ROLE = "EMR_EC2_DefaultRole"
  DEFAULT_SERVICE_ROLE = "EMR_DefaultRole"

  # The values we set here should likely be global across a cluster. Though
  # there is nothing stopping you from including completely independent jobs
  # in the same cluster by using per-step overrides as defined below.
  def initialize(
    region: "us-east-1",
    input_bucket:,
    output_bucket:,
    code_bucket:,
    log_bucket:
  )
    @client = Aws::EMR::Client.new(region: region)
    @input_bucket = input_bucket
    @output_bucket = output_bucket
    @code_bucket = code_bucket
    @log_bucket = log_bucket
  end

  # This function builds two client calls to the EMR API to create our job
  # flow in full. One defines the cluster, and the other adds the job steps.
  def run_job(instance_options:,shared_step_args:,step_args:)
    # Much of this code is just defining step details, so see that function
    # for details.
    steps = configure_steps(
      shared_args: shared_step_args,
      step_args: step_args
    )
    # This is the option hash for the #run_job_flow API call.
    opts = {
      name: "Log Processing Job",
      ami_version: AMI_VERSION,
      # Instance configuration is a bit more involved, so it is pulled into its
      # own function.
      instances: configure_instances(instance_options),
      steps: [
        # The debugging step is pretty important to include and run first. If
        # any other step fails, this will, for example, provide the log files
        # and task details for each process in the job.
        {
          name: "Setup Debugging",
          action_on_failure: "TERMINATE_CLUSTER",
          hadoop_jar_step: {
            jar: "s3://us-east-1.elasticmapreduce/libs/script-runner/script-runner.jar",
            args: ["s3://us-east-1.elasticmapreduce/libs/state-pusher/0.1/fetch"]
          }
        }
      ],
      # Those debug logs go here. Best to dedicate a bucket to this purpose.
      log_uri: "s3://#{@log_bucket}/",
      # Note that, if your mappers or reducers will be calling out to other AWS
      # services, you may need purpose-built roles here.
      job_flow_role: DEFAULT_JOB_FLOW_ROLE,
      service_role: DEFAULT_SERVICE_ROLE
    }
    puts "Creating job flow."
    resp = @client.run_job_flow(opts)
    puts "Adding processing steps to job flow."
    @client.add_job_flow_steps(
      job_flow_id: resp.job_flow_id,
      steps: steps
    )
    resp.job_flow_id
  end

  private
  # Recall from the presentation that master node and worker node types may vary
  # based on the specific needs of your processing jobs. You should make sure to
  # have your EC2 keys handy, you need it if you intend on logging in to your
  # master node for any debugging.
  def configure_instances(master_type:,worker_type:,worker_count:,ec2_key_name:)
    instances = {
      instance_groups: [
        {
          name: "Master",
          market: "ON_DEMAND",
          instance_role: "MASTER",
          instance_type: master_type,
          instance_count: 1
        },
        {
          name: "Workers",
          market: "ON_DEMAND",
          instance_role: "CORE",
          instance_type: worker_type,
          instance_count: worker_count
        }
      ],
      ec2_key_name: ec2_key_name
    }
  end

  # In this example, "steps" are defined by having an associated "step_args"
  # array entry.
  def configure_steps(shared_args:,step_args:)
    steps = []
    step_args.each do |args|
      steps << define_step(args.merge(shared_args))
    end
    steps
  end

  # This builds out a single step definition as explained in the presentation.
  # It is useful to note that we're providing "reasonable default" values here
  # for many options, as they often have a common convention. In many cases, the
  # ONLY thing that will vary between steps in a cluster is the input source and
  # output destination.
  def define_step(
    name: "Log Processing Step",
    action_on_failure: "CONTINUE",
    mapper_filename: "mapper.rb",
    reducer_filename: "reducer.rb",
    mapper_prefix: "code/",
    reducer_prefix: "code/",
    input_prefix:,
    output_prefix:
  )
    return {
      name: name,
      # We've chosen to continue running the rest of the jobs if one fails.
      # If you don't want this, you could terminate the cluster on step failure
      # instead.
      action_on_failure: action_on_failure,
      hadoop_jar_step: {
        jar: HADOOP_STREAMING_JAR,
        args: [
          "--files", "\"s3://#{@code_bucket}/#{mapper_prefix}/#{mapper_filename},"\
            "s3://#{@code_bucket}/#{reducer_prefix}/#{reducer_filename}\"",
          "-input", "s3://#{@input_bucket}/#{input_prefix}",
          "-output", "s3://#{@output_bucket}/#{output_prefix}",
          "-mapper", mapper_filename,
          "-reducer", reducer_filename
        ]
      }
    }
  end

end
