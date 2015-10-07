# This script shows how you might call the LogProcessor class to kick off a
# clustered job. Names are very stub-like.

require_relative 'log_processor'

# Bucket names are obviously tremendously fake. Substitute your own as
# appropriate.
processor = LogProcessor.new(
  input_bucket: "log-bucket",
  output_bucket: "job-result-bucket",
  code_bucket: "source-code-bucket",
  log_bucket: "debug-log-bucket" # "Logging the Log Processing: Meta Logs"
)

instance_options = {
  master_type: "m1.large",
  worker_type: "m1.large",
  worker_count: 5,
  ec2_key_name: "mykey"
}

# Note that individual steps do not vary much at all. Just input sources and
# output targets.
step_args = [
  {
    name: "Log Processing Job 1",
    input_prefix: "input/1/",
    output_prefix: "output/1/"
  },
  {
    name: "Log Processing Job 2",
    input_prefix: "input/2/",
    output_prefix: "output/2/"
  },
  {
    name: "Log Processing Job 3",
    input_prefix: "input/3/",
    output_prefix: "output/3/"
  },
  {
    name: "Log Processing Job 4",
    input_prefix: "input/4/",
    output_prefix: "output/4/"
  },
]

resp = processor.run_job(
  instance_options: instance_options,
  shared_step_args: {}, # In this case, using defaults.
  step_args: step_args
)
