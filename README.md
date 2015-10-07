# Sample Code for "Large Scale Metrics Analysis in Ruby"

This repository contains annotated example code to accompany the AWS re:Invent
2015 presentation DEV309: "Large Scale Metrics Analysis in Ruby".

# mapper.rb and reducer.rb

These are executable Ruby files that can be used with Hadoop Streaming on Amazon
Elastic MapReduce clusters. They're made to work with a particular log file
format shown in the presentation. However, like most of these examples, they can
serve as useful guideposts when you design your own purpose-built mappers and
reducers.

# log_processor.rb and batching_example.rb

Shows an example of a control plane for log processing jobs, that can support
job batching. Includes an example of how this control plane might be called for
a simple example job.

# redshift_operations.rb

Shows some Redshift cluster management operations, based on the pattern used in
the presentation.
