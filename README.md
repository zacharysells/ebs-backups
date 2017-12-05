
![travis](https://travis-ci.org/zacharysells/ebs-backups.svg?branch=master)

# ebs-backups
This is the source of a Docker image that can be used to generate EBS backups. 
The script will iterate over all AWS regions, looking for instances with a `backup=True` tag. Any found instances will have their volumes snapshotted. 

# Usage
While this image can be run locally, it is meant to be run on a scheduled interval via some other process. 

To run it locally, use the `Makefile`
```
make run
```
The `Makefile` will forward AWS authentication environment variables like `AWS_PROFILE`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` into the container.

It will also forward certain configuration settings as environment variables. Here's an example with passing in environment variables
```
AWS_PROFILE=dev NUM_SNAPS_TO_KEEP=5 make run
```
