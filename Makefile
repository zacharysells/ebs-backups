build:
	docker build -t zacharysells/ebs-backups .

run: build
	docker run \
	-e AWS_PROFILE \
	-e NUM_SNAPS_TO_KEEP \
	-e DRY_RUN \
	-v ~/.aws/:/root/.aws \
	-ti zacharysells/ebs-backups

push: build
	docker push zacharysells/ebs-backups