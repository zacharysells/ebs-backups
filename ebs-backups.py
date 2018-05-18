import os
import sys
import time
import json
import datetime
from collections import defaultdict

import boto3
import botocore

def volumes_to_snap(ec2):
    """
    Gathers a list of volumes with some metadata to be snapshotted.
    """
    delete_date = datetime.date.today() + datetime.timedelta(days=int(os.environ.get('NUM_SNAPS_TO_KEEP', 7)))
    volumes = []
    response = ec2.describe_instances(Filters=[{'Name': 'tag-key', 'Values': ['backup', 'True']}])
    for r in response['Reservations']:
        for i in r['Instances']:
            for v in i['BlockDeviceMappings']:
                if v.get('Ebs') and v['Ebs']['Status'] == 'attached':
                    snapshot_name = '%s.%s' % (i['InstanceId'], v['DeviceName'])
                    volumes.append({
                        "VolumeId": v['Ebs']['VolumeId'],
                        "Tags": {
                            "Name": snapshot_name,
                            "CreatedBy": "ebs-backups",
                            "DeleteOn": delete_date.strftime('%Y-%m-%d')
                        }
                    })
    return volumes

def create_snaps(volumes):
    """
    Expects 'volumes' argument to be a dictionary object with the following structure
    {
        'Region': 'eu-central-1',
        'Volumes': [
            {
                'VolumeId': 'vol-05fc5e9bd12dace09',
                'Tags': {
                    'Name': 'i-096e2af9541e050c0./dev/sda1',
                    'CreatedBy': 'ebs-backups',
                    'DeleteOn': '2017-11-14'
                }
            },
            {
                'VolumeId': 'vol-0f078e83d46cf9f9a',
                'Tags': {
                    'Name': 'i-096e2af9541e050c0./dev/sdf',
                    'CreatedBy': 'ebs-backups',
                    'DeleteOn': '2017-11-14'
                }
            },
            {
                'VolumeId': 'vol-0205e78cf4e8501c2',
                'Tags': {
                    'Name': 'i-0e7944c5a6f216516./dev/sda1',
                    'CreatedBy': 'ebs-backups',
                    'DeleteOn': '2017-11-14'
                }
            }
        ]
    }
    This fuction then goes through each volume in the specified region and creates a snapshot, tagging it appropriately
    according to the specified tags.
    """
    error = ''
    region = volumes['Region']
    ec2 = boto3.client('ec2', region_name=region)
    print("Creating snapshots for %d volumes in %s" % (len(volumes['Volumes']), region))
    for volume in volumes['Volumes']:
        successful = False
        sleep_time = 1
        while not successful:
            try:
                snapshot_name = volume['Tags']['Name']
                print("Creating snapshot %s" % snapshot_name)
                if not os.environ.get('DRY_RUN', False):
                    response = ec2.create_snapshot(
                        Description=snapshot_name,
                        VolumeId=volume['VolumeId'],
                    )
                    ec2.create_tags(
                        Resources=[response['SnapshotId']],
                        Tags=[
                            {
                                'Key': key,
                                'Value': value
                            }
                            for key, value in volume['Tags'].items()
                        ]
                    )
            except botocore.exceptions.ClientError as e:
                if 'RequestLimitExceeded' in str(e) or 'SnapshotLimitExceeded' in str(e):
                    time.sleep(sleep_time)
                    print("Request limit reached. Sleeping for %ds Retrying %s..." % (sleep_time, snapshot_name))
                    sleep_time *= 2
                else:
                    print(str(e))
                    error += '%s\n' + str(e)
            else:
                successful = True

    return error

def snapshots_to_purge(ec2):
    error = False
    snaps_to_delete = []
    volume_to_snaps = defaultdict(list)
    today = datetime.datetime.today()
    filters = [
        {'Name': 'tag-key', 'Values': ['DeleteOn']},
    ]
    snapshot_response = ec2.describe_snapshots(Filters=filters)
    for snap in snapshot_response['Snapshots']:
        volume_to_snaps[snap['VolumeId']].append(snap)

    for vol_id, snaps in volume_to_snaps.items():
        # sort the list of snapshots by their "DeleteOn" Tag date in decsending order.
        snaps.sort(reverse=True, key=lambda s: next(tag['Value'] for tag in s['Tags'] if tag['Key'] == 'DeleteOn'))
        while len(snaps) > int(os.environ.get('NUM_SNAPS_TO_KEEP', 7)):
            snap_to_del = snaps[-1]
            tags = dict([(t['Key'], t['Value']) for t in snap_to_del['Tags']])
            if tags['DeleteOn'] > today.strftime('%Y-%m-%d'):
                # Don't delete any snapshots that haven't surpased the DeletOn date yet.
                break
            snaps_to_delete.append(snap_to_del['SnapshotId'])
            snaps.pop()

    return snaps_to_delete


def purge_snaps(snaps):
    error = ''
    region = snaps['Region']
    ec2 = boto3.client('ec2', region_name=region)
    print("Purging %d snapshots in %s" % (len(snaps['Snapshots']), region))
    for s in snaps['Snapshots']:
        successful = False
        sleep_time = 1
        while not successful:
            try:
                print("Deleting %s" % s)
                if not os.environ.get('DRY_RUN', False):                
                    ec2.delete_snapshot(SnapshotId=s)
            except botocore.exceptions.ClientError as e:
                if 'RequestLimitExceeded' in str(e):
                    time.sleep(sleep_time)
                    print("Request limit reached. Sleeping for %ds Retrying %s..." % (sleep_time, s))
                    sleep_time *= 2
                else:
                    print(str(e))
                    error += '%s\n' % str(e)
            else:
                successful = True
    return error


def main():
    """
    This function iterates over all regions, takes snapshots of volumes that are attached to an instance that has a 'backup=True' tag.
    It then deletes snapshots that have "expired" i.e are older that the NUM_SNAPS_TO_KEEP environment variable.

    It expects AWS access keys to be set either by environment variables, or by IAM role.

    Accepted environment arguments:
        - NUM_SNAPS_TO_KEEP - Defaults to 7. Number of days of snapshots to keep before purging.
        - DRY_RUN - Defaults to False. Just prints out what it would do, but doesn't actually create or purge snapshots. 
                    If you want to actually do a dry run, just set this environment variable to any string.
    """
    ec2 = boto3.client('ec2')
    response = ec2.describe_regions()
    for r in response['Regions']:
        region_name = r['RegionName']
        print ("Checking region %s..." % region_name)
        ec2 = boto3.client('ec2', region_name=region_name)

        # Volumes to snapshot
        volumes = {
            'Region': region_name,
            'Volumes': volumes_to_snap(ec2)
        }
        err_create = create_snaps(volumes)

        # Snaphots to delete
        snapshots = {
            'Region': region_name,
            'Snapshots': snapshots_to_purge(ec2)
        }
        err_purge = purge_snaps(snapshots)

        if err_create:
            print("The following errors occured during the create_snapshot operation: %s" % err_create)
        if err_purge:
            print("The following errors occured during the purge snapshot operation: %s" % err_purge)

        if err_create or err_purge:
            sys.exit(1)

if __name__ == '__main__':
    main()