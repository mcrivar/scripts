#!/bin/bash

REGION=$1

if [ -z $REGION ]; then echo "Usage: $(basename "$0") <REGION>" && exit 0; fi

channels=$(aws kinesisvideo list-signaling-channels --region eu-west-1 | jq -r '.[][].ChannelARN')
echo "Total channels to delete:"
echo $(aws kinesisvideo list-signaling-channels --region eu-west-1 | jq -r '.[][].ChannelARN' | wc -l)
STATUS=0
for channel in ${channels[@]}; do
	aws kinesisvideo delete-signaling-channel --channel-arn $channel
	if [ $? -eq 0 ]; then
		echo "Deleted channel $channel successfully"
	else
		echo "Failed to delete channel"
		STATUS=$? && exit 1
	fi
done

if [ $STATUS -eq 0 ]; then echo "All channels were deleted successfully"; else "Delete failed"; fi