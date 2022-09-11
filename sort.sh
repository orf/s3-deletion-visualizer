#!/usr/local/bin/fish

mkdir -p data/events_sorted/
rm -rf data/events_sorted/*
echo "Decompressing..."
gzip -d data/events/*.gz
echo "Sorting..."
ls data/events/ | xargs -n 1 -P 4 -I{} bash -c 'jq -s -c "sort_by(.bucket, .operation) | .[]" data/events/{} | gzip > data/events_sorted/{}.gzip'
