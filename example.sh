#!/bin/sh

echo example 1 using arrow-cat
./nics2arrow-ipc-stream |
	arrow-cat |
	tail -3

echo
echo example 2 using sql
./nics2arrow-ipc-stream |
	rs-ipc-stream2df \
	--max-rows 1024 \
	--tabname 'nics' \
	--sql "
		SELECT
			index,
			name,
			if_type,
			ipv4,
			oper_state,
			mtu,
			is_up,
			is_multicast,
			is_point_to_point,
			is_running,
			is_physical
		FROM nics
		LIMIT 3
	" |
	rs-arrow-ipc-stream-cat
