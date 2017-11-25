#!/bin/bash

cat "$@" \
	| awk '
		{
			if (! /^insert/ && $3 !~ /\|/) {
				$3 = $3 "|0";
			} else {
				$3 = $3;
			}
			print;
		}
		' \
	| sed \
		-e '/^connected to/d' \
		-e '/can.t get data/d' \
		-e '/reconnect/d' \
		-e '/DBClient/d' \
		-e 's/\*//g' \
		-e 's/|/ /g' \
		-e 's/:\([0-9.]\+%\)/ \1/' \
	| awk '
		BEGIN {
			print "#time insert query update update_r delete getmore command command_r flushes mapped vsize res non-mapped faults locked_db lock% idx_miss_% qr qw ar aw netIn netOut conn set repl";
			header = 0;
		}

		{
			if (/^insert/) {
				if (!header) {
					$0 = gensub("^", "#time ", "", $0);
					$0 = gensub("time *$", "", "", $0);
					$0 = gensub("update", "update update_r", "", $0);
					$0 = gensub("command", "command command_r", "", $0);
					$0 = gensub("locked db", "locked_db lock%", "", $0);
					$0 = gensub("idx miss %", "idx_miss_%", "", $0);
					for (i = 1; i <= NF; i++) {
						printf("%s(%d)%s", (i==1)?"#":"", i, (i==NF)?"\n":" ");
					}
					print;
					#header = 1;
				}

			} else {

				for (i = 1; i <= NF; i++) {
					if ($i ~ /k$/) {
						$i = 1000 * gensub("k$", "", "", $i);
					}
					if ($i ~ /m$/) {
						$i = 1000000 * gensub("m$", "", "", $i);
					}
					if ($i ~ /g$/) {
						$i = 1000000000 * gensub("g$", "", "", $i);
					}
				}
				if ($3 >= 0) {
				}
				if ($1 >= 0) {
					time = $NF;
					for (i = NF; i > 1; i--) {
						$i = $(i-1);
					}
					$1 = time;
					print;
				}
			}
		}
		' \
	| column -tn

