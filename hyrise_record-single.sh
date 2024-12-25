#!/bin/bash

# perf record -F 99 -g -- ./hyrise-htap/build/hyriseBenchmarkCH
# perf record -F 99 -g -- ./hyrise-chbenchmark/build/hyriseBenchmarkCH -t 300

# nohup ./hyrise-chbenchmark/build/hyriseServer -p 5434 --benchmark_data ch_bench:5 > nohup.out &
# nohup ./hyrise-chbenchmark/build/hyriseServer -p 5435 --benchmark_data ch_bench:10 > nohup.out &
# nohup ./hyrise-chbenchmark/build/hyriseServer -p 5436 --benchmark_data ch_bench:20 > nohup.out &
# server_pid=$!
# sleep 150

# nohup ./hyrise_record-single.sh 61308 5434 > hyrise_record.log &

# ${{ps -aux | grep hyrise | awk '{print $2}' | head -n 1}}          


#别忘了输入端口
server_pid=$1
server_port=$2

time=3600

# tp=1
# ap=1

# python metric.py --save-local -d $time -p -s --no-sudo --pid $server_pid --name sf${sf}_tp${tp}_ap${ap} &
# perf record -F 10 -g -p $server_pid -o perfs${sf}c${tp}h${ap}.data &
# perf_pid=$!
# python ./hyrise-chbenchmark/scripts/ch_bench.py -p $server_port  -cc $tp -ch $ap -t $time > chbenchmark-sf${sf}-tp${tp}-ap${ap}.log
# kill -2 $perf_pid

# echo "perf script -i perfs${sf}c${tp}h${ap}.data &> perf.unfold && ./FlameGraph/stackcollapse-perf.pl perf.unfold &> perf.folded && ./FlameGraph/flamegraph.pl perf.folded > perf-sf${sf}-tp${tp}-ap${ap}-arm.svg"

sf=5
con_num=16
perf_freq=29

tp=$con_num
ap=0

/home/wangzhengjin/usr/python3/bin/python metric.py --save-local -d $time -p -s --no-sudo --pid $server_pid --name sf${sf}_tp${tp}_ap${ap} &
perf record -F $perf_freq -g -p $server_pid -o perfs${sf}c${tp}h${ap}.data &
perf_pid=$!
/home/wangzhengjin/usr/python3/bin/python ./hyrise-chbenchmark/scripts/ch_bench.py -p $server_port  -cc $tp -ch $ap -t $time > chbenchmark-sf${sf}-tp${tp}-ap${ap}.log
kill -2 $perf_pid

echo "perf script -i perfs${sf}c${tp}h${ap}.data &> perf.unfold && ./FlameGraph/stackcollapse-perf.pl perf.unfold &> perf.folded && ./FlameGraph/flamegraph.pl perf.folded > perf-sf${sf}-tp${tp}-ap${ap}-arm.svg"

tp=0
ap=$con_num

/home/wangzhengjin/usr/python3/bin/python  metric.py --save-local -d $time -p -s --no-sudo --pid $server_pid --name sf${sf}_tp${tp}_ap${ap} &
perf record -F $perf_freq -g -p $server_pid -o perfs${sf}c${tp}h${ap}.data &
perf_pid=$!
/home/wangzhengjin/usr/python3/bin/python ./hyrise-chbenchmark/scripts/ch_bench.py -p $server_port  -cc $tp -ch $ap -t $time > chbenchmark-sf${sf}-tp${tp}-ap${ap}.log
kill -2 $perf_pid

echo "perf script -i perfs${sf}c${tp}h${ap}.data &> perf.unfold && ./FlameGraph/stackcollapse-perf.pl perf.unfold &> perf.folded && ./FlameGraph/flamegraph.pl perf.folded > perf-sf${sf}-tp${tp}-ap${ap}-arm.svg"

tp=$con_num
ap=$con_num

/home/wangzhengjin/usr/python3/bin/python  metric.py --save-local -d $time -p -s --no-sudo --pid $server_pid --name sf${sf}_tp${tp}_ap${ap} &
perf record -F $perf_freq -g -p $server_pid -o perfs${sf}c${tp}h${ap}.data &
perf_pid=$!
/home/wangzhengjin/usr/python3/bin/python  ./hyrise-chbenchmark/scripts/ch_bench.py -p $server_port  -cc $tp -ch $ap -t $time > chbenchmark-sf${sf}-tp${tp}-ap${ap}.log
kill -2 $perf_pid

echo "perf script -i perfs${sf}c${tp}h${ap}.data &> perf.unfold && ./FlameGraph/stackcollapse-perf.pl perf.unfold &> perf.folded && ./FlameGraph/flamegraph.pl perf.folded > perf-sf${sf}-tp${tp}-ap${ap}-arm.svg"

# tp=16
# ap=1

# python metric.py --save-local -d $time -p -s --no-sudo --pid $server_pid --name sf${sf}_tp${tp}_ap${ap} &
# perf record -F 10 -g -p $server_pid -o perfs${sf}c${tp}h${ap}.data &
# perf_pid=$!
# python ./hyrise-chbenchmark/scripts/ch_bench.py -p $server_port  -cc $tp -ch $ap -t $time > chbenchmark-sf${sf}-tp${tp}-ap${ap}.log
# kill -2 $perf_pid

# echo "perf script -i perfs${sf}c${tp}h${ap}.data &> perf.unfold && ./FlameGraph/stackcollapse-perf.pl perf.unfold &> perf.folded && ./FlameGraph/flamegraph.pl perf.folded > perf-sf${sf}-tp${tp}-ap${ap}-arm.svg"
# sleep 5

# tp=1
# ap=1
# perf script -i perfs${sf}c${tp}h${ap}.data &> perfs${sf}c${tp}h${ap}.unfold && ./FlameGraph/stackcollapse-perf.pl perfs${sf}c${tp}h${ap}.unfold &> perfs${sf}c${tp}h${ap}.folded && ./FlameGraph/flamegraph.pl perfs${sf}c${tp}h${ap}.folded > perf-sf${sf}-tp${tp}-ap${ap}-arm.svg &
tp=$con_num
ap=0
perf script -i perfs${sf}c${tp}h${ap}.data &> perfs${sf}c${tp}h${ap}.unfold && ./FlameGraph/stackcollapse-perf.pl perfs${sf}c${tp}h${ap}.unfold &> perfs${sf}c${tp}h${ap}.folded && ./FlameGraph/flamegraph.pl perfs${sf}c${tp}h${ap}.folded > perf-sf${sf}-tp${tp}-ap${ap}-arm.svg &
tp=0
ap=$con_num
perf script -i perfs${sf}c${tp}h${ap}.data &> perfs${sf}c${tp}h${ap}.unfold && ./FlameGraph/stackcollapse-perf.pl perfs${sf}c${tp}h${ap}.unfold &> perfs${sf}c${tp}h${ap}.folded && ./FlameGraph/flamegraph.pl perfs${sf}c${tp}h${ap}.folded > perf-sf${sf}-tp${tp}-ap${ap}-arm.svg &
tp=$con_num
ap=$con_num
perf script -i perfs${sf}c${tp}h${ap}.data &> perfs${sf}c${tp}h${ap}.unfold && ./FlameGraph/stackcollapse-perf.pl perfs${sf}c${tp}h${ap}.unfold &> perfs${sf}c${tp}h${ap}.folded && ./FlameGraph/flamegraph.pl perfs${sf}c${tp}h${ap}.folded > perf-sf${sf}-tp${tp}-ap${ap}-arm.svg &
# tp=16
# ap=1
# perf script -i perfs${sf}c${tp}h${ap}.data &> perfs${sf}c${tp}h${ap}.unfold && ./FlameGraph/stackcollapse-perf.pl perfs${sf}c${tp}h${ap}.unfold &> perfs${sf}c${tp}h${ap}.folded && ./FlameGraph/flamegraph.pl perfs${sf}c${tp}h${ap}.folded > perf-sf${sf}-tp${tp}-ap${ap}-arm.svg &