#!/usr/bin/env python
import json
import os
import sys

from gumby.statsparser import StatisticsParser


class WavesStatisticsParser(StatisticsParser):
    """
    This class is responsible for parsing statistics of the market community
    """

    def __init__(self, node_directory):
        super(WavesStatisticsParser, self).__init__(node_directory)

    def analyse_blocks(self):
        order_latencies = []
        block_throughputs = []  # List of (time, num_transactions)
        for peer_nr, filename, dir in self.yield_files(file_to_check='blockchain.txt'):
            with open(filename) as blockchain_file:
                lines = blockchain_file.readlines()
                for line in lines:
                    if not line:
                        continue
                    block = json.loads(line)

                    order_blocks = 0
                    for transaction in block["transactions"]:
                        if transaction["type"] != 4 and transaction["type"] != 3:
                            order_blocks += 1
                            order_latency = block["timestamp"] - transaction["timestamp"]
                            if order_latency > 0:
                                order_latencies.append(order_latency)

                    block_throughputs.append((block["timestamp"], order_blocks))

        with open("order_latencies.txt", "w") as order_latencies_file:
            order_latencies_file.write("time\n")
            for latency in order_latencies:
                order_latencies_file.write("%s\n" % latency)

        # Find the maximum throughput per second
        max_throughput = 0
        for cur_index in range(1, len(block_throughputs)):  # Compare with the prev block every time
            cur_block = block_throughputs[cur_index]
            prev_block = block_throughputs[cur_index - 1]
            time_interval = float(cur_block[0] - prev_block[0]) / 1000.0  # Convert to seconds
            throughput = float(cur_block[1]) / float(time_interval)
            if throughput > max_throughput:
                max_throughput = throughput

        with open("throughput.txt", "w") as throughput_file:
            throughput_file.write("%f" % max_throughput)

    def run(self):
        self.analyse_blocks()


# cd to the output directory
os.chdir(os.environ['OUTPUT_DIR'])

parser = WavesStatisticsParser(sys.argv[1])
parser.run()
