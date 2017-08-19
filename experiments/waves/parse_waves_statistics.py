#!/usr/bin/env python2
import json
import os
import re
import sys


class WavesStatisticsParser(object):
    """
    This class is responsible for parsing statistics of the market community
    """

    def __init__(self, node_directory):
        self.node_directory = node_directory

    def yield_files(self, file_to_check='market_stats.log'):
        pattern = re.compile('[0-9]+')
        for headnode in os.listdir(self.node_directory):
            headdir = os.path.join(self.node_directory, headnode)
            if os.path.isdir(headdir):
                for node in os.listdir(headdir):
                    nodedir = os.path.join(self.node_directory, headnode, node)
                    if os.path.isdir(nodedir):
                        for peer in os.listdir(nodedir):
                            peerdir = os.path.join(self.node_directory, headnode, node, peer)
                            if os.path.isdir(peerdir) and pattern.match(peer):
                                peer_nr = int(peer)

                                filename = os.path.join(self.node_directory, headnode, node, peer, file_to_check)
                                if os.path.exists(filename):
                                    yield peer_nr, filename, peerdir

    def analyse_blocks(self):
        block_throughputs = []  # List of (time, num_transactions)
        for peer_nr, filename, dir in self.yield_files(file_to_check='blockchain.txt'):
            with open(filename) as blockchain_file:
                lines = blockchain_file.readlines()
                for line in lines:
                    if not line:
                        continue
                    block = json.loads(line)
                    block_throughputs.append((block["timestamp"], block["transactionCount"]))

        # Find the maximum throughput per second
        max_throughput = 0
        for cur_index in xrange(1, len(block_throughputs)):  # Compare with the prev block every time
            cur_block = block_throughputs[cur_index]
            prev_block = block_throughputs[cur_index - 1]
            time_interval = float(cur_block[0] - prev_block[0]) / 1000.0  # Convert to seconds
            throughput = float(cur_block[1]) / float(time_interval)
            if throughput > max_throughput:
                max_throughput = throughput

        with open("throughput.txt", "w", 0) as throughput_file:
            throughput_file.write("%f" % max_throughput)

    def run(self):
        self.analyse_blocks()


# cd to the output directory
os.chdir(os.environ['OUTPUT_DIR'])

parser = WavesStatisticsParser(sys.argv[1])
parser.run()
