#!/usr/bin/env python2
import os
import re
import sys


class TrustchainStatisticsParser(object):
    """
    This class is responsible for parsing statistics of the trustchain community
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

    def parse_fraud_times(self):
        lowest_time = 2525451299248
        for peer_nr, filename, dir in self.yield_files(file_to_check='detection_time.txt'):
            with open(filename) as detect_time_file:
                detect_time = int(detect_time_file.read().rstrip('\n'))
                if detect_time < lowest_time:
                    lowest_time = detect_time

        fraud_time = -1
        for peer_nr, filename, dir in self.yield_files(file_to_check='fraud_time.txt'):
            with open(filename) as detect_time_file:
                fraud_time = int(detect_time_file.read().rstrip('\n'))
                break

        if fraud_time != -1:
            with open("detect_time.txt", "w") as detect_time_file:
                detect_time_file.write("%d" % (lowest_time - fraud_time))

    def run(self):
        self.parse_fraud_times()


# cd to the output directory
os.chdir(os.environ['OUTPUT_DIR'])

parser = TrustchainStatisticsParser(sys.argv[1])
parser.run()
