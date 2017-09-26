#!/usr/bin/env python2
import json
import os
import re
import sys


class MarketStatisticsParser(object):
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

    def aggregate_general_stats(self):
        """
        Aggregate general statistics for each peer
        """
        avg_pagerank_rank = 0
        avg_pagerank_value = 0
        avg_personalised_rank = 0
        avg_personalised_value = 0
        avg_temporal_rank = 0
        avg_temporal_value = 0

        count = 1
        for peer_nr, filename, dir in self.yield_files(file_to_check='results.csv'):
            with open(filename) as stats_file:
                content = stats_file.read()
                for line in content.split('\n'):
                    if len(line) == 0:
                        continue

                    parts = line.split(",")
                    if parts[3] == "PageRank":
                        avg_pagerank_rank += float(parts[1])
                        avg_pagerank_value += float(parts[2])
                    elif parts[3] == "Personalised PageRank":
                        avg_personalised_rank += float(parts[1])
                        avg_personalised_value += float(parts[2])
                    else:
                        avg_temporal_rank += float(parts[1])
                        avg_temporal_value += float(parts[2])

            count += 1

        avg_pagerank_rank /= count
        avg_pagerank_value /= count
        avg_personalised_rank /= count
        avg_personalised_value /= count
        avg_temporal_rank /= count
        avg_temporal_value /= count

        sybil_region_size = int(os.environ['SYBIL_REGION_SIZE'])

        with open('results.log', 'w', 0) as stats_file:
            stats_file.write("%d,%f,%f,%s\n" % (sybil_region_size, avg_pagerank_rank, avg_pagerank_value, "PageRank"))
            stats_file.write("%d,%f,%f,%s\n" % (sybil_region_size, avg_personalised_rank, avg_personalised_value, "Personalised PageRank"))
            stats_file.write("%d,%f,%f,%s\n" % (sybil_region_size, avg_temporal_rank, avg_temporal_value, "Temporal PageRank"))

    def run(self):
        self.aggregate_general_stats()


# cd to the output directory
os.chdir(os.environ['OUTPUT_DIR'])

parser = MarketStatisticsParser(sys.argv[1])
parser.run()
