#!/usr/bin/env python2
import os
import re
import sys


class IOMStatisticsParser(object):
    """
    This class is responsible for parsing statistics of the market community
    """

    def __init__(self, node_directory):
        self.node_directory = node_directory
        self.total_quantity_traded = 0
        self.total_payment = 0
        self.total_ask_quantity = 0
        self.total_bid_quantity = 0
        self.avg_order_latency = 0

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

    def aggregate_candidate_connections(self):
        with open('candidate_connections.log', 'w', 0) as connections_file:
            for peer_nr, filename, dir in self.yield_files(file_to_check='candidate_connections_evolve.log'):
                candidate_connections_evolve = [line for line in open(filename)]
                for line in candidate_connections_evolve:
                    connections_file.write("%s %s" % (str(peer_nr), line))

    def aggregate_stolen_money(self):
        totals = {}
        for peer_nr, filename, dir in self.yield_files(file_to_check='stolen.log'):
            with open(filename, 'r') as stolen_file:
                for line in stolen_file.readlines():
                    parts = line.rstrip().split(" ")
                    time = int(parts[0])
                    amount = float(parts[1])

                    if time not in totals:
                        totals[time] = 0
                    totals[time] += amount

        with open('total_stolen.log', 'w', 0) as total_file:
            total_file.write('time,total_stolen\n')
            for time in totals.keys():
                total_file.write('%d,%.2f\n' % (time, totals[time]))

    def aggregate_total_payments(self):
        total = 0
        for peer_nr, filename, dir in self.yield_files(file_to_check='total_payments.log'):
            with open(filename, 'r') as payments_file:
                total += float(payments_file.read())

        with open('total_transacted.log', 'w') as transacted_file:
            transacted_file.write('%f' % total)

    def run(self):
        self.aggregate_candidate_connections()
        self.aggregate_total_payments()
        self.aggregate_stolen_money()


# cd to the output directory
os.chdir(os.environ['OUTPUT_DIR'])

parser = IOMStatisticsParser(sys.argv[1])
parser.run()
