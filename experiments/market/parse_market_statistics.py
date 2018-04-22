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

    def aggregate_transaction_data(self):
        """
        Aggregate all transaction data during the experiment
        """
        transactions_str = ""
        transactions_cumulative_str = "0,0\n"
        transactions_times = []

        for peer_nr, filename, dir in self.yield_files(file_to_check='transactions.log'):
            transactions = [line.rstrip('\n') for line in open(filename)]
            for transaction in transactions:
                parts = transaction.split(',')
                self.total_quantity_traded += float(parts[2])
                self.total_payment += float(parts[1]) * float(parts[2])
                transactions_str += transaction + '\n'
                transactions_times.append(float(parts[0]))

        transactions_times = sorted(transactions_times)
        total_transactions = 0
        for transaction_time in transactions_times:
            total_transactions += 1
            transactions_cumulative_str += str(transaction_time) + "," + str(total_transactions) + "\n"

        with open('transactions.log', 'w', 0) as transactions_file:
            transactions_file.write("time,price,quantity,payments,peer1,peer2\n")
            transactions_file.write(transactions_str)

        with open('transactions_cumulative.csv', 'w') as transactions_file:
            transactions_file.write("time,transactions\n")
            transactions_file.write(transactions_cumulative_str)

    def aggregate_candidate_connections(self):
        candidate_connections = set()

        for peer_nr, filename, dir in self.yield_files(file_to_check='verified_candidates.txt'):
            peer_connections = [line.rstrip('\n') for line in open(filename)]
            for peer_connection in peer_connections:
                candidate_connections.add((peer_nr, int(peer_connection)))

        with open('candidate_connections.log', 'w', 0) as connections_file:
            connections_file.write("peer_a,peer_b\n")
            for peer_a, peer_b in candidate_connections:
                connections_file.write("%d,%d\n" % (peer_a, peer_b))

    def aggregate_bandwidth(self):
        total_up, total_down = 0, 0
        for peer_nr, filename, dir in self.yield_files(file_to_check='bandwidth.txt'):
            with open(filename) as bandwidth_file:
                parts = bandwidth_file.read().rstrip('\n').split(",")
                total_up += int(parts[0])
                total_down += int(parts[1])

        with open('total_bandwidth.log', 'w', 0) as taxi_file:
            taxi_file.write("%s,%s,%s\n" % (total_up, total_down, (total_up + total_down) / 2))

    def aggregate_order_data(self):
        """
        Aggregate all data of the orders
        """
        orders_str = "time,id,peer,is_ask,completed,price,quantity,reserved_quantity,traded_quantity,completed_time\n"
        orders_data_all = ""

        for peer_nr, filename, dir in self.yield_files(file_to_check='orders.log'):
            with open(filename) as order_file:
                orders_data = order_file.read()
                orders_str += orders_data
                orders_data_all += orders_data

        with open('orders.log', 'w', 0) as orders_file:
            orders_file.write(orders_str)

        # Calculate the average order latency
        sum = 0
        amount = 0

        for line in orders_data_all.split('\n'):
            if len(line) == 0:
                continue

            parts = line.split(',')
            if parts[4] == "complete":
                sum += float(parts[9]) - float(parts[0])
                amount += 1

            if parts[3] == "ask":
                self.total_ask_quantity += float(parts[6])
            else:
                self.total_bid_quantity += float(parts[6])

        if amount > 0:
            self.avg_order_latency = float(sum) / float(amount)

    def aggregate_general_stats(self):
        """
        Aggregate general statistics for each peer
        """
        total_asks = 0
        total_bids = 0
        fulfilled_asks = 0
        fulfilled_bids = 0

        for peer_nr, filename, dir in self.yield_files(file_to_check='market_stats.log'):
            with open(filename) as stats_file:
                stats_dict = json.loads(stats_file.read())
                total_asks += stats_dict['asks']
                total_bids += stats_dict['bids']
                fulfilled_asks += stats_dict['fulfilled_asks']
                fulfilled_bids += stats_dict['fulfilled_bids']

        with open('aggregated_market_stats.log', 'w', 0) as stats_file:
            stats_dict = {'asks': total_asks, 'bids': total_bids,
                          'fulfilled_asks': fulfilled_asks, 'fulfilled_bids': fulfilled_bids,
                          'total_quantity_traded': self.total_quantity_traded,
                          'total_payment': self.total_payment,
                          'avg_order_latency': self.avg_order_latency,
                          'total_ask_quantity': int(self.total_ask_quantity),
                          'total_bid_quantity': int(self.total_bid_quantity)}
            stats_file.write(json.dumps(stats_dict))

    def analyse_blocks(self):
        blocks = []  # List of timestamps
        for peer_nr, filename, dir in self.yield_files(file_to_check='full_blocks.txt'):
            with open(filename) as blocks_file:
                for line in blocks_file.readlines():
                    if not line:
                        continue
                    parts = line.split(",")
                    blocks.append(int(parts[1]))

        # Sort the blocks
        blocks = sorted(blocks)

        # Write them
        with open("full_blocks.txt", "w") as blocks_file:
            for block in blocks:
                blocks_file.write("%d\n" % block)

        # Find the maximum throughput
        cur_max = -1
        for start_index in xrange(len(blocks)):
            start_time = blocks[start_index]
            cnt = 1
            cur_index = start_index + 1
            while cur_index < len(blocks) and blocks[cur_index] - start_time < 1000:
                cnt += 1
                cur_index += 1

            if cnt > cur_max:
                cur_max = cnt

        with open("throughput.txt", "w") as throughput_file:
            throughput_file.write("%d" % cur_max)

    def run(self):
        self.aggregate_transaction_data()
        self.aggregate_candidate_connections()
        self.aggregate_order_data()
        self.aggregate_general_stats()
        self.aggregate_bandwidth()
        self.analyse_blocks()


# cd to the output directory
os.chdir(os.environ['OUTPUT_DIR'])

parser = MarketStatisticsParser(sys.argv[1])
parser.run()
