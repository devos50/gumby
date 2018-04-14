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
        self.avg_dist = 0.0
        self.total_rides = 0
        self.total_up_bw = 0
        self.total_down_bw = 0

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
        for peer_nr, filename, dir in self.yield_files(file_to_check='bandwidth.txt'):
            with open(filename) as bandwidth_file:
                parts = bandwidth_file.read().rstrip('\n').split(",")
                self.total_up_bw += float(parts[0])
                self.total_down_bw += float(parts[1])

        with open('total_bandwidth.log', 'w', 0) as taxi_file:
            taxi_file.write("%f,%f,%f\n" % (self.total_up_bw, self.total_down_bw, self.total_up_bw + self.total_down_bw))

    def aggregate_taxi_rides(self):
        final_string = ''
        for peer_nr, filename, dir in self.yield_files(file_to_check='taxi_rides.log'):
            with open(filename) as taxi_file:
                line = taxi_file.read()
                if line:
                    distance = float(line.rstrip('\n').split(',')[-1])
                    self.avg_dist += distance
                    self.total_rides += 1
                    final_string += line

        with open('taxi_rides.log', 'w', 0) as taxi_file:
            taxi_file.write(final_string)

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

    def write_parameters(self):
        self.avg_dist /= float(self.total_rides)

        if not os.path.exists(os.path.join("..", "results")):
            os.mkdir(os.path.join("..", "results"))

        # Append the results to the file
        scenarios = [(1, 5, 1), (1, 5, 110), (1, 5, 220), (1, 5, 330), (1, 5, 440), (1, 5, 550),
                     (1, 8, 1), (1, 8, 110), (1, 8, 220), (1, 8, 330), (1, 8, 440), (1, 8, 550),
                     (1, 10, 1), (1, 10, 110), (1, 10, 220), (1, 10, 330), (1, 10, 440), (1, 10, 550),
                     (2, 5, 1), (2, 5, 110), (2, 5, 220), (2, 5, 330), (2, 5, 440), (2, 5, 550),]

        ttl = int(os.environ["DEFAULT_TTL"])  # 1 or 2
        brange = int(os.environ["BROADCAST_RANGE"])  # 5, 8 or 10
        matchmakers = int(os.environ["NUM_MATCHMAKERS"])  # 55, 110, 165, 220, 275, ...
        with open(os.path.join("..", "results", "results.txt"), "a") as results_file:
            results_file.write("%s,%s,%s,%f,%f,%f,%f\n" % (os.environ["DEFAULT_TTL"], os.environ["BROADCAST_RANGE"], os.environ["NUM_MATCHMAKERS"], self.avg_dist, self.total_up_bw, self.total_down_bw, self.total_up_bw + self.total_down_bw))

        # Determine parameters for the next run
        cur_ind = scenarios.index((ttl, brange, matchmakers))
        if cur_ind == len(scenarios) - 1:  # Wrap around
            next_ind = 0
        else:
            next_ind = cur_ind + 1

        with open(os.path.join("..", "parameters.ini"), "w") as parameters_file:
            parameters_file.write("GUMBY_DEFAULT_TTL=%d\n" % scenarios[next_ind][0])
            parameters_file.write("GUMBY_BROADCAST_RANGE=%d\n" % scenarios[next_ind][1])
            parameters_file.write("GUMBY_NUM_MATCHMAKERS=%d\n" % scenarios[next_ind][2])

    def run(self):
        self.aggregate_transaction_data()
        self.aggregate_candidate_connections()
        self.aggregate_order_data()
        self.aggregate_general_stats()
        self.aggregate_bandwidth()
        self.aggregate_taxi_rides()
        self.write_parameters()


# cd to the output directory
os.chdir(os.environ['OUTPUT_DIR'])

parser = MarketStatisticsParser(sys.argv[1])
parser.run()
