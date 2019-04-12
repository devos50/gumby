#!/usr/bin/env python2
import json
import os
import sys

from gumby.statsparser import StatisticsParser


class MarketStatisticsParser(StatisticsParser):
    """
    This class is responsible for parsing statistics of the market community
    """

    def __init__(self, node_directory):
        super(MarketStatisticsParser, self).__init__(node_directory)
        self.total_quantity_traded = 0
        self.total_ask_quantity = 0
        self.total_bid_quantity = 0
        self.avg_order_latency = 0
        self.total_trades = 0

    def aggregate_trade_data(self):
        """
        Aggregate all trade data during the experiment
        """
        trades_str = ""
        trades_cumulative_str = "0,0\n"
        trades_times = []

        for peer_nr, filename, dir in self.yield_files('trades.log'):
            trades = [line.rstrip('\n') for line in open(filename)]
            for trade in trades:
                parts = trade.split(',')
                self.total_quantity_traded += float(parts[2])
                trades_str += trade + '\n'
                trades_times.append(float(parts[0]))
                self.total_trades += 1

        trades_times = sorted(trades_times)
        total_trades = 0
        for trade_time in trades_times:
            total_trades += 1
            trades_cumulative_str += str(trade_time) + "," + str(total_trades) + "\n"

        with open('trades.log', 'w', 0) as trades_file:
            trades_file.write("time,price,quantity,peer1,peer2\n")
            trades_file.write(trades_str)

        with open('trades_cumulative.csv', 'w') as trades_file:
            trades_file.write("time,trades\n")
            trades_file.write(trades_cumulative_str)

    def aggregate_order_data(self):
        """
        Aggregate all data of the orders
        """
        orders_str = "time,id,peer,is_ask,completed,price,quantity,reserved_quantity,traded_quantity,completed_time\n"
        orders_data_all = ""

        for peer_nr, filename, dir in self.yield_files('orders.log'):
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

        for peer_nr, filename, dir in self.yield_files('market_stats.log'):
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
                          'total_trades': self.total_trades,
                          'avg_order_latency': self.avg_order_latency,
                          'total_ask_quantity': int(self.total_ask_quantity),
                          'total_bid_quantity': int(self.total_bid_quantity)}
            stats_file.write(json.dumps(stats_dict))

    def run(self):
        self.aggregate_trade_data()
        self.aggregate_order_data()
        self.aggregate_general_stats()


# cd to the output directory
os.chdir(os.environ['OUTPUT_DIR'])

parser = MarketStatisticsParser(sys.argv[1])
parser.run()
