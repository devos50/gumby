#!/usr/bin/env python2
import json
import os
import sys

from gumby.statsparser import StatisticsParser

from Tribler.community.market.core.assetamount import AssetAmount
from Tribler.community.market.core.assetpair import AssetPair
from Tribler.community.market.core.matching_engine import MatchingEngine, PriceTimeStrategy
from Tribler.community.market.core.message import TraderId
from Tribler.community.market.core.order import OrderId
from Tribler.community.market.core.orderbook import OrderBook
from Tribler.community.market.core.tick import Ask, Bid
from Tribler.community.market.core.timeout import Timeout
from Tribler.community.market.core.timestamp import Timestamp


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
                trades_times.append(int(parts[0]))
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
        orders_str = "time,id,peer,type,status,asset1_amount,asset1_type,asset2_amount,asset2_type,reserved_quantity,traded_quantity,completed_time\n"
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
            if parts[4] == "completed":
                sum += float(parts[11]) - float(parts[0])
                amount += 1

            if parts[3] == "ask":
                self.total_ask_quantity += float(parts[5])
            else:
                self.total_bid_quantity += float(parts[5])

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

        with open('avg_latency.txt', 'w', 0) as latency_file:
            latency_file.write("%f" % self.avg_order_latency)

    def check_missed_matches(self):
        orders = []

        with open("orders.log") as orders_file:
            for line in orders_file.readlines()[1:]:  # First line = header
                parts = line.split(",")
                order_type = parts[3]
                order_status = parts[4]
                order_cls = Ask if order_type == "ask" else Bid

                if order_status == "cancelled" or order_status == "expired":
                    continue

                order_id_str = parts[1]
                order_id_parts = order_id_str.split(".")
                trader_id = TraderId(order_id_parts[0].decode('hex'))
                order_number = int(order_id_parts[1])
                order_id = OrderId(trader_id, order_number)
                asset_pair = AssetPair(AssetAmount(int(parts[5]), parts[6]), AssetAmount(int(parts[7]), parts[8]))
                timestamp = Timestamp(int(parts[0]))
                traded = int(parts[10])
                order = order_cls(order_id, asset_pair, Timeout(3600 * 24), timestamp, traded)
                orders.append(order)

        orders.sort(key=lambda order: order.timestamp)

        order_book = OrderBook()
        matching_engine = MatchingEngine(PriceTimeStrategy(order_book))

        missed = 0

        # Start inserting them
        for order in orders:
            if isinstance(order, Ask):
                order_book.insert_ask(order)
            else:
                order_book.insert_bid(order)

            entry = order_book.get_tick(order.order_id)
            matched_ticks = matching_engine.match(entry)
            if matched_ticks:
                missed += 1
                print "Found possible match of %s and %s!" % (order.order_id, matched_ticks[0].order_id)

        print("Asks in book: %d" % len(order_book.asks))
        print("Bids in book: %d" % len(order_book.bids))
        print("Missed: %d" % missed)

        with open("num_missed.txt", "w", 0) as missed_file:
            missed_file.write("%d" % missed)

    def parse_messages(self):
        messages_dict = {}
        num_messages_dict = {}
        for peer_nr, filename, dir in self.yield_files('messages.txt'):
            total_messages = 0
            with open(filename) as messages_file:
                for line in messages_file.readlines():
                    line_stripped = line.rstrip()
                    parts = line_stripped.split(",")
                    msg_name = parts[0]
                    msg_count = int(parts[1])
                    total_messages += msg_count
                    if msg_name not in messages_dict:
                        messages_dict[msg_name] = 0
                    messages_dict[msg_name] += msg_count
            num_messages_dict[peer_nr] = total_messages

        with open("messages_received.txt", "w", 0) as messages_out_file:
            for msg_name, msg_count in messages_dict.items():
                messages_out_file.write("%s,%d\n" % (msg_name, msg_count))

        with open("msg_received_per_peer.txt", "w", 0) as msg_file:
            for peer_nr, num_messages in num_messages_dict.iteritems():
                msg_file.write("%s,%d\n" % (peer_nr, num_messages))

    def run(self):
        self.aggregate_trade_data()
        self.aggregate_order_data()
        self.aggregate_general_stats()
        self.parse_messages()
        self.check_missed_matches()


# cd to the output directory
os.chdir(os.environ['OUTPUT_DIR'])

parser = MarketStatisticsParser(sys.argv[1])
parser.run()
