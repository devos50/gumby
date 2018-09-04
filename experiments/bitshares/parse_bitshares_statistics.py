#!/usr/bin/env python3
import json
import os
import re
import sys

import time
from dateutil import parser as dateparser


class BitsharesStatisticsParser(object):
    """
    This class is responsible for parsing statistics of Bitshares experiment
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

    def aggregate_bandwidth(self):
        total_up, total_down = 0, 0
        for peer_nr, filename, dir in self.yield_files(file_to_check='bandwidth.txt'):
            with open(filename) as bandwidth_file:
                parts = bandwidth_file.read().rstrip('\n').split(",")
                total_up += int(parts[0])
                total_down += int(parts[1])

        with open('total_bandwidth.log', 'w') as taxi_file:
            taxi_file.write("%s,%s,%s\n" % (total_up, total_down, (total_up + total_down) / 2))

    def analyse_blockchain(self):
        blocks = []
        block_timestamp_map = {}
        signature_order_map = {}
        order_fulfill_block_map = {}

        block_throughputs = []  # List of (time, num_transactions)
        for peer_nr, filename, dir in self.yield_files(file_to_check='blockchain.txt'):
            with open(filename) as blockchain_file:
                lines = blockchain_file.readlines()
                cur_block_num = 1
                for line in lines:
                    if not line:
                        continue
                    block = json.loads(line)
                    blocks.append(block)
                    cur_block_num += 1

                    total_ops = 0
                    # Count number of operations (excluding transfers)
                    for transaction in block["transactions"]:
                        cur_ind = 0
                        for operation in transaction["operations"]:
                            if operation[0] != 0:
                                total_ops += 1

                                if operation[0] == 1:  # If we have a new order...
                                    signature_order_map[transaction["signatures"][cur_ind]] = transaction["operation_results"][cur_ind][1]

                            cur_ind += 1

                    timestamp = time.mktime(dateparser.parse(block["timestamp"]).timetuple())
                    block_throughputs.append((timestamp, total_ops))
                    print("THROUGHPUT: %f - %s" % (timestamp, total_ops))

                    # Store the timestamp of this block
                    block_timestamp_map[cur_block_num] = timestamp
            break

        # Parse the account histories of users and create a map that stores the order_id with the block that captures the fulfillment
        for peer_nr, filename, dir in self.yield_files(file_to_check='history.txt'):
            with open(filename) as history_file:
                content = history_file.read()
                history_json = json.loads(content)
                for history_item in history_json:
                    if 'fill_order_operation' in history_item["description"]:
                        order_id = history_item["op"]["op"][1]["order_id"]
                        fulfill_block_num = history_item["op"]["block_num"]
                        order_fulfill_block_map[order_id] = fulfill_block_num

        # Find the maximum throughput per second
        max_throughput = 0
        for cur_index in range(1, len(block_throughputs)):  # Compare with the prev block every time
            cur_block = block_throughputs[cur_index]
            prev_block = block_throughputs[cur_index - 1]
            time_interval = float(cur_block[0] - prev_block[0])  # Convert to seconds
            throughput = float(cur_block[1]) / float(time_interval)
            if throughput > max_throughput:
                max_throughput = throughput

        with open("throughput.txt", "w") as throughput_file:
            throughput_file.write("%f" % max_throughput)

        # Determine individual order fulfillment times
        order_fulfill_times = {}
        for peer_nr, filename, dir in self.yield_files(file_to_check='created_orders.txt'):
            with open(filename) as created_orders_file:
                lines = created_orders_file.readlines()
                for line in lines:
                    if not line:
                        continue
                    stripped_line = line.rstrip('\n')
                    parts = stripped_line.split(',')

                    order_create_time = int(parts[0])
                    order_signature = parts[1]
                    if order_signature not in signature_order_map:
                        continue
                    order_id = signature_order_map[order_signature]

                    # We have the ID of the order now and the time it was created, find the time it was fulfilled
                    if order_id in order_fulfill_block_map:
                        fulfill_block_num = order_fulfill_block_map[order_id]
                        if fulfill_block_num in block_timestamp_map:
                            order_fulfill_time = block_timestamp_map[fulfill_block_num] - order_create_time
                            order_fulfill_times[order_id] = order_fulfill_time

        with open("order_fulfill_times.txt", "w") as order_fulfill_file:
            order_fulfill_file.write("order_id,time\n")
            for order_id, order_time in order_fulfill_times.items():
                order_fulfill_file.write("%s,%s\n" % (order_id, order_time))

    def run(self):
        self.aggregate_bandwidth()
        self.analyse_blockchain()

# cd to the output directory
os.chdir(os.environ['OUTPUT_DIR'])

parser = BitsharesStatisticsParser(sys.argv[1])
parser.run()
