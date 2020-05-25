#!/usr/bin/env python
from __future__ import print_function

import csv
import json
import os
import statistics as np
import sys
from binascii import hexlify

from gumby.post_process_blockchain import BlockchainTransactionsParser

from scripts.trustchain_database_reader import GumbyDatabaseAggregator


class NoodleStatisticsParser(BlockchainTransactionsParser):
    """
    Parse Noodle statistics after an experiment has been completed.
    """

    def __init__(self, node_directory):
        super(NoodleStatisticsParser, self).__init__(node_directory)
        self.aggregator = GumbyDatabaseAggregator(os.path.join(os.environ['PROJECT_DIR'], 'output'))
        self.tx_propagation_info = {}  # Keep track of whether a transaction has been seen by the counterparty / confirmed by the initiator

    def aggregate_databases(self):
        aggregation_path = os.path.join(os.environ['PROJECT_DIR'], 'output', 'sqlite')
        if not os.path.exists(aggregation_path):
            os.makedirs(aggregation_path)

        self.aggregator.combine_databases()

    def parse_transactions(self):
        """
        Parse all transactions, based on the info in the blocks.
        """

        # First get all the peer IDs and build a map
        peer_map = {}  # peer id str -> peer id int
        for peer_nr, filename, dir in self.yield_files('overlays.txt'):
            with open(filename) as read_file:
                for line in read_file.readlines():
                    if not line:
                        continue

                    parts = line.split(",")
                    if parts[0] == "NoodleCommunity":
                        peer_id = parts[1][-8:]
                        peer_map[peer_id] = peer_nr
                        break

        # Assign the empty key to peer 0
        peer_map["30303030"] = 0

        tx_info = {}  # Keep track of the submit time and confirmation times for each transaction we see.
        tx_info_ds = {}
        claimed_per_peer = {}  # Used while detecting if a peer signed a double spend
        stopped_interactions_times = {}

        with open("blocks.csv", "w") as blocks_file:
            writer = csv.DictWriter(blocks_file, ['time', 'type', 'from_seq_num', 'to_seq_num', 'from_peer_id', 'to_peer_id', 'seen_by', 'transaction'])
            writer.writeheader()

            for peer_nr, filename, dir in self.yield_files('blocks.csv'):
                with open(filename) as read_file:
                    csv_reader = csv.reader(read_file)
                    first = True
                    for row in csv_reader:
                        if first:
                            first = False
                        else:
                            block_time = int(row[0])
                            transaction = row[1]
                            block_type = row[2]
                            from_seq_num = int(row[3])
                            to_seq_num = int(row[4])
                            from_peer_id = row[5]
                            to_peer_id = row[6]

                            if from_peer_id not in peer_map:
                                print("Peer %s not found in map!" % from_peer_id)
                                continue

                            if to_peer_id not in peer_map:
                                print("Peer %s not found in map!" % to_peer_id)
                                continue

                            from_peer_id = peer_map[from_peer_id]
                            to_peer_id = peer_map[to_peer_id]

                            writer.writerow({
                                "time": block_time,
                                'type': block_type,
                                'from_seq_num': from_seq_num,
                                'to_seq_num': to_seq_num,
                                'from_peer_id': from_peer_id,
                                'to_peer_id': to_peer_id,
                                'seen_by': peer_nr,
                                'transaction': transaction
                            })

                            if block_type == "spend":
                                tx_id = "%d.%d.%d" % (from_peer_id, to_peer_id, from_seq_num)
                                if tx_id not in tx_info:
                                    tx_info[tx_id] = [-1, -1]

                                # Check if this peer is making multiple spends with the same sequence number
                                if from_peer_id == peer_nr and (from_peer_id, from_seq_num) not in tx_info_ds:
                                    tx_info_ds[(from_peer_id, from_seq_num)] = block_time - self.avg_start_time
                                elif from_peer_id == peer_nr:
                                    print("Double spend with tx id %d - %d!" % (from_peer_id, from_seq_num))
                                    latest_spend = max(block_time - self.avg_start_time, tx_info_ds[(from_peer_id, from_seq_num)])
                                    # TODO finish

                                if tx_id not in self.tx_propagation_info:
                                    self.tx_propagation_info[tx_id] = [False, False]

                                if peer_nr == to_peer_id:
                                    self.tx_propagation_info[tx_id][0] = True

                                # Update the submit time
                                tx_info[tx_id][0] = block_time - self.avg_start_time
                            elif block_type == "claim" and to_peer_id == peer_nr:
                                tx_id = "%d.%d.%d" % (to_peer_id, from_peer_id, to_seq_num)
                                if tx_id not in tx_info:
                                    tx_info[tx_id] = [-1, -1]

                                if peer_nr == from_peer_id:  # If this claim is targeted to this peer...
                                    if from_peer_id not in claimed_per_peer:
                                        claimed_per_peer[from_peer_id] = set()

                                    if tx_id in claimed_per_peer[from_peer_id]:
                                        print("Double spend %s got through!" % tx_id)
                                    else:
                                        claimed_per_peer[from_peer_id].add(tx_id)

                                if tx_id not in self.tx_propagation_info:
                                    self.tx_propagation_info[tx_id] = [False, False]

                                self.tx_propagation_info[tx_id][1] = True

                                # Update the confirm time
                                tx_info[tx_id][1] = block_time - self.avg_start_time
                            elif block_type == "reject_interactions":
                                if peer_nr not in stopped_interactions_times:
                                    stopped_interactions_times[peer_nr] = {}

                                malicious_peer_id = transaction.strip().split(":")[1][2:-2]
                                stopped_interactions_times[peer_nr][malicious_peer_id] = block_time - self.avg_start_time

            for tx_id, individual_tx_info in tx_info.items():
                tx_latency = -1
                if individual_tx_info[0] != -1 and individual_tx_info[1] != -1:
                    tx_latency = individual_tx_info[1] - individual_tx_info[0]

                if individual_tx_info[0] >= 0:  # Do not include mint transactions or transactions created before the experiment starts
                    self.transactions.append((1, tx_id, individual_tx_info[0], individual_tx_info[1], tx_latency))

        print("Stopped interactions: %s" % stopped_interactions_times)

    def write_blocks_to_file(self):
        # First, determine the experiment start time
        start_time = 0
        for peer_nr, filename, dir in self.yield_files('start_time.txt'):
            with open(filename) as start_time_file:
                start_time = int(float(start_time_file.read()) * 1000)
                break

        print("Writing TrustChain blocks to file")
        # Prior to writing all blocks, we construct a map from peer ID to public key
        key_map = {}
        for peer_nr, filename, dir in self.yield_files('overlays.txt'):
            with open(filename) as overlays_file:
                content = overlays_file.readlines()
                for line in content:
                    if not line:
                        continue
                    parts = line.split(',')
                    if parts[0] == 'NoodleCommunity':
                        print("Mapping %s to peer %s" % (parts[1].rstrip(), peer_nr))
                        key_map[parts[1].rstrip()] = peer_nr

        interactions = []

        # Get all blocks
        blocks = self.aggregator.database.get_all_blocks()

        with open('trustchain.csv', 'w') as trustchain_file:
            # Write header
            trustchain_file.write(
                "peer;public_key;sequence_number;link_peer;link_public_key;"
                "link_sequence_number;previous_hash;signature;hash;type;time;time_since_start;tx\n"
            )

            # Write blocks
            for block in blocks:
                if hexlify(block.link_public_key) not in key_map:
                    link_peer = 0
                else:
                    link_peer = key_map[hexlify(block.link_public_key)]

                if hexlify(block.public_key) not in key_map:
                    print("Public key %s cannot be mapped to a peer!" % hexlify(block.public_key))
                    continue

                peer = key_map[hexlify(block.public_key)]
                trustchain_file.write(
                    "%s;%s;%s;%s;%s;%s;%s;%s;%s;%s;%d;%s;%s\n" % (
                        peer,
                        hexlify(block.public_key),
                        block.sequence_number,
                        link_peer,
                        hexlify(block.link_public_key),
                        block.link_sequence_number,
                        hexlify(block.previous_hash),
                        hexlify(block.signature),
                        hexlify(block.hash),
                        block.type,
                        block.timestamp,
                        block.timestamp - start_time,
                        json.dumps(block.transaction))
                )

                if (peer, link_peer) not in interactions and (link_peer, peer) not in interactions:
                    interactions.append((peer, link_peer))

        with open('trustchain_interactions.csv', 'w') as trustchain_interactions_file:
            trustchain_interactions_file.write("peer_a,peer_b\n")
            for peer_a, peer_b in interactions:
                trustchain_interactions_file.write("%d,%d\n" % (peer_a, peer_b))

    def write_perf_results(self):
        # Compute average throughput
        tx_spawn_duration = int(os.environ["TX_SPAWN_DURATION"])
        grace_period = float(os.environ["TX_GRACE_PERIOD"]) * 1000
        total_confirmed = 0
        total_unconfirmed = 0
        for transaction in self.transactions:
            if transaction[3] == -1:
                total_unconfirmed += 1

            if grace_period <= transaction[2] <= transaction[2] + grace_period:
                total_confirmed += 1

        # Compute tx propagation info
        num_spend_send_fail = 0
        num_claim_send_fail = 0
        for tx_id in self.tx_propagation_info:
            if not self.tx_propagation_info[tx_id][0]:
                num_spend_send_fail += 1
            if not self.tx_propagation_info[tx_id][1]:
                num_claim_send_fail += 1

        # Compute transactions / sec
        throughput_per_sec = {}
        for transaction in self.transactions:
            time_slot = transaction[2] // 1000
            if time_slot not in throughput_per_sec:
                throughput_per_sec[time_slot] = 0

            throughput_per_sec[time_slot] += 1

        # Write performance results in a file
        with open("perf_results.txt", 'w') as w_file:
            w_file.write("Total spend transactions: %d\n" % len(self.transactions))
            w_file.write("Total unconfirmed transactions: %d\n" % total_unconfirmed)
            w_file.write("Number of peers: %s\n" % os.environ["GUMBY_das4_instances_to_run"])

            if os.getenv('TX_RATE'):
                tx_rate = int(os.getenv('TX_RATE'))
                w_file.write("System transaction rate: %d\n" % tx_rate)

            w_file.write("\n")

            w_file.write("=== throughput ===\n")
            w_file.write("Peak throughput: %d\n" % max(throughput_per_sec.values()))
            w_file.write("Avg throughput: %f\n" % (total_confirmed / tx_spawn_duration))
            w_file.write("St dev throughput: %d\n" % np.stdev(throughput_per_sec.values()))
            w_file.write("Min throughput: %d\n" % min(throughput_per_sec.values()))
            w_file.write("\n")

            latencies = self.get_latencies()
            w_file.write("=== latency ===\n")
            w_file.write("Min tx latency: %f\n" % min(latencies))
            w_file.write("Mean tx latency: %f\n" % np.mean(latencies))
            w_file.write("St dev tx latency: %f\n" % np.stdev(latencies))
            w_file.write("Max tx latency: %f\n" % max(latencies))
            w_file.write("\n")

            w_file.write("=== network reliability ===\n")
            w_file.write("Spend transactions not seen by counterparty: %d\n" % num_spend_send_fail)
            w_file.write("Claim transactions not seen by counterparty: %d\n" % num_claim_send_fail)

    def aggregate_transfer_stats(self):
        print("Aggregating transfer starts...")
        total_transfers = 0
        completed_transfers = 0
        for peer_nr, filename, dir in self.yield_files('transfers.txt'):
            with open(filename) as transfers_file:
                parts = transfers_file.read().split(",")
                total_transfers += int(parts[0])
                completed_transfers += int(parts[1])

        with open("transfers.txt", "w") as transfers_file:
            transfers_file.write("%d,%d" % (total_transfers, completed_transfers))

    def run(self):
        self.parse()
        self.write_perf_results()
        self.aggregate_databases()
        self.write_blocks_to_file()
        self.aggregate_transfer_stats()


if __name__ == "__main__":
    # cd to the output directory
    # cd to the output directory
    os.chdir(os.environ['OUTPUT_DIR'])

    parser = NoodleStatisticsParser(sys.argv[1])
    parser.run()
