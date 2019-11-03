#!/usr/bin/env python
import os
import sys

from gumby.statsparser import StatisticsParser


class StellarStatisticsParser(StatisticsParser):
    """
    Parse stellar statistics after an experiment has been completed.
    """

    def __init__(self, node_directory):
        super(StellarStatisticsParser, self).__init__(node_directory)

    def aggregate_transactions(self):
        avg_latency = 0
        total_transactions = 0
        confirmation_times = []

        # Note that we have two seperate files: one that specifies the submit times (created by each client) and
        # another one that has all the finalization times (created by validator 1).
        # We need to merge the information in these files first.
        tx_info = {}
        for peer_nr, filename, dir in self.yield_files('tx_submit_times.txt'):
            with open(filename, "r") as tx_submit_times_file:
                content = tx_submit_times_file.read()
                for line in content.split("\n"):
                    if not line:
                        continue

                    parts = line.split(",")
                    tx_id = parts[0]
                    submit_time = int(parts[1])
                    tx_info[tx_id] = (submit_time, -1)

        for peer_nr, filename, dir in self.yield_files('tx_finalized_times.txt'):
            with open(filename, "r") as tx_finalized_file:
                content = tx_finalized_file.read()
                for line in content.split("\n"):
                    if not line:
                        continue

                    parts = line.split(",")
                    tx_id = parts[0]
                    confirm_time = int(parts[1])

                    if tx_id not in tx_info:
                        print("Transaction with ID %s not made!" % tx_id)
                        continue

                    submit_time = tx_info[tx_id][0]
                    tx_info[tx_id] = (submit_time, confirm_time)
                    tx_latency = confirm_time - submit_time
                    confirmation_times.append(confirm_time)
                    avg_latency += tx_latency
                    total_transactions += 1

        with open("transactions.txt", "w") as transactions_file:
            transactions_file.write("tx_id,submit_time,confirm_time,latency\n")
            for tx_id, tx_info in tx_info.items():
                latency = -1
                if tx_info[1] != -1:
                    latency = tx_info[1] - tx_info[0]
                transactions_file.write("%s,%d,%d,%d\n" % (tx_id, tx_info[0], tx_info[1], latency))

        avg_latency /= total_transactions
        with open("latency.txt", "w") as latency_file:
            latency_file.write("%f" % avg_latency)

        confirmation_times = sorted(confirmation_times)

        # Aggregate times
        freq_map = {}
        for confirmation_time in confirmation_times:
            if confirmation_time not in freq_map:
                freq_map[confirmation_time] = 0
            freq_map[confirmation_time] += 1

        # Make tuples
        tup_list = []
        for confirmation_time, freq in freq_map.items():
            tup_list.append((confirmation_time, freq))

        tup_list = sorted(tup_list, key=lambda tup: tup[0])

        # Compute
        max_throughput = 0
        for ind in range(1, len(tup_list)):
            time_diff = (tup_list[ind][0] - tup_list[ind - 1][0]) / 1000
            tx_sec = tup_list[ind][1] / time_diff
            if tx_sec > max_throughput:
                max_throughput = tx_sec

        with open("throughput.txt", "w") as throughput_file:
            throughput_file.write("%d" % max_throughput)

    def run(self):
        self.aggregate_transactions()


# cd to the output directory
os.chdir(os.environ['OUTPUT_DIR'])

parser = StellarStatisticsParser(sys.argv[1])
parser.run()
