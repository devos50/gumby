#!/usr/bin/env python
import os
import sys

from gumby.statsparser import StatisticsParser


class HyperledgerStatisticsParser(StatisticsParser):
    """
    Parse TrustChain statistics after an experiment has been completed.
    """

    def __init__(self, node_directory):
        super(HyperledgerStatisticsParser, self).__init__(node_directory)

    def aggregate_transactions(self):
        avg_latency = 0
        total_transactions = 0
        confirmation_times = []
        with open("transactions.txt", "w") as transactions_file:
            transactions_file.write("peer_id,tx_id,submit_time,confirm_time,latency\n")
            for peer_nr, filename, dir in self.yield_files('transactions.txt'):
                with open(filename, "r") as individual_transactions_file:
                    content = individual_transactions_file.read()
                    for line in content.split("\n"):
                        if not line:
                            continue

                        parts = line.split(",")
                        if len(parts) != 4:
                            continue

                        block_nr = int(parts[0])
                        submit_time = int(parts[2])
                        confirm_time = int(parts[1])
                        tx_id = parts[3]

                        if confirm_time != -1 and block_nr != 1 and block_nr != 2:  # The first block contains chaincode instantiation
                            tx_latency = confirm_time - submit_time
                            confirmation_times.append(confirm_time)
                            avg_latency += tx_latency
                            total_transactions += 1
                        else:
                            tx_latency = -1

                        transactions_file.write(
                            "%d,%s,%d,%d,%d\n" % (peer_nr, tx_id, submit_time, confirm_time, tx_latency))

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

parser = HyperledgerStatisticsParser(sys.argv[1])
parser.run()
