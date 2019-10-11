#!/usr/bin/env python
import os
import sys

from gumby.statsparser import StatisticsParser


class LibraStatisticsParser(StatisticsParser):
    """
    Parse TrustChain statistics after an experiment has been completed.
    """

    def __init__(self, node_directory):
        super(LibraStatisticsParser, self).__init__(node_directory)

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
                        tx_id = int(parts[0])
                        submit_time = int(parts[1])
                        confirm_time = int(parts[2])

                        if confirm_time != -1:
                            tx_latency = confirm_time - submit_time
                            confirmation_times.append(confirm_time)
                            avg_latency += tx_latency
                            total_transactions += 1
                        else:
                            tx_latency = -1

                        transactions_file.write("%d,%d,%d,%d,%d\n" % (peer_nr, tx_id, submit_time, confirm_time, tx_latency))

        avg_latency /= total_transactions
        with open("latency.txt", "w") as latency_file:
            latency_file.write("%f" % avg_latency)

        confirmation_times = sorted(confirmation_times)

        # Determine max throughput per second
        max_throughput = -1
        for index in range(len(confirmation_times)):
            start_index = index
            current_index = index
            while current_index < len(confirmation_times) and (confirmation_times[current_index] - confirmation_times[start_index]) < 1000:
                current_index += 1

            cur_throughput = current_index - index
            if cur_throughput > max_throughput:
                max_throughput = cur_throughput

        with open("throughput.txt", "w") as throughput_file:
            throughput_file.write("%d" % max_throughput)

    def run(self):
        self.aggregate_transactions()


# cd to the output directory
os.chdir(os.environ['OUTPUT_DIR'])

parser = LibraStatisticsParser(sys.argv[1])
parser.run()
