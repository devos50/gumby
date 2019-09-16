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

    def run(self):
        pass


# cd to the output directory
os.chdir(os.environ['OUTPUT_DIR'])

parser = HyperledgerStatisticsParser(sys.argv[1])
parser.run()
