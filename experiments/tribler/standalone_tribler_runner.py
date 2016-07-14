#!/usr/bin/env python2

import logging
import sys
import os
from twisted.internet import reactor
from twisted.internet.task import LoopingCall

from gumby.instrumentation import init_instrumentation
from gumby.scenario import ScenarioRunner

sys.path.append(os.path.abspath('./tribler'))
sys.path.append(os.path.abspath('./tribler/twisted/plugins'))

from tribler_plugin import TriblerServiceMaker


class StandaloneTriblerRunner(object):
    """
    This class starts a run of Tribler.
    """
    def __init__(self):
        init_instrumentation()
        self.service = None
        self.scenario_file = None
        self._logger = logging.getLogger(self.__class__.__name__)
        self.community_stats_file = open(os.path.join(os.environ['OUTPUT_DIR'], "community_stats.csv"), 'w')
        self.tribler_session = None
        self.stats_lc = LoopingCall(self.write_stats)

    def start_experiment(self):
        """
        Start the experiment by parsing and running the scenario file.
        """
        self._logger.info("Starting experiment")
        self.scenario_file = os.environ.get('SCENARIO_FILE', 'tribler_minimal_run.scenario')
        scenario_file_path = os.path.join(os.environ['EXPERIMENT_DIR'], self.scenario_file)
        self.scenario_runner = ScenarioRunner(scenario_file_path)
        self.scenario_runner._read_scenario(scenario_file_path)

        self.scenario_runner.register(self.start_session)
        self.scenario_runner.register(self.stop_session)
        self.scenario_runner.parse_file()
        self.scenario_runner.run()

    def write_stats(self):
        """

        """
        pass

    def start_session(self):
        """
        Start the Tribler session.
        """
        self._logger.info("Starting Tribler session")
        self.service = TriblerServiceMaker()
        options = {"restapi": 8085, "statedir": None, "dispersy": -1, "libtorrent": -1}
        self.service.start_tribler(options)
        self.tribler_session = self.service.session

        # Fetch the communities in the tribler session
        for community in self.tribler_session.get_dispersy_instance().communities:
            print community

    def stop_session(self):
        """
        Stop the Tribler session and write all statistics away
        """
        self._logger.error("Stopping Tribler session")
        self.service.session.shutdown()
        reactor.stop()

        self.community_stats_file.close()

if __name__ == "__main__":
    runner = StandaloneTriblerRunner()
    reactor.callWhenRunning(runner.start_experiment)
    reactor.run()
