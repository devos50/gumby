#!/usr/bin/env python2
import logging
import sys
import os
from twisted.internet import reactor

from gumby.instrumentation import init_instrumentation
from gumby.scenario import ScenarioRunner

sys.path.append(os.path.abspath('./tribler'))
sys.path.append(os.path.abspath('./tribler/twisted/plugins'))

from tribler_plugin import TriblerServiceMaker


class StandaloneTriblerRunner(object):
    """
    This class simply starts a run of Tribler.
    """
    def __init__(self):
        init_instrumentation()
        self.service = None
        self.scenario_file = None
        self._logger = logging.getLogger(self.__class__.__name__)

    def start_experiment(self):
        self._logger.info("Starting experiment")
        self.scenario_file = os.environ.get('SCENARIO_FILE', 'tribler_minimal_run.scenario')
        scenario_file_path = os.path.join(os.environ['EXPERIMENT_DIR'], self.scenario_file)
        self.scenario_runner = ScenarioRunner(scenario_file_path)
        self.scenario_runner._read_scenario(scenario_file_path)

        self.scenario_runner.register(self.start_session)
        self.scenario_runner.register(self.stop)

    def start_session(self):
        self._logger.info("Starting Tribler session")
        self.service = TriblerServiceMaker()
        options = {"restapi": 8085, "statedir": None, "dispersy": -1, "libtorrent": -1}
        self.service.start_tribler(options)

    def stop(self):
        self._logger.info("Stopping Tribler session")
        self.service.session.shutdown()
        reactor.stop()

if __name__ == "__main__":
    runner = StandaloneTriblerRunner()
    reactor.callWhenRunning(runner.start_experiment)
    reactor.run()
