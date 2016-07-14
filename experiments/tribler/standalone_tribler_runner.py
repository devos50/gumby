#!/usr/bin/env python2

import logging
import sys
import os
import time
from twisted.internet import reactor
from twisted.internet.task import LoopingCall

from gumby.instrumentation import init_instrumentation
from gumby.scenario import ScenarioRunner

sys.path.append(os.path.abspath('./tribler'))
sys.path.append(os.path.abspath('./tribler/twisted/plugins'))

from tribler_plugin import TriblerServiceMaker

from Tribler.community.allchannel.community import AllChannelCommunity
from Tribler.community.search.community import SearchCommunity
from Tribler.community.tunnel.hidden_community import HiddenTunnelCommunity
from Tribler.Core.simpledefs import NTFY_CHANNEL, NTFY_DISCOVERED, NTFY_TORRENT
from Tribler.dispersy.discovery.community import DiscoveryCommunity


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
        self.community_stats_file.write("Time,Search Communtiy,AllChannel Community,Tunnel Community,Discovery Community\n")
        self.discovered_stats_file = open(os.path.join(os.environ['OUTPUT_DIR'], "discovered_stats.csv"), 'w')
        self.community_stats_file.write("Time,Channels,Torrents\n")

        self.tribler_session = None
        self.tribler_start_time = 0.0
        self.stats_lc = LoopingCall(self.write_stats)

        self.discovered_torrents = 0
        self.discovered_channels = 0

        # Communities
        self.search_community = None
        self.discovery_community = None
        self.allchannel_community = None
        self.tunnel_community = None

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

    def get_num_candidates(self, community):
        """
        Get the number of candidates in a specific community
        """
        if not community:
            return 0
        return len(community.candidates)

    def write_stats(self):
        """
        Write the gatherered statistics
        """
        self.community_stats_file.write("%.2f,%d,%d,%d,%d\n" % (time.time() - self.tribler_start_time,
                                                                self.get_num_candidates(self.search_community),
                                                                self.get_num_candidates(self.allchannel_community),
                                                                self.get_num_candidates(self.tunnel_community),
                                                                self.get_num_candidates(self.discovery_community)))
        self.discovered_stats_file.write("%.2f,%d,%d\n" % (time.time() - self.tribler_start_time,
                                                           self.discovered_channels, self.discovered_torrents))

    def start_session(self):
        """
        Start the Tribler session.
        """
        self._logger.info("Starting Tribler session")
        self.service = TriblerServiceMaker()
        options = {"restapi": 8085, "statedir": None, "dispersy": -1, "libtorrent": -1}
        self.service.start_tribler(options)
        self.tribler_start_time = time.time()
        self.tribler_session = self.service.session

        self.tribler_session.add_observer(self.on_channel_discovered, NTFY_CHANNEL, [NTFY_DISCOVERED])
        self.tribler_session.add_observer(self.on_torrent_discovered, NTFY_TORRENT, [NTFY_DISCOVERED])

        # Fetch the communities in the tribler session
        for community in self.tribler_session.get_dispersy_instance().get_communities():
            if isinstance(community, AllChannelCommunity):
                self.allchannel_community = community
            elif isinstance(community, SearchCommunity):
                self.search_community = community
            elif isinstance(community, HiddenTunnelCommunity):
                self.tunnel_community = community
            elif isinstance(community, DiscoveryCommunity):
                self.discovery_community = community

        self.stats_lc.start(1)

    def on_channel_discovered(self, subject, changetype, objectID, *args):
        self.discovered_channels += 1

    def on_torrent_discovered(self, subject, changetype, objectID, *args):
        self.discovered_torrents += 1

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
