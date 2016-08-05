#!/usr/bin/env python2
import logging
import os
import random
import time
import sys
from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from gumby.instrumentation import init_instrumentation

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', "tribler")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', "tribler", "twisted", "plugins")))

from tribler_plugin import TriblerServiceMaker

from Tribler.community.search.community import SearchCommunity
from Tribler.Core.Session import Session
from Tribler.Core.simpledefs import SIGNAL_SEARCH_COMMUNITY, SIGNAL_ON_SEARCH_RESULTS


MIN_PEERS_SEARCH = 20


class VideoExperimentRunner(object):

    def __init__(self):
        init_instrumentation()
        self.service = None
        self.general_stats = {'num_search_hits': 0}
        self.experiment_start_time = 0
        self._logger = logging.getLogger(self.__class__.__name__)

        self.tribler_session = None
        self.tribler_start_time = 0.0
        self.search_community = None
        self.search_peers_lc = LoopingCall(self.check_peers_search)

        self.search_keywords = []

        # Read keyword file
        with open('gumby/experiments/tribler/popular_keywords.txt', 'r') as keyword_file:
            content = keyword_file.read()
            self.search_keywords = [keyword for keyword in content.split('\n') if len(keyword) > 0]

    def start_experiment(self):
        """
        Start the experiment by parsing and running the scenario file.
        """
        self._logger.error("Starting experiment")
        reactor.callLater(1, self.start_session)

    def stop_session(self):
        """
        Stop the Tribler session and write all statistics away
        """
        self._logger.error("Stopping Tribler session")
        self.service.session.shutdown()
        Session.del_instance()

        reactor.stop()

    def start_session(self):
        """
        Start the Tribler session.
        """
        self._logger.error("Starting Tribler session")
        self.experiment_start_time = time.time()
        self.service = TriblerServiceMaker()
        options = {"restapi": 5289, "statedir": None, "dispersy": -1, "libtorrent": -1}
        self.service.start_tribler(options)
        self.general_stats['tribler_startup'] = time.time() - self.experiment_start_time
        self.tribler_session = self.service.session

        # Fetch the communities in the Tribler session
        for community in self.tribler_session.get_dispersy_instance().get_communities():
            if isinstance(community, SearchCommunity):
                self.search_community = community

        self.tribler_session.add_observer(self.on_torrent_search_results, SIGNAL_SEARCH_COMMUNITY, SIGNAL_ON_SEARCH_RESULTS)
        self.search_peers_lc.start(0.5)

    def on_torrent_search_results(self, subject, changetype, objectID, search_results):
        cur_time = time.time()
        self.general_stats['num_search_hits'] += len(search_results['results'])
        if self.general_stats['search_first_response'] == -1 and len(search_results['results']) >= 1:
            self.general_stats['search_first_response'] = cur_time - self.experiment_start_time
        self.search_stats['search_last_response'] = cur_time - self.experiment_start_time

        # TODO check whether we can pick this torrent for downloading
        for result in search_results['results']:
            self._logger.error(result[4][0])

    def get_num_candidates(self, community):
        """
        Get the number of candidates in a specific community
        """
        if not community:
            return 0
        return len(community.candidates)

    def perform_remote_search(self):
        search_keyword = random.choice(self.search_keywords)
        self._logger.error("Searching for %s" % search_keyword)
        self.tribler_session.search_remote_torrents([search_keyword])
        reactor.callLater(30, self.stop_session)

    def check_peers_search(self):
        if self.get_num_candidates(self.search_community) >= MIN_PEERS_SEARCH:
            self.search_peers_lc.stop()
            self.general_stats['start_search'] = time.time() - self.experiment_start_time
            self._logger.error("Starting search")
            self.perform_remote_search()

if __name__ == "__main__":
    runner = VideoExperimentRunner()
    reactor.callWhenRunning(runner.start_experiment)
    reactor.run()
