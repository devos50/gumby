#!/usr/bin/env python2
from binascii import hexlify
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
from Tribler.Core.TorrentDef import TorrentDef
from Tribler.Core.simpledefs import dlstatus_strings, SIGNAL_SEARCH_COMMUNITY, SIGNAL_ON_SEARCH_RESULTS, DOWNLOAD, UPLOAD


MIN_PEERS_SEARCH = 30


class VideoExperimentRunner(object):

    def __init__(self):
        init_instrumentation()
        self.service = None
        self.general_stats = {'num_search_hits': 0, 'search_first_response': -1}
        self.experiment_start_time = 0
        self._logger = logging.getLogger(self.__class__.__name__)

        self.tribler_session = None
        self.tribler_start_time = 0.0
        self.search_community = None
        self.search_peers_lc = LoopingCall(self.check_peers_search)

        self.search_keywords = []
        self.potential_results = []

        print "Working directory: %s" % os.getcwdu()

        # Read keyword file
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'popular_keywords.txt'), 'r') as keyword_file:
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
        options = {"restapi": 8085, "statedir": None, "dispersy": -1, "libtorrent": -1}
        self.service.start_tribler(options)
        self.general_stats['tribler_startup'] = time.time() - self.experiment_start_time
        self.tribler_session = self.service.session
        print "State dir: %s" % self.tribler_session.get_state_dir()

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
        self.general_stats['search_last_response'] = cur_time - self.experiment_start_time

        for result in search_results['results']:
            category = result[4][0]
            if category == 'Video' or category == 'xxx':
                self.potential_results.append(result)

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
        self.tribler_session.search_remote_torrents([unicode(search_keyword)])
        reactor.callLater(30, self.pick_torrent_to_download)

    def received_torrent_def(self, _, infohash):
        tdef = TorrentDef.load_from_memory(self.tribler_session.lm.torrent_store.get(infohash))
        print tdef
        self.stop_session()

    def pick_torrent_to_download(self):
        if len(self.potential_results) == 0:
            self._logger.error("No video results, aborting...")
            self.stop_session()
            return

        random_result = random.choice(self.potential_results)

        # Download from other peers
        print random_result

        # Download from DHT
        self.tribler_session.download_torrentfile(random_result[0], self.received_torrent_def)

        #magnetlink = "magnet:?xt=urn:btih:" + hexlify(random_result[0])
        #self.tribler_session.start_download_from_uri(magnetlink)
        #self.tribler_session.set_download_states_callback(self.downloads_callback)
        #reactor.callLater(120, self.stop_session)

    def downloads_callback(self, download_states_list):
        for download_state in download_states_list:
            print "%s,%s,%s,%s,%s\n" % (download_state.download.get_def().get_infohash().encode('hex'),
                                        dlstatus_strings[download_state.get_status()],
                                        download_state.get_progress() * 100,
                                        download_state.get_current_speed(DOWNLOAD),
                                        download_state.get_current_speed(UPLOAD))

        return 1.0, []

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
