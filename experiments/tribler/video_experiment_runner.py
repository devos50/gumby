#!/usr/bin/env python2
from binascii import hexlify
import logging
import os
import random
import time
import sys
from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from twisted.web.client import Agent, readBody
from twisted.web.http_headers import Headers
from gumby.instrumentation import init_instrumentation

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', "tribler")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', "tribler", "twisted", "plugins")))

from tribler_plugin import TriblerServiceMaker

from Tribler.community.search.community import SearchCommunity
from Tribler.Core.DownloadConfig import DownloadStartupConfig
from Tribler.Core.Session import Session
from Tribler.Core.TorrentDef import TorrentDef
from Tribler.Core.simpledefs import dlstatus_strings, DLSTATUS_DOWNLOADING, SIGNAL_SEARCH_COMMUNITY, SIGNAL_ON_SEARCH_RESULTS, DOWNLOAD, UPLOAD
from Tribler.Core.Utilities.network_utils import autodetect_socket_style, get_random_port
from Tribler.Core.Video.utils import videoextdefaults


MIN_PEERS_SEARCH = 30


class VideoExperimentRunner(object):

    def __init__(self):
        init_instrumentation()
        self.service = None
        self.general_stats = {'num_search_hits': 0, 'search_first_response': -1}
        self.experiment_start_time = 0
        self._logger = logging.getLogger(self.__class__.__name__)
        self.last_download_state = -1
        self.download_received_bytes = False

        self.tribler_session = None
        self.tribler_start_time = 0.0
        self.search_community = None
        self.search_peers_lc = LoopingCall(self.check_peers_search)
        self.received_torrent_info = False
        self.active_download = None
        self.largest_video_index = -1
        self.events_file = open('events_%s.txt' % hexlify(os.urandom(8)), 'w', 0)

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
        reactor.callLater(10, self.stop_session)

    def stop_session(self):
        """
        Stop the Tribler session and write all statistics away
        """
        self._logger.error("Stopping Tribler session")
        self.service.session.shutdown()
        Session.del_instance()

        reactor.stop()
        self.events_file.close()

    def write_event(self, name):
        self.events_file.write("%s %s\n" % (time.time() - self.experiment_start_time, name))

    def start_session(self):
        """
        Start the Tribler session.
        """
        self._logger.error("Starting Tribler session")
        self.experiment_start_time = time.time()
        self.service = TriblerServiceMaker()
        options = {"restapi": get_random_port(), "statedir": None, "dispersy": -1, "libtorrent": -1}
        self.service.start_tribler(options)
        self.general_stats['tribler_startup'] = time.time() - self.experiment_start_time
        self.write_event("tribler_startup")
        self.tribler_session = self.service.session
        print "State dir: %s" % self.tribler_session.get_state_dir()

        # Fetch the communities in the Tribler session
        for community in self.tribler_session.get_dispersy_instance().get_communities():
            if isinstance(community, SearchCommunity):
                self.search_community = community

        self.tribler_session.add_observer(self.on_torrent_search_results, SIGNAL_SEARCH_COMMUNITY, SIGNAL_ON_SEARCH_RESULTS)
        self.search_peers_lc.start(0.5)

    def on_torrent_search_results(self, subject, changetype, objectID, search_results):
        self.write_event("incoming_results")
        cur_time = time.time()
        self.general_stats['num_search_hits'] += len(search_results['results'])
        if self.general_stats['search_first_response'] == -1 and len(search_results['results']) >= 1:
            self.general_stats['search_first_response'] = cur_time - self.experiment_start_time
        self.general_stats['search_last_response'] = cur_time - self.experiment_start_time

        for result in search_results['results']:
            category = result[4][0]
            if category == 'Video':
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
        self.write_event("start_remote_search")
        self._logger.error("Searching for %s" % search_keyword)
        self.tribler_session.search_remote_torrents([unicode(search_keyword)])
        reactor.callLater(30, self.pick_torrents_to_fetch)

    def received_torrent_def(self, infohash):
        self.write_event("received_torrent_def_%s" % infohash)
        if self.received_torrent_info:
            # We already got another result
            return

        self.received_torrent_info = True
        tdef = TorrentDef.load_from_memory(self.tribler_session.lm.torrent_store.get(infohash))
        self._logger.error("Received tdef of infohash %s" % infohash)

        # Get largest video file
        video_files = tdef.get_files_as_unicode(exts=videoextdefaults)
        print video_files
        largest_file_name = sorted(video_files, key=lambda x: tdef.get_length(selectedfiles=[x]))[-1]
        self.largest_video_index = tdef.get_files_as_unicode().index(largest_file_name)
        print "Largest video file index: %d" % self.largest_video_index

        # Start the download
        dscfg = DownloadStartupConfig()
        dscfg.set_hops(1)
        self.active_download = self.tribler_session.start_download_from_tdef(tdef, dscfg)
        print "Destination: %s" % self.active_download.get_dest_dir()
        self.tribler_session.set_download_states_callback(self.downloads_callback)
        self.write_event("download_started_%s" % infohash)
        reactor.callLater(120, self.stop_session)

    def check_for_torrent(self):
        if not self.received_torrent_info:
            self.write_event("no_torrent_info_received")
            self.stop_session()

    def check_if_bytes_downloaded(self):
        if not self.download_received_bytes:
            self.write_event("no_bytes_downloaded")
            self.stop_session()

    def pick_torrents_to_fetch(self):
        self.write_event("picking_torrents_to_download")
        if len(self.potential_results) == 0:
            self._logger.error("No video results, aborting...")
            self.write_event("stopping_no_video_results")
            self.stop_session()
            return

        random_results = random.sample(self.potential_results, min(len(self.potential_results), 5))
        print random_results
        reactor.callLater(60, self.check_for_torrent)

        # TODO Download from other peers

        # Download from DHT
        for random_result in random_results:
            self.write_event("scheduling_dht_lookup_%s" % random_result[0].encode('hex'))
            self.tribler_session.download_torrentfile(random_result[0], self.received_torrent_def)

    def perform_video_request(self):
        video_server_port = self.tribler_session.get_videoserver_port()
        Agent(reactor).request(
            'GET', 'http://127.0.0.1:%s/%s/%s' % (video_server_port, self.active_download.tdef.get_infohash().encode('hex'), self.largest_video_index), Headers({'User-Agent': ['Tribler']}), None).addCallback(self.on_video_request).addCallback(self.on_video_response)

    def on_video_request(self, response):
        return readBody(response)

    def on_video_response(self, response):
        self.write_event("got_video_response")
        self.stop_session()

    def downloads_callback(self, download_states_list):
        for download_state in download_states_list:
            if self.last_download_state != DLSTATUS_DOWNLOADING and download_state.get_status() == DLSTATUS_DOWNLOADING:
                # Workaround for anon download that does not start for the first time
                self.write_event("circuits_ready")
                reactor.callLater(60, self.check_if_bytes_downloaded)
                download_state.download.force_recheck()
                self.perform_video_request()

            if not self.download_received_bytes and download_state.get_current_speed(DOWNLOAD) > 0:
                self.write_event("download_received_first_bytes")
                self.download_received_bytes = True

            print "%s,%s,%s,%s,%s\n" % (download_state.download.get_def().get_infohash().encode('hex'),
                                        dlstatus_strings[download_state.get_status()],
                                        download_state.get_progress() * 100,
                                        download_state.get_current_speed(DOWNLOAD),
                                        download_state.get_current_speed(UPLOAD))
            self.last_download_state = download_state.get_status()

        return 0.5, []

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
