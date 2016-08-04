#!/usr/bin/env python2

import logging
import sys
import os
import time
import shutil
from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.protocol import Protocol
from twisted.internet.task import LoopingCall
from twisted.web.client import Agent
from twisted.web.http_headers import Headers

from gumby.instrumentation import init_instrumentation
from gumby.scenario import ScenarioRunner

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', "tribler")))
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', "tribler", "twisted", "plugins")))

#sys.path.append(os.path.abspath('./tribler'))
#sys.path.append(os.path.abspath('./tribler/twisted/plugins'))

from tribler_plugin import TriblerServiceMaker

from Tribler.community.allchannel.community import AllChannelCommunity
from Tribler.community.search.community import SearchCommunity
from Tribler.community.tunnel.hidden_community import HiddenTunnelCommunity
from Tribler.Core.Session import Session
from Tribler.Core.simpledefs import dlstatus_strings, NTFY_CHANNEL, NTFY_DISCOVERED, NTFY_TORRENT, SIGNAL_SEARCH_COMMUNITY, SIGNAL_ON_SEARCH_RESULTS, DOWNLOAD, UPLOAD, NTFY_TORRENTS, NTFY_CHANNELCAST, NTFY_INSERT
from Tribler.Core.Utilities.search_utils import split_into_keywords
from Tribler.dispersy.discovery.community import DiscoveryCommunity


class StandaloneTriblerRunner(object):
    """
    This class starts a run of Tribler.
    """
    def __init__(self):
        init_instrumentation()
        self.service = None
        self.scenario_file = None
        self.general_stats = {}
        self.run_index = 0
        self.pending_searches = []
        self._logger = logging.getLogger(self.__class__.__name__)
        self.community_stats_file = open(os.path.join(os.environ['OUTPUT_DIR'], "community_stats.csv"), 'w')
        self.community_stats_file.write("Time,Search Communtiy,AllChannel Community,Tunnel Community,Discovery Community\n")
        self.discovered_stats_file = open(os.path.join(os.environ['OUTPUT_DIR'], "discovered_stats.csv"), 'w')
        self.discovered_stats_file.write("Time,Channels,Torrents\n")
        self.download_stats_file = open(os.path.join(os.environ['OUTPUT_DIR'], "download_stats.csv"), 'w')
        self.download_stats_file.write("Infohash,State,Progress,Download speed,Upload speed\n")
        self.local_search_stats_file = open(os.path.join(os.environ['OUTPUT_DIR'], "local_search_stats.txt"), 'w')
        self.local_search_stats_file.write("Time,Hits,Query,DB Time\n")
        self.metainfo_stats_file = open(os.path.join(os.environ['OUTPUT_DIR'], "metainfo_stats.csv"), 'w')
        self.metainfo_stats_file.write("Infohash,Time\n")

        self.tribler_session = None
        self.tribler_start_time = 0.0
        self.stats_lc = LoopingCall(self.write_stats)

        # Communities
        self.search_community = None
        self.discovery_community = None
        self.allchannel_community = None
        self.tunnel_community = None

    def start_experiment(self):
        """
        Start the experiment by parsing and running the scenario file.
        """
        self._logger.error("Starting experiment")
        self.scenario_file = os.environ.get('SCENARIO_FILE', 'tribler_minimal_run.scenario')
        scenario_file_path = os.path.join(os.environ['EXPERIMENT_DIR'], self.scenario_file)
        self.scenario_runner = ScenarioRunner(scenario_file_path)
        self.scenario_runner._read_scenario(scenario_file_path)

        self.scenario_runner.register(self.start_session)
        self.scenario_runner.register(self.stop_session)
        self.scenario_runner.register(self.clean_state_dir)
        self.scenario_runner.register(self.search_torrent)
        self.scenario_runner.register(self.local_search_torrent)
        self.scenario_runner.register(self.get_metainfo)
        self.scenario_runner.register(self.start_download)
        self.scenario_runner.register(self.subscribe)
        self.scenario_runner.register(self.stop)

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
        Write the gathered statistics
        """
        self.community_stats_file.write("%.2f,%d,%d,%d,%d\n" % (time.time() - self.tribler_start_time,
                                                                self.get_num_candidates(self.search_community),
                                                                self.get_num_candidates(self.allchannel_community),
                                                                self.get_num_candidates(self.tunnel_community),
                                                                self.get_num_candidates(self.discovery_community)))
        self.discovered_stats_file.write("%.2f,%d,%d\n" % (time.time() - self.tribler_start_time,
                                                           self.discovered_channels, self.discovered_torrents))

        # Check whether we should start pending searches
        items_to_remove = set()
        for query, min_peers in self.pending_searches:
            if self.get_num_candidates(self.search_community) >= min_peers:
                items_to_remove.add((query, min_peers))
                self.perform_torrent_search(query)

        for item in items_to_remove:
            self.pending_searches.remove(item)

    def write_general_stats(self):
        general_stats_file = open(os.path.join(os.environ['OUTPUT_DIR'], "general_stats.txt"), 'a')
        general_stats_file.write("---- run %d:\n" % self.run_index)
        for key, value in self.general_stats.iteritems():
            general_stats_file.write("%s %s\n" % (key, value))
        general_stats_file.close()

    def write_search_stats(self):
        search_stats_file = open(os.path.join(os.environ['OUTPUT_DIR'], "search_stats.txt"), 'a')
        for query, search_info in self.search_stats.iteritems():
            search_stats_file.write("---- run %d query %s:\n" % (self.run_index, query))
            for key, value in search_info.iteritems():
                search_stats_file.write("%s %s\n" % (key, value))
        search_stats_file.close()

    def write_sub_torrent_discover_stats(self):
        sub_torrent_discover_stats_file = open(os.path.join(os.environ['OUTPUT_DIR'], "sub_torrent_discover_stats.csv"), 'a')
        for cid, info in self.sub_torrent_discover_stats.iteritems():
            time_until_first_discovery = info['discover_time'] - info['start_time']
            sub_torrent_discover_stats_file.write("%s,%s\n" % (cid, time_until_first_discovery))
        sub_torrent_discover_stats_file.close()

    def start_session(self):
        """
        Start the Tribler session.
        """
        self.run_index += 1
        self._logger.error("Starting Tribler session")
        begin_time = time.time()
        self.service = TriblerServiceMaker()
        options = {"restapi": 5289 + self.run_index, "statedir": None, "dispersy": -1, "libtorrent": -1}
        self.service.start_tribler(options)
        self.tribler_start_time = time.time()
        self.general_stats['tribler_startup'] = self.tribler_start_time - begin_time
        self.tribler_session = self.service.session

        self.search_stats = {}
        self.sub_torrent_discover_stats = {}
        self.metainfo_requests = {}
        self.discovered_torrents = 0
        self.discovered_channels = 0

        self.tribler_session.set_download_states_callback(self.downloads_callback)
        self.tribler_session.add_observer(self.on_channel_discovered, NTFY_CHANNEL, [NTFY_DISCOVERED])
        self.tribler_session.add_observer(self.on_torrent_discovered, NTFY_TORRENT, [NTFY_DISCOVERED])
        self.tribler_session.add_observer(self.on_torrent_search_results, SIGNAL_SEARCH_COMMUNITY, SIGNAL_ON_SEARCH_RESULTS)
        #self.tribler_session.add_observer(self.on_metainfo, NTFY_METAINFO, NTFY_INSERT)
        #self.tribler_session.add_observer(self.on_metainfo_timeout, NTFY_METAINFO, NTFY_TIMEOUT)

        # Fetch the communities in the Tribler session
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

    def downloads_callback(self, download_states_list):
        for download_state in download_states_list:
            self.download_stats_file.write("%s,%s,%s,%s,%s\n" % (download_state.download.get_def().get_infohash().encode('hex'),
                                                                 dlstatus_strings[download_state.get_status()],
                                                                 download_state.get_progress() * 100,
                                                                 download_state.get_current_speed(DOWNLOAD),
                                                                 download_state.get_current_speed(UPLOAD)))

        return 1.0, []

    def on_torrent_search_results(self, subject, changetype, objectID, search_results):
        cur_time = time.time()
        query = ' '.join(search_results['keywords'])
        start_time_search = self.search_stats[query]['start_time']
        self.search_stats[query]['num_hits'] += len(search_results['results'])
        if self.search_stats[query]['time_first_response'] == -1 and len(search_results['results']) >= 1:
            self.search_stats[query]['time_first_response'] = cur_time - start_time_search
        self.search_stats[query]['time_last_response'] = cur_time - start_time_search

    def on_metainfo(self, subject, changetype, objectID, data):
        infohash = data['infohash']
        if infohash in self.metainfo_requests:
            start_time = self.metainfo_requests[infohash]['start_time']
            self.metainfo_stats_file.write("%s,%s\n" % (infohash, time.time() - start_time))

    def on_metainfo_timeout(self, subject, changetype, objectID, data):
        infohash = data['infohash']
        if infohash in self.metainfo_requests:
            self.metainfo_stats_file.write("%s,%s\n" % (infohash, -1))

    def on_channel_discovered(self, subject, changetype, objectID, *args):
        if self.discovered_channels == 0:
            self.general_stats['first_channel_discovered'] = time.time() - self.tribler_start_time
        self.discovered_channels += 1

    def on_torrent_discovered(self, subject, changetype, objectID, *args):
        if self.discovered_torrents == 0:
            self.general_stats['first_torrent_discovered'] = time.time() - self.tribler_start_time
        self.discovered_torrents += 1

        cid = args[0]['dispersy_cid']
        if cid in self.sub_torrent_discover_stats and self.sub_torrent_discover_stats[cid]['discover_time'] == -1:
            self.sub_torrent_discover_stats[cid]['discover_time'] = time.time()

    def stop_session(self):
        """
        Stop the Tribler session and write all statistics away
        """
        self.stats_lc.stop()
        self._logger.error("Stopping Tribler session")
        self.service.session.shutdown()
        Session.del_instance()

        self.write_general_stats()
        self.write_search_stats()
        self.write_sub_torrent_discover_stats()

    def clean_state_dir(self):
        shutil.rmtree(self.tribler_session.get_state_dir())

    def perform_torrent_search(self, query):
        self._logger.error("Starting remote torrent search with query %s" % query)
        self.search_stats[query] = {'num_hits': 0, 'time_first_response': -1, 'time_last_response': -1,
                                    'start_time': time.time()}
        keywords = split_into_keywords(unicode(query))
        self.tribler_session.search_remote_torrents(keywords)

    def search_torrent(self, query, min_peers=0):
        query = query.replace("_", " ")
        min_peers = int(min_peers)
        if min_peers == 0:
            self.perform_torrent_search(query)
        else:
            self.pending_searches.append((query, min_peers))

    def local_search_torrent(self, query):
        torrent_db = self.tribler_session.open_dbhandler(NTFY_TORRENTS)
        keywords = split_into_keywords(unicode(query))
        start_time = time.time()
        results = torrent_db.searchNames(keywords, keys=['infohash', 'T.name'], doSort=False)
        end_time = time.time()
        self.local_search_stats_file.write("%s,%d,%s,%s\n" % (end_time - start_time, len(results), query, torrent_db.local_search_times[query]))

    def get_metainfo(self, infohash):
        infohash = infohash.lower()
        self.metainfo_requests[infohash] = {'start_time': time.time()}
        self.tribler_session.download_torrentfile(infohash=infohash.decode('hex'), usercallback=None, prio=0)

    def start_download(self, uri, hops):
        self.tribler_session.start_download_from_uri(uri, hops)

    def subscribe_to_channel(self, cid):
        self.sub_torrent_discover_stats[cid.encode('hex')] = {'start_time': time.time(), 'discover_time': -1}
        self._logger.error("Subscribing to channel with cid %s" % cid.encode('hex'))
        for community in self.tribler_session.get_dispersy_instance().get_communities():
            if isinstance(community, AllChannelCommunity):
                community.disp_create_votecast(cid, 2, int(time.time()))
                break

    def subscribe(self, cid):
        if cid == "random":
            # Pick a random, popular channel to subscribe to
            channel_db = self.tribler_session.open_dbhandler(NTFY_CHANNELCAST)
            all_channels = [channel for channel in channel_db.getAllChannels() if not channel[7] == 2]
            self.subscribe_to_channel(all_channels[0][1])
        else:
            self.subscribe_to_channel(bytes(cid.decode('hex')))

    def stop(self):
        # Close files
        self.community_stats_file.close()
        self.discovered_stats_file.close()
        self.download_stats_file.close()
        self.local_search_stats_file.close()
        self.metainfo_stats_file.close()

        reactor.stop()

if __name__ == "__main__":
    runner = StandaloneTriblerRunner()
    reactor.callWhenRunning(runner.start_experiment)
    reactor.run()
