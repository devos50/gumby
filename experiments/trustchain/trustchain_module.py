import os
from random import randint, choice, random

import time
from Tribler.Core import permid
from Tribler.pyipv8.ipv8.attestation.trustchain.community import TrustChainCommunity
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks

from gumby.experiment import experiment_callback

from gumby.modules.experiment_module import static_module
from gumby.modules.community_experiment_module import IPv8OverlayExperimentModule

from twisted.internet.task import LoopingCall, deferLater


@static_module
class TrustchainModule(IPv8OverlayExperimentModule):
    def __init__(self, experiment):
        super(TrustchainModule, self).__init__(experiment, TrustChainCommunity)
        self.request_signatures_lc = LoopingCall(self.request_random_signature)
        self.peers_to_crawl = []
        self.crawl_start_time = None

    def on_id_received(self):
        super(TrustchainModule, self).on_id_received()
        self.tribler_config.set_dispersy_enabled(False)

        # We need the trustchain key at this point. However, the configured session is not started yet. So we generate
        # the keys here and place them in the correct place. When the session starts it will load these keys.
        trustchain_keypair = permid.generate_keypair_trustchain()
        trustchain_pairfilename = self.tribler_config.get_trustchain_permid_keypair_filename()
        permid.save_keypair_trustchain(trustchain_keypair, trustchain_pairfilename)
        permid.save_pub_key_trustchain(trustchain_keypair, "%s.pub" % trustchain_pairfilename)

        self.vars['trustchain_public_key'] = trustchain_keypair.pub().key_to_bin().encode("base64")

    def get_peer_public_key(self, peer_id):
        # override the default implementation since we use the trustchain key here.
        return self.all_vars[peer_id]['trustchain_public_key']

    @experiment_callback
    def start_requesting_signatures(self):
        self.request_signatures_lc.start(1)

    @experiment_callback
    def stop_requesting_signatures(self):
        self.request_signatures_lc.stop()

    @experiment_callback
    def request_signature(self, peer_id, up, down):
        self.request_signature_from_peer(self.get_peer(peer_id), up, down)

    @experiment_callback
    def request_crawl(self, peer_id, sequence_number):
        self._logger.info("%s: Requesting block: %s for peer: %s" % (self.my_id, sequence_number, peer_id))
        self.overlay.send_crawl_request(self.get_peer(peer_id),
                                        self.get_peer(peer_id).public_key.key_to_bin(),
                                        int(sequence_number))

    @experiment_callback
    def request_random_signature(self):
        """
        Request a random signature from one of your known verified peers
        """
        rand_up = randint(1, 1000)
        rand_down = randint(1, 1000)

        if not self.overlay.network.verified_peers:
            self._logger.warning("No verified peers to request random signature from!")
            return

        verified_peers = list(self.overlay.network.verified_peers)
        self.request_signature_from_peer(choice(verified_peers), rand_up * 1024 * 1024, rand_down * 1024 * 1024)

    def request_signature_from_peer(self, peer, up, down, double_spend=False):
        self._logger.info("%s: Requesting signature from peer: %s (double spend? %s)" % (self.my_id, peer, double_spend))
        transaction = {"up": up, "down": down}
        return self.overlay.sign_block(peer, peer.public_key.key_to_bin(), transaction, double_spend=double_spend)

    def get_next_peer_nr(self, nr):
        total_peers = len(self.all_vars.keys())
        next_nr = (nr + 1) % total_peers
        if next_nr == self.experiment.scenario_runner._peernumber - 1:
            # Advance it one more
            next_nr = (next_nr + 1) % total_peers
        return next_nr

    @experiment_callback
    def init_trustchain(self):
        self.overlay._use_main_thread = True

    @experiment_callback
    def create_blocks(self):
        """
        Create a number of blocks by initiating transactions with other peers in the network.
        """
        transactions_per_peer = int(os.environ['NUM_TX_IND'])

        @inlineCallbacks
        def create_blocks_inner():
            yield deferLater(reactor, random(), lambda: None)  # Wait a random interval
            total_peers = len(self.all_vars.keys())
            cur_peer = self.experiment.scenario_runner._peernumber % total_peers  # 0-based
            for ind in xrange(transactions_per_peer):
                peer = self.get_peer(str(cur_peer + 1))  # Since cur_peer is 0-based

                # Should we double spend?
                double_spend = False
                if self.experiment.scenario_runner._peernumber == len(self.all_vars.keys()) and ind == transactions_per_peer / 2:
                    double_spend = True

                yield self.request_signature_from_peer(peer, 10, 10, double_spend=double_spend)
                cur_peer = self.get_next_peer_nr(cur_peer)

        create_blocks_inner()

    @experiment_callback
    def crawl_blocks(self):
        for peer_id in self.all_vars.keys():
            self.peers_to_crawl.append(peer_id)
        self.peers_to_crawl.remove("%d" % self.experiment.scenario_runner._peernumber)

        self.crawl_start_time = int(round(time.time() * 1000))
        self.overlay.persistence.crawl_start_time = self.crawl_start_time

        @inlineCallbacks
        def crawl_blocks_inner():
            while self.peers_to_crawl:
                peer_id = choice(self.peers_to_crawl)
                self._logger.info("Will requests blocks of peer %s", peer_id)
                peer = self.get_peer(peer_id)
                yield self.overlay.send_crawl_request(peer, peer.public_key.key_to_bin(), sequence_number=0)
                self.peers_to_crawl.remove(peer_id)

        crawl_blocks_inner()

    @experiment_callback
    def write_stats(self):
        if self.crawl_start_time and self.overlay.persistence.double_spend_detection_time:
            interval = self.overlay.persistence.double_spend_detection_time - self.crawl_start_time
            with open("detection_time.txt", "w") as out:
                out.write("%d" % interval)
