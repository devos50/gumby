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

        self.crawl_lc = None
        self.transact_lc = LoopingCall(self.do_transact)
        self.has_first_tx = False
        self.has_attacked = False

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

    def do_transact(self):
        total_peers = len(self.all_vars.keys())
        rand_peer_id = randint(1, total_peers)
        self._logger.info("Will do transaction with peer %d", rand_peer_id)
        peer = self.get_peer(str(rand_peer_id))  # Since cur_peer is 0-based

        # Should we double spend?
        double_spend = False
        if self.experiment.scenario_runner._peernumber == len(
                self.all_vars.keys()) and self.has_first_tx and random() <= 0.2 and not self.has_attacked:
            self._logger.info("Doing double spend!")
            double_spend = True
            self.has_attacked = True

            # Write it away
            with open("fraud_time.txt", "w") as out:
                out.write("%d" % int(round(time.time() * 1000)))

        self.request_signature_from_peer(peer, 10, 10, double_spend=double_spend)
        self.has_first_tx = True

    @experiment_callback
    def start_transactions(self, tx_rate):
        tx_rate = float(tx_rate)
        total_peers = len(self.all_vars.keys())

        def do_crawl():
            rand_peer_id = randint(1, total_peers)
            peer = self.get_peer(str(rand_peer_id))

            # Find the lowest unknown sequence number
            cur_seq = 1
            while True:
                if (peer.public_key.key_to_bin(), cur_seq) not in self.overlay.persistence.block_cache:
                    break
                cur_seq += 1

            self._logger.info("Will requests blocks of peer %s (seq: %d)", rand_peer_id, cur_seq)
            self.overlay.send_crawl_request(peer, peer.public_key.key_to_bin(), sequence_number=cur_seq)

        # Start crawling
        self.crawl_lc = LoopingCall(do_crawl)
        crawl_interval = int(os.environ['CRAWL_FREQ'])
        self.crawl_lc.start(1.0 / float(crawl_interval))

        # Start making transactions
        tx_interval = float(total_peers) / tx_rate
        self._logger.info("Tx interval: %f", tx_interval)

        def start_transact():
            self._logger.info("Starting to transact with interval %f", tx_interval)
            self.transact_lc.start(tx_interval)

        # Wait a small period to align transaction creation
        wait_period = tx_interval / float(total_peers) * self.experiment.scenario_runner._peernumber
        self._logger.info("Waiting for %f seconds before transacting...", wait_period)
        deferLater(reactor, wait_period, start_transact)

    @experiment_callback
    def init_trustchain(self):
        self.overlay._use_main_thread = True

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
