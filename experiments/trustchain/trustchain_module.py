from random import randint, choice
from time import time

from Tribler.Core import permid
from Tribler.pyipv8.ipv8.attestation.trustchain.community import TrustChainCommunity
from Tribler.pyipv8.ipv8.peerdiscovery.discovery import RandomWalk

from gumby.experiment import experiment_callback

from gumby.modules.experiment_module import static_module
from gumby.modules.community_experiment_module import IPv8OverlayExperimentModule

from twisted.internet.task import LoopingCall


@static_module
class TrustchainModule(IPv8OverlayExperimentModule):
    def __init__(self, experiment):
        super(TrustchainModule, self).__init__(experiment, TrustChainCommunity)
        self.crawler_history = []
        self.crawler_lc = LoopingCall(self.record_num_blocks_lc)
        self.crawler_started = 0
        self.request_signatures_lc = LoopingCall(self.request_random_signature)

    def on_id_received(self):
        super(TrustchainModule, self).on_id_received()
        self.tribler_config.set_dispersy_enabled(False)
        self.tribler_config.set_trustchain_enabled(True)

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
        self.request_signature_from_peer(self.get_peer(peer_id), int(up), int(down))

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

    def request_signature_from_peer(self, peer, up, down):
        self._logger.info("%s: Requesting signature from peer: %s" % (self.my_id, peer))
        transaction = {"up": up, "down": down}
        self.overlay.sign_block(peer, peer.public_key.key_to_bin(), transaction)

    def record_num_blocks_lc(self):
        if self.overlay:
            num_blocks = list(self.overlay.persistence.execute("SELECT COUNT(*) FROM blocks;"))[0][0]
            self.crawler_history.append((time() - self.crawler_started, num_blocks))

    @experiment_callback
    def write_crawler_stats(self):
        with open('crawler_blocks.txt', 'w', 0) as stats_file:
            stats_file.write("time,num_blocks\n")
            for stats in self.crawler_history:
                stats_file.write("%s,%d\n" % stats)

    @experiment_callback
    def start_crawler(self):
        peer = self.overlay.my_peer
        self.ipv8.unload_overlay(self.overlay)

        crawler_overlay = TrustChainCommunity(peer,
                                              self.ipv8.endpoint,
                                              self.ipv8.network,
                                              working_directory=self.session.config.get_state_dir())
        crawler_overlay.crawling = True
        self.ipv8.overlays.append(crawler_overlay)
        self.ipv8.strategies.append((RandomWalk(crawler_overlay), -1))
        self.crawler_started = time()
        self.crawler_lc.start(1)
