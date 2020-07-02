import os
from random import choice
import csv

from ipv8.attestation.trustchain.community import TrustChainCommunity
from ipv8.attestation.trustchain.listener import BlockListener

from gumby.experiment import experiment_callback
from gumby.modules.experiment_module import static_module
from gumby.modules.transactions_module import TransactionsModule
from gumby.modules.community_experiment_module import IPv8OverlayExperimentModule


class FakeBlockListener(BlockListener):
    """
    Block listener that only signs blocks
    """

    def should_sign(self, _):
        return True

    def received_block(self, block):
        pass


@static_module
class TrustchainModule(IPv8OverlayExperimentModule):

    def __init__(self, experiment):
        super(TrustchainModule, self).__init__(experiment, TrustChainCommunity)
        self.num_blocks_in_db_task = None
        self.block_stat_file = None
        self.tx_rate = int(os.environ["TX_RATE"])
        self.transactions_manager = None
        self.block_stat_file = None

    def on_all_vars_received(self):
        super(TrustchainModule, self).on_all_vars_received()

        # Find the transactions manager and set it
        for module in self.experiment.experiment_modules:
            if isinstance(module, TransactionsModule):
                self._logger.info("Found transaction manager!")
                self.transactions_manager = module

        self.transactions_manager.transfer = self.transfer

    def get_peer_public_key(self, peer_id):
        # override the default implementation since we use the trustchain key here.
        return self.all_vars[peer_id]['trustchain_public_key']

    @experiment_callback
    def init_trustchain(self):
        self.overlay.add_listener(FakeBlockListener(), [b'transfer'])
        if os.getenv('BROADCAST_FANOUT'):
            self._logger.error("Setting broadcast fanout to %s" % os.getenv('BROADCAST_FANOUT'))
            self.overlay.settings.broadcast_fanout = int(os.getenv('BROADCAST_FANOUT'))
        if os.getenv('SIGN_ATTEMPT_DELAY'):
            self._logger.error("Setting sign attempt delay to %s" % os.getenv('SIGN_ATTEMPT_DELAY'))
            self.overlay.settings.sign_attempt_delay = float(os.getenv('SIGN_ATTEMPT_DELAY'))

    @experiment_callback
    def disable_broadcast(self):
        self.overlay.settings.broadcast_fanout = 0

    @experiment_callback
    def disable_max_peers(self):
        self.overlay.max_peers = -1

    @experiment_callback
    def enable_trustchain_memory_db(self):
        self.tribler_config.set_trustchain_memory_db(True)

    @experiment_callback
    def set_validation_range(self, value):
        self.overlay.settings.validation_range = int(value)

    @experiment_callback
    def enable_crawler(self):
        self.overlay.settings.crawler = True

    @experiment_callback
    def init_block_writer(self):
        # Open projects output directory and save blocks arrival time
        self.block_stat_file = 'blocks.csv'
        with open(self.block_stat_file, "w") as t_file:
            writer = csv.DictWriter(t_file, ['time', 'transaction', 'type', "seq_num", "link", 'from_id', 'to_id'])
            writer.writeheader()
        self.overlay.persistence.block_file = self.block_stat_file

    @experiment_callback
    def request_crawl(self, peer_id, sequence_number):
        self._logger.info("%s: Requesting block: %s for peer: %s" % (self.my_id, sequence_number, peer_id))
        self.overlay.send_crawl_request(self.get_peer(peer_id),
                                        self.get_peer(peer_id).public_key.key_to_bin(),
                                        int(sequence_number))

    def transfer(self):
        verified_peers = list(self.overlay.network.verified_peers)
        rand_peer = choice(verified_peers)
        peer_id = self.experiment.get_peer_id(rand_peer.address[0], rand_peer.address[1])
        transaction = {"tokens": 1 * 1024 * 1024, "from_peer": self.my_id, "to_peer": peer_id}
        self.overlay.sign_block(rand_peer, rand_peer.public_key.key_to_bin(), block_type=b'transfer', transaction=transaction)

    @experiment_callback
    def commit_block_times(self):
        self._logger.error("Commit block times to the file %s", self.overlay.persistence.block_file)
        self.overlay.persistence.commit_block_times()

    @experiment_callback
    def commit_blocks_to_db(self):
        if self.session.config.use_trustchain_memory_db():
            self.overlay.persistence.commit(self.overlay.my_peer.public_key.key_to_bin())
