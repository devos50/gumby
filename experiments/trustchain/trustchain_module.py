import os
import time
from asyncio import get_event_loop
from binascii import hexlify
from random import choice, random
import csv

from ipv8.attestation.trustchain.community import TrustChainCommunity
from ipv8.attestation.trustchain.listener import BlockListener

from gumby.experiment import experiment_callback
from gumby.modules.experiment_module import static_module
from gumby.modules.transactions_module import TransactionsModule
from gumby.modules.community_experiment_module import IPv8OverlayExperimentModule
from gumby.util import run_task


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
        self.crawl_lc = None
        self.peers_to_crawl = []
        self.did_double_spend = False
        self.experiment.message_callback = self

    def on_message(self, from_id, msg_type, msg):
        self._logger.info("Received message with type %s from peer %d", msg_type, from_id)
        if msg_type == b"kill":
            self.die()

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
        return self.all_vars[peer_id]['public_key']

    @experiment_callback
    def init_trustchain(self):
        self.overlay.add_listener(FakeBlockListener(), [b'transfer'])
        if os.getenv('BROADCAST_FANOUT'):
            self._logger.error("Setting broadcast fanout to %s" % os.getenv('BROADCAST_FANOUT'))
            self.overlay.settings.broadcast_fanout = int(os.getenv('BROADCAST_FANOUT'))
        if os.getenv('SIGN_ATTEMPT_DELAY'):
            self._logger.error("Setting sign attempt delay to %s" % os.getenv('SIGN_ATTEMPT_DELAY'))
            self.overlay.settings.sign_attempt_delay = float(os.getenv('SIGN_ATTEMPT_DELAY'))
        if os.getenv('SHARE_INCONSISTENCIES'):
            self._logger.error("Setting 'share inconsistencies' to True")
            self.overlay.settings.share_inconsistencies = True
        if os.getenv('CRAWL_SEND_RANDOM_BLOCKS'):
            self._logger.error("Sending random blocks during crawl!")
            self.overlay.settings.crawl_send_random_blocks = True

        self.overlay.persistence.kill_callback = self.on_fraud_detected

    def on_fraud_detected(self):
        self._logger.error("Double spend detected!!")
        self.experiment.annotate("double-spend-detected")

        for str_client_id in self.all_vars.keys():
            client_id = int(str_client_id)
            if client_id == self.my_id:
                continue

            self.experiment.send_message(client_id, b"kill", b"")

        self.die()

    def die(self):
        # Write bandwidth statistics
        with open('bandwidth.txt', 'w') as bandwidth_file:
            bandwidth_file.write("%d,%d" % (self.session.ipv8.endpoint.bytes_up,
                                            self.session.ipv8.endpoint.bytes_down))

        # Write verified peers
        with open('verified_peers.txt', 'w') as peers_file:
            for peer in self.session.ipv8.network.verified_peers:
                peers_file.write('%d\n' % (peer.address[1] - 12000))

        # Write num verified peers
        with open('num_verified_peers.txt', 'w') as num_peers_file:
            num_peers_file.write('%d\n' % len(self.session.ipv8.network.verified_peers))

        get_event_loop().stop()

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
    def start_crawling(self):
        self._logger.info("Start crawling peers")

        # Reset bandwidth stats
        self.session.ipv8.endpoint.reset_statistics()

        for peer_id in self.all_vars.keys():
            self.peers_to_crawl.append(peer_id)
        self.peers_to_crawl.remove("%d" % self.experiment.scenario_runner._peernumber)

        if "CRAWL_INTERVAL" in os.environ:
            crawl_interval = float(os.environ["CRAWL_INTERVAL"])
        else:
            crawl_interval = 1

        rand_delay = random() * crawl_interval
        self._logger.info("Starting to crawl with interval %f", crawl_interval)
        run_task(self.crawl, delay=rand_delay, interval=crawl_interval)

    def crawl(self):
        """
        Forward crawl a random peer
        """
        peer_id = choice(self.peers_to_crawl)
        peer = self.get_peer(peer_id)

        latest_block = self.overlay.persistence.get_latest(peer.public_key.key_to_bin())
        if latest_block:
            start_seq = latest_block.sequence_number + 1
        else:
            start_seq = 1

        crawl_batch_size = int(os.environ["CRAWL_BATCH_SIZE"])
        end_seq = start_seq + crawl_batch_size
        self.overlay.send_crawl_request(peer, peer.public_key.key_to_bin(), start_seq, end_seq)
        self._logger.info("Crawling peer %s (%d - %d)", peer_id, start_seq, end_seq)

    @experiment_callback
    def request_crawl(self, peer_id, sequence_number):
        self._logger.info("%s: Requesting block: %s for peer: %s" % (self.my_id, sequence_number, peer_id))
        self.overlay.send_crawl_request(self.get_peer(peer_id),
                                        self.get_peer(peer_id).public_key.key_to_bin(),
                                        int(sequence_number))

    def transfer(self):
        latest_block = self.overlay.persistence.get_latest(self.overlay.my_peer.public_key.key_to_bin())

        verified_peers = list(self.overlay.network.verified_peers)
        if latest_block:
            verified_peers = [peer for peer in verified_peers if peer.public_key.key_to_bin() != latest_block.link_public_key]
        rand_peer = choice(verified_peers)
        peer_id = self.experiment.get_peer_id(rand_peer.address[0], rand_peer.address[1])
        transaction = {"tokens": 1 * 1024 * 1024, "from_peer": self.my_id, "to_peer": peer_id}

        # Should we double spend?
        if random() <= 0.1 and not self.did_double_spend and latest_block and latest_block.sequence_number > 1:
            self._logger.info("Double spending!")
            #self.experiment.annotate("double-spend")
            self.did_double_spend = True

            # Write it away
            with open("fraud_time.txt", "w") as out:
                hex_pk = hexlify(self.overlay.my_peer.public_key.key_to_bin()).decode()
                out.write("%s,%d" % (hex_pk, int(round(time.time() * 1000))))

            self.overlay.sign_block(rand_peer, rand_peer.public_key.key_to_bin(), block_type=b'transfer',
                                    transaction=transaction, double_spend=True)
        else:
            self.overlay.sign_block(rand_peer, rand_peer.public_key.key_to_bin(), block_type=b'transfer', transaction=transaction)

    @experiment_callback
    def commit_block_times(self):
        self._logger.error("Commit block times to the file %s", self.overlay.persistence.block_file)
        self.overlay.persistence.commit_block_times()

    @experiment_callback
    def commit_blocks_to_db(self):
        if self.session.config.use_trustchain_memory_db():
            self.overlay.persistence.commit(self.overlay.my_peer.public_key.key_to_bin())
