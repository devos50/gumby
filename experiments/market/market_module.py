import json
import os
import random

from Tribler.community.market.core.order_manager import OrderManager
from Tribler.community.market.core.order_repository import MemoryOrderRepository
from Tribler.community.market.core.transaction_manager import TransactionManager
from Tribler.community.market.core.transaction_repository import MemoryTransactionRepository
from Tribler.community.market.wallet.tc_wallet import TrustchainWallet
from Tribler.community.triblerchain.community import TriblerChainCommunity
from Tribler.pyipv8.ipv8.peer import Peer
from Tribler.pyipv8.ipv8.peerdiscovery.discovery import EdgeWalk

from gumby.experiment import experiment_callback
from gumby.modules.community_experiment_module import IPv8OverlayExperimentModule
from gumby.modules.experiment_module import static_module


from Tribler.community.market.community import MarketCommunity
from Tribler.community.market.wallet.dummy_wallet import DummyWallet1, DummyWallet2


@static_module
class MarketModule(IPv8OverlayExperimentModule):
    """
    This module contains code to manage experiments with the market community.
    """

    def __init__(self, experiment):
        super(MarketModule, self).__init__(experiment, MarketCommunity)
        self.num_bids = 0
        self.num_asks = 0
        self.tc_community = None
        self.order_id_map = {}
        self.cancelled_orders = set()

    def on_id_received(self):
        super(MarketModule, self).on_id_received()
        self.tribler_config.set_dispersy_enabled(False)
        self.tribler_config.set_market_community_enabled(True)

        self.ipv8_community_launcher.community_kwargs["working_directory"] = u":memory:"

    @experiment_callback
    def init_trustchain(self):
        triblerchain_peer = Peer(self.session.trustchain_keypair)

        self.session.lm.triblerchain_community = TriblerChainCommunity(triblerchain_peer, self.ipv8.endpoint,
                                                                       self.ipv8.network,
                                                                       tribler_session=self.session,
                                                                       working_directory=self.session.config.get_state_dir())
        self.ipv8.overlays.append(self.session.lm.triblerchain_community)
        self.ipv8.strategies.append((EdgeWalk(self.session.lm.triblerchain_community), 20))

    @experiment_callback
    def init_wallets(self):
        dummy1_wallet = DummyWallet1()
        dummy2_wallet = DummyWallet2()
        self.overlay.use_local_address = True
        self.overlay.wallets = {
            dummy1_wallet.get_identifier(): dummy1_wallet,
            dummy2_wallet.get_identifier(): dummy2_wallet
        }

        dummy1_wallet.balance = 1000000000
        dummy2_wallet.balance = 1000000000
        dummy1_wallet.MONITOR_DELAY = 0
        dummy2_wallet.MONITOR_DELAY = 0

        if self.tc_community:
            tc_wallet = TrustchainWallet(self.tc_community)
            tc_wallet.check_negative_balance = False
            self.overlay.wallets[tc_wallet.get_identifier()] = tc_wallet

        # We use a memory repository in the market community
        self.overlay.order_manager = OrderManager(MemoryOrderRepository(self.overlay.mid))
        self.overlay.transaction_manager = TransactionManager(MemoryTransactionRepository(self.overlay.mid))

        # Disable incremental payments
        self.overlay.use_incremental_payments = False

        # Disable tick validation to improve performance
        self.overlay.validate_tick_signatures = False

    @experiment_callback
    def init_matchmakers(self):
        peer_num = self.experiment.scenario_runner._peernumber
        if peer_num > int(os.environ['NUM_MATCHMAKERS']):
            self.overlay.disable_matchmaker()

    @experiment_callback
    def connect_matchmakers(self, num_to_connect):
        num_total_matchmakers = int(os.environ['NUM_MATCHMAKERS'])
        if int(num_to_connect) > num_total_matchmakers:
            connect = range(1, num_total_matchmakers + 1)
        else:
            connect = random.sample(range(1, num_total_matchmakers + 1), int(num_to_connect))

        # Send introduction request to matchmakers
        for peer_num in connect:
            self._logger.info("Connecting to matchmaker %d", peer_num)
            self.overlay.walk_to(self.experiment.get_peer_ip_port_by_id(peer_num))

    def on_order_created(self, order, order_id):
        if order_id and not order_id in self.cancelled_orders:
            self.order_id_map[order_id] = order.order_id

    @experiment_callback
    def ask(self, price, price_type, quantity, quantity_type, order_id=None):
        self.num_asks += 1
        self.overlay.create_ask(float(price), price_type, float(quantity), quantity_type, 3600)\
            .addCallback(lambda order: self.on_order_created(order, order_id))

    @experiment_callback
    def bid(self, price, price_type, quantity, quantity_type, order_id=None):
        self.num_bids += 1
        self.overlay.create_bid(float(price), price_type, float(quantity), quantity_type, 3600)\
            .addCallback(lambda order: self.on_order_created(order, order_id))

    @experiment_callback
    def cancel(self, order_id):
        if order_id not in self.order_id_map:
            self._logger.warning("Want to cancel order but order id %s not found!", order_id)
            return

        self.cancelled_orders.add(order_id)
        self.overlay.cancel_order(self.order_id_map[order_id])

    @experiment_callback
    def compute_reputation(self):
        self.overlay.compute_reputation()

    @experiment_callback
    def write_stats(self):
        scenario_runner = self.experiment.scenario_runner
        transactions = []

        # Parse transactions
        for transaction in self.overlay.transaction_manager.find_all():
            partner_peer_id = self.overlay.lookup_ip(transaction.partner_order_id.trader_id)[1] - 12000
            if partner_peer_id < scenario_runner._peernumber:  # Only one peer writes the transaction
                transactions.append((float(transaction.timestamp) - scenario_runner._expstartstamp,
                                     float(transaction.price), float(transaction.total_quantity),
                                     len(transaction.payments), scenario_runner._peernumber, partner_peer_id))

        # Write transactions
        with open('transactions.log', 'w', 0) as transactions_file:
            for transaction in transactions:
                transactions_file.write("%s,%s,%s,%s,%s,%s\n" % transaction)

        # Write orders
        with open('orders.log', 'w', 0) as orders_file:
            for order in self.overlay.order_manager.order_repository.find_all():
                orders_file.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (float(order.timestamp),
                                                                       order.order_id,
                                                                       scenario_runner._peernumber,
                                                              'ask' if order.is_ask() else 'bid',
                                                              'complete' if order.is_complete() else 'incomplete',
                                                              float(order.price), float(order.total_quantity),
                                                              float(order.reserved_quantity), float(order.traded_quantity),
                                                              float(order.completed_timestamp) if order.is_complete() else '-1'))

        # Write ticks in order book
        with open('orderbook.txt', 'w', 0) as orderbook_file:
            orderbook_file.write(str(self.overlay.order_book))

        # Write known matchmakers
        with open('matchmakers.txt', 'w', 0) as matchmakers_file:
            for matchmaker in self.overlay.matchmakers:
                matchmakers_file.write("%s,%d\n" % (matchmaker.address[0], matchmaker.address[1]))

        # Write verified candidates
        with open('verified_candidates.txt', 'w', 0) as candidates_files:
            for peer in self.overlay.network.get_peers_for_service(self.overlay.master_peer.mid):
                if peer.address[1] > 15000:
                    continue
                candidates_files.write('%d\n' % (peer.address[1] - 12000))

        # TODO IPv8 has no bandwidth accounting
        #with open('bandwidth.txt', 'w', 0) as bandwidth_file:
        #    bandwidth_file.write("%s,%f" % (self.dispersy.statistics.total_up, self.dispersy.statistics.total_down))

        # Get statistics about the amount of fulfilled orders (asks/bids)
        fulfilled_asks = 0
        fulfilled_bids = 0
        for order in self.overlay.order_manager.order_repository.find_all():
            if order.is_complete():  # order is fulfilled
                if order.is_ask():
                    fulfilled_asks += 1
                else:
                    fulfilled_bids += 1

        with open('market_stats.log', 'w', 0) as stats_file:
            stats_dict = {'asks': self.num_asks, 'bids': self.num_bids,
                          'fulfilled_asks': fulfilled_asks, 'fulfilled_bids': fulfilled_bids}
            stats_file.write(json.dumps(stats_dict))

        # Write reputation
        with open('reputation.log', 'w', 0) as rep_file:
            for peer_id, reputation in self.overlay.reputation_dict.iteritems():
                rep_file.write("%s,%s\n" % (peer_id.encode('hex'), reputation))
