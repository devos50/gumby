import json
import os
import random
from math import radians, sin, asin, cos, sqrt

from Tribler.community.market.core.order_manager import OrderManager
from Tribler.community.market.core.order_repository import MemoryOrderRepository
from Tribler.community.market.core.transaction_manager import TransactionManager
from Tribler.community.market.core.transaction_repository import MemoryTransactionRepository
from Tribler.community.market.wallet.tc_wallet import TrustchainWallet
from Tribler.community.triblerchain.community import TriblerChainCommunity
from Tribler.pyipv8.ipv8.peer import Peer
from Tribler.pyipv8.ipv8.peerdiscovery.discovery import EdgeWalk
from twisted.internet import reactor

from gumby.experiment import experiment_callback
from gumby.modules.community_experiment_module import IPv8OverlayExperimentModule
from gumby.modules.experiment_module import static_module


from Tribler.community.market.community import MarketCommunity
from Tribler.community.market.wallet.dummy_wallet import DummyWallet1, DummyWallet2, TaxiWallet


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

    def haversine(self, lon1, lat1, lon2, lat2):
        """
        Calculate the great circle distance between two points
        on the earth (specified in decimal degrees)
        """
        # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

        # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        r = 6371  # Radius of earth in kilometers. Use 3956 for miles
        return c * r

    def on_id_received(self):
        super(MarketModule, self).on_id_received()
        self.tribler_config.set_dispersy_enabled(False)
        self.tribler_config.set_market_community_enabled(True)

        self.ipv8_community_launcher.community_kwargs["working_directory"] = u":memory:"
        self.ipv8_community_launcher.community_kwargs["use_database"] = False

    def on_dispersy_available(self, dispersy):
        # Disable threadpool messages
        self.overlay._use_main_thread = True

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
        taxi_wallet = TaxiWallet()
        self.overlay.use_local_address = True
        self.overlay.wallets = {
            dummy1_wallet.get_identifier(): dummy1_wallet,
            dummy2_wallet.get_identifier(): dummy2_wallet,
            taxi_wallet.get_identifier(): taxi_wallet,
        }

        dummy1_wallet.balance = 1000000000
        dummy2_wallet.balance = 1000000000
        taxi_wallet.balance = 10000000
        dummy1_wallet.MONITOR_DELAY = 0
        dummy2_wallet.MONITOR_DELAY = 0
        taxi_wallet.MONITOR_DELAY = 0

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

        if 'NUM_PREVIOUS_BLOCKS' in os.environ:
            self._logger.info("Setting required blocks to %d", int(os.environ['NUM_PREVIOUS_BLOCKS']))
            self.overlay.required_previous_blocks = int(os.environ['NUM_PREVIOUS_BLOCKS'])

        if 'BROADCAST_RANGE' in os.environ:
            self._logger.info("Setting broadcast range to %d", int(os.environ['BROADCAST_RANGE']))
            self.overlay.BROADCAST_FANOUT = int(os.environ['BROADCAST_RANGE'])

        if 'DEFAULT_TTL' in os.environ:
            self._logger.info("Setting default ttl to %d", int(os.environ['DEFAULT_TTL']))
            self.overlay.DEFAULT_TTL = int(os.environ['DEFAULT_TTL'])

    @experiment_callback
    def init_matchmakers(self):
        peer_num = self.experiment.scenario_runner._peernumber
        if peer_num > int(os.environ['NUM_MATCHMAKERS']):
            self.overlay.disable_matchmaker()

    @experiment_callback
    def connect_matchmakers(self, num_to_connect):
        # Seed the RNG
        #random.seed(self.experiment.scenario_runner._peernumber)

        num_total_matchmakers = int(os.environ['NUM_MATCHMAKERS'])
        if int(num_to_connect) > num_total_matchmakers:
            connect = range(1, num_total_matchmakers + 1)
        else:
            connect = random.sample(range(1, num_total_matchmakers + 1), int(num_to_connect))

        if len(connect) == 1:
            # If we only have one peer, spread the sending of the introduction requests a bit
            reactor.callLater(random.random() * 10, self.overlay.walk_to, self.experiment.get_peer_ip_port_by_id(connect[0]))
        else:
            # Send introduction request to matchmakers
            for peer_num in connect:
                self._logger.info("Connecting to matchmaker %d", peer_num)
                self.overlay.walk_to(self.experiment.get_peer_ip_port_by_id(peer_num))

    def on_order_created(self, order, order_id):
        if order_id and not order_id in self.cancelled_orders:
            self.order_id_map[order_id] = order.order_id

    @experiment_callback
    def ride_offer(self, latitude, longitude):
        self.num_asks += 1
        self.overlay.create_ride_offer(float(latitude), float(longitude), 1800)

    @experiment_callback
    def ride_request(self, latitude, longitude):
        self.num_bids += 1
        self.overlay.create_ride_request(float(latitude), float(longitude), 1800)

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
    def connect_to_closest_peers(self, num_to_connect):
        # Parse the distances file first
        peer_locations = {}
        peer_distances = []

        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(dir_path, 'taxi_experiment_550_500.dist'), 'r') as distances_file:
            for line in distances_file.readlines():
                parts = line.split(' ')
                peer_locations[int(parts[0])] = (float(parts[1]), float(parts[2]))

        # Get the x closest peers
        my_coords = peer_locations[self.experiment.scenario_runner._peernumber]
        for peer_id, coords in peer_locations.iteritems():
            if peer_id == self.experiment.scenario_runner._peernumber or peer_id > 550:
                continue

            dist = self.haversine(coords[0], coords[1], my_coords[0], my_coords[1])
            peer_distances.append((peer_id, dist))

        for peer_num, _ in sorted(peer_distances, key=lambda tup: tup[1])[:int(num_to_connect)]:
            # Connect to this peer
            self._logger.info("Connecting to peer %d", peer_num)
            self.overlay.walk_to(self.experiment.get_peer_ip_port_by_id(peer_num))

    @experiment_callback
    def write_stats(self):
        scenario_runner = self.experiment.scenario_runner
        transactions = []

        # Parse transactions
        for transaction in self.overlay.transaction_manager.find_all():
            partner_peer_id = self.overlay.lookup_ip(transaction.partner_order_id.trader_id)[1] - 12000
            if partner_peer_id < scenario_runner._peernumber:  # Only one peer writes the transaction
                transactions.append((float(transaction.timestamp) - scenario_runner._expstartstamp,
                                     transaction.latitude, transaction.longitude, float(transaction.total_quantity),
                                     len(transaction.payments), scenario_runner._peernumber, partner_peer_id))

        # Write taxi rides
        with open('taxi_rides.log', 'w', 0) as taxi_file:
            for transaction in self.overlay.transaction_manager.find_all():
                partner_peer_id = self.overlay.lookup_ip(transaction.partner_order_id.trader_id)[1] - 12000
                if partner_peer_id < scenario_runner._peernumber:  # Only one peer writes the transaction
                    order = self.overlay.order_manager.order_repository.find_by_id(transaction.order_id)
                    distance = self.overlay.haversine(order.latitude, order.longitude, transaction.latitude,
                                                      transaction.longitude)
                    taxi_file.write("%d,%d,%f,%f,%f,%f,%f\n" % (scenario_runner._peernumber, partner_peer_id, order.latitude, order.longitude, transaction.latitude, transaction.longitude, distance))

        # Write transactions
        with open('transactions.log', 'w', 0) as transactions_file:
            for transaction in transactions:
                transactions_file.write("%s,%s,%s,%s,%s,%s,%s\n" % transaction)

        # Write orders
        with open('orders.log', 'w', 0) as orders_file:
            for order in self.overlay.order_manager.order_repository.find_all():
                orders_file.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % (float(order.timestamp),
                                                                       order.order_id,
                                                                       scenario_runner._peernumber,
                                                              'ask' if order.is_ask() else 'bid',
                                                              'complete' if order.is_complete() else 'incomplete',
                                                              order.latitude, order.longitude, float(order.total_quantity),
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

        with open('bandwidth.txt', 'w', 0) as bandwidth_file:
            bandwidth_file.write("%s,%f" % (self.overlay.endpoint.sent_bytes, self.overlay.endpoint.received_bytes))

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
