import json
import os
import random
from base64 import b64decode

from anydex.core.community import MarketCommunity
from anydex.core.assetamount import AssetAmount
from anydex.core.assetpair import AssetPair
from anydex.core.message import TraderId
from anydex.wallet.dummy_wallet import DummyWallet1, DummyWallet2
from anydex.wallet.tc_wallet import TrustchainWallet

from gumby.experiment import experiment_callback
from gumby.modules.community_experiment_module import IPv8OverlayExperimentModule
from gumby.modules.experiment_module import static_module

from ipv8.peer import Peer


@static_module
class MarketModule(IPv8OverlayExperimentModule):
    """
    This module contains code to manage experiments with the market community.
    """

    def __init__(self, experiment):
        super(MarketModule, self).__init__(experiment, MarketCommunity)
        self.num_bids = 0
        self.num_asks = 0
        self.order_id_map = {}

    def on_ipv8_available(self, _):
        # Disable threadpool messages
        self.overlay._use_main_thread = True

        # Init settings according to the env variables
        if 'FANOUT' in os.environ:
            self._logger.info("Setting fanout to %d", int(os.environ['FANOUT']))
            self.overlay.settings.fanout = int(os.environ['FANOUT'])
        if 'MATCH_WINDOW' in os.environ:
            self.overlay.settings.match_window = float(os.environ['MATCH_WINDOW'])
            self._logger.info("Setting match window to %f", float(os.environ['MATCH_WINDOW']))
        if 'MATCH_SEND_INTERVAL' in os.environ:
            self.overlay.settings.match_send_interval = float(os.environ['MATCH_SEND_INTERVAL'])
            self._logger.info("Setting match send interval to %f", float(os.environ['MATCH_SEND_INTERVAL']))
        if 'SYNC_POLICY' in os.environ:
            self.overlay.set_sync_policy(int(os.environ['SYNC_POLICY']))
            self._logger.info("Setting sync policy to %d", int(os.environ['SYNC_POLICY']))
        if 'DISSEMINATION_POLICY' in os.environ:
            self.overlay.settings.dissemination_policy = int(os.environ['DISSEMINATION_POLICY'])
            self._logger.info("Setting dissemination policy to %d", int(os.environ['DISSEMINATION_POLICY']))
        if 'NUM_ORDER_SYNC' in os.environ:
            self.overlay.settings.num_order_sync = int(os.environ['NUM_ORDER_SYNC'])
            self._logger.info("Setting num order sync to %d", int(os.environ['NUM_ORDER_SYNC']))
        if 'SEND_FAIL_RATE' in os.environ:
            self.overlay.settings.send_fail_rate = float(os.environ['SEND_FAIL_RATE'])
            self._logger.info("Setting send fail rate to %f", float(os.environ['SEND_FAIL_RATE']))
        if 'MATCHMAKER_MALICIOUS_RATE' in os.environ:
            self.overlay.settings.matchmaker_malicious_rate = float(os.environ['MATCHMAKER_MALICIOUS_RATE'])
            self._logger.info("Setting matchmaker malicious rate to %f", float(os.environ['MATCHMAKER_MALICIOUS_RATE']))

    @experiment_callback
    def init_matchmakers(self):
        peer_num = self.experiment.scenario_runner._peernumber
        if peer_num > int(os.environ['NUM_MATCHMAKERS']):
            self.overlay.disable_matchmaker()

    @experiment_callback
    def disable_max_peers(self):
        self.overlay.max_peers = -1

    @experiment_callback
    def set_fanout(self, fanout):
        self.overlay.settings.fanout = int(fanout)

    @experiment_callback
    def set_match_window(self, window_size):
        self.overlay.settings.match_window = int(window_size)

    @experiment_callback
    def fix_broadcast_set(self):
        rand_peers = random.sample(self.overlay.matchmakers,
                                   min(len(self.overlay.matchmakers), self.overlay.settings.fanout))
        self.overlay.fixed_broadcast_set = rand_peers
        self._logger.info("Fixed broadcast set to %d peers:", len(rand_peers))
        for peer in rand_peers:
            self._logger.info("Will broadcast to peer: %s", str(peer))

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

    @experiment_callback
    def init_trader_lookup_table(self):
        """
        Initialize the lookup table for all traders so we do not have to use the DHT.
        """
        num_total_matchmakers = int(os.environ['NUM_MATCHMAKERS'])
        for peer_id in self.all_vars.keys():
            target = self.all_vars[peer_id]
            address = (str(target['host']), target['port'])

            if 'public_key' not in self.all_vars[peer_id]:
                self._logger.error("Could not find public key of peer %s!", peer_id)
            else:
                peer = Peer(b64decode(self.all_vars[peer_id]['public_key']), address=address)
                self.overlay.update_ip(TraderId(peer.mid), address)

                if int(peer_id) <= num_total_matchmakers:
                    self.overlay.matchmakers.add(peer)

    @experiment_callback
    def ask(self, asset1_amount, asset1_type, asset2_amount, asset2_type, order_id=None):
        self.num_asks += 1
        pair = AssetPair(AssetAmount(int(asset1_amount), asset1_type), AssetAmount(int(asset2_amount), asset2_type))
        order = self.overlay.create_ask(pair, 3600)
        if order_id:
            self.order_id_map[order_id] = order.order_id

    @experiment_callback
    def bid(self, asset1_amount, asset1_type, asset2_amount, asset2_type, order_id=None):
        self.num_bids += 1
        pair = AssetPair(AssetAmount(int(asset1_amount), asset1_type), AssetAmount(int(asset2_amount), asset2_type))
        order = self.overlay.create_bid(pair, 3600)
        if order_id:
            self.order_id_map[order_id] = order.order_id

    @experiment_callback
    def cancel(self, order_id):
        if order_id not in self.order_id_map:
            self._logger.warning("Want to cancel order but order id %s not found!", order_id)
            return

        self.overlay.cancel_order(self.order_id_map[order_id])

    @experiment_callback
    def initialize_latencies(self):
        """
        Initialize all latencies in IPv8.
        """
        self._logger.info("Initializing latencies...")
        latencies_file_path = "/home/pouwelse/latencies.txt"
        with open(latencies_file_path) as latencies_file:
            for latency_str in latencies_file.readlines():
                if latency_str:
                    self.overlay.endpoint.latencies.append(float(latency_str))

    @experiment_callback
    def write_stats(self):
        scenario_runner = self.experiment.scenario_runner
        trades = []

        # Parse trades
        for trade in self.overlay.trading_engine.completed_trades:
            partner_peer_id = self.overlay.lookup_ip(trade.order_id.trader_id)[1] - 12000
            if partner_peer_id < scenario_runner._peernumber:  # Only one peer writes the transaction
                trades.append((int(trade.timestamp) - int(scenario_runner.exp_start_time * 1000),
                               trade.assets.first.amount,
                               trade.assets.second.amount,
                               scenario_runner._peernumber, partner_peer_id))

        # Write trades
        with open('trades.log', 'w') as trades_file:
            for trade in trades:
                trades_file.write("%s,%s,%s,%s,%s\n" % trade)

        # Write orders
        with open('orders.log', 'w') as orders_file:
            for order in self.overlay.order_manager.order_repository.find_all():
                order_data = (int(order.timestamp), order.order_id, scenario_runner._peernumber,
                              'ask' if order.is_ask() else 'bid',
                              order.status,
                              order.assets.first.amount, order.assets.first.asset_id, order.assets.second.amount,
                              order.assets.second.asset_id, order.reserved_quantity,
                              order.traded_quantity,
                              int(order.completed_timestamp) if order.is_complete() else '-1')
                orders_file.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % order_data)

        # Write ticks in order book
        with open('orderbook.txt', 'w') as orderbook_file:
            orderbook_file.write(str(self.overlay.order_book))

        # Write known matchmakers
        with open('matchmakers.txt', 'w') as matchmakers_file:
            for matchmaker in self.overlay.matchmakers:
                matchmakers_file.write("%s,%d\n" % (matchmaker.address[0], matchmaker.address[1]))

        # Write items in the matching queue
        with open('match_queue.txt', 'w') as queue_file:
            for match_cache in self.overlay.get_match_caches():
                for retries, price, other_order_id in match_cache.queue.queue:
                    queue_file.write("%s,%d,%s,%s\n" % (match_cache.order.order_id, retries, price, other_order_id))

        # Write away the different messages
        with open('messages.txt', 'w') as messages_file:
            messages_file.write("cancel_orders,%d\n" % self.overlay.num_received_cancel_orders)
            messages_file.write("orders,%d\n" % self.overlay.num_received_orders)
            messages_file.write("match,%d\n" % self.overlay.num_received_match)
            messages_file.write("match_decline,%d\n" % self.overlay.num_received_match_decline)
            messages_file.write("propose_trade,%d\n" % self.overlay.num_received_proposed_trade)
            messages_file.write("decline_trade,%d\n" % self.overlay.num_received_declined_trade)
            messages_file.write("counter_trade,%d\n" % self.overlay.num_received_counter_trade)
            messages_file.write("complete_trade,%d\n" % self.overlay.num_received_complete_trade)

        # Get statistics about the amount of fulfilled orders (asks/bids)
        fulfilled_asks = 0
        fulfilled_bids = 0
        for order in self.overlay.order_manager.order_repository.find_all():
            if order.is_complete():  # order is fulfilled
                if order.is_ask():
                    fulfilled_asks += 1
                else:
                    fulfilled_bids += 1

        with open('market_stats.log', 'w') as stats_file:
            stats_dict = {'asks': self.num_asks, 'bids': self.num_bids,
                          'fulfilled_asks': fulfilled_asks, 'fulfilled_bids': fulfilled_bids}
            stats_file.write(json.dumps(stats_dict))

        # Write mid register
        with open('mid_register.log', 'w') as mid_file:
            for trader_id, host in self.overlay.mid_register.items():
                mid_file.write("%s,%s\n" % (trader_id.as_hex(), "%s:%d" % host))

        # Write bandwidth statistics
        with open('bandwidth.txt', 'w') as bandwidth_file:
            bandwidth_file.write("%d,%d" % (self.overlay.endpoint.bytes_up,
                                            self.overlay.endpoint.bytes_down))
