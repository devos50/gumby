import json
import os
import random
import time

from Tribler.community.market.community import MarketCommunity
from Tribler.community.market.core.assetamount import AssetAmount
from Tribler.community.market.core.assetpair import AssetPair
from Tribler.community.market.core.message import TraderId
from Tribler.pyipv8.ipv8.peer import Peer
from twisted.internet import reactor
from twisted.internet.task import LoopingCall

from gumby.experiment import experiment_callback
from gumby.modules.community_experiment_module import IPv8OverlayExperimentModule
from gumby.modules.experiment_module import static_module


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
        self.trade_lc = None
        self.create_ask = True  # Toggles between true/false when creating random orders

    def on_id_received(self):
        super(MarketModule, self).on_id_received()
        self.tribler_config.set_dht_enabled(True)
        self.tribler_config.set_market_community_enabled(True)

    def on_ipv8_available(self, _):
        # Disable threadpool messages
        self.overlay._use_main_thread = True

    @experiment_callback
    def init_matchmakers(self):
        peer_num = self.experiment.scenario_runner._peernumber
        if peer_num > int(os.environ['NUM_MATCHMAKERS']):
            self.overlay.disable_matchmaker()

    @experiment_callback
    def start_creating_orders(self, interval, asset1_amount, asset2_amount):
        """
        Start trading with random nodes
        """
        self._logger.info("Starting with random order creation on %s" % int(round(time.time() * 1000)))
        self.trade_lc = LoopingCall(self.create_random_order, asset1_amount, asset2_amount)
        self.trade_lc.start(int(interval))

    @experiment_callback
    def stop_creating_orders(self):
        """
        Stop trading with random nodes
        """
        self.trade_lc.stop()

    def create_random_order(self, asset1_amount, asset2_amount):
        if self.create_ask:
            self.ask(asset1_amount, "DUM1", asset2_amount, "DUM2")
        else:
            self.bid(asset1_amount, "DUM1", asset2_amount, "DUM2")
        self.create_ask = not self.create_ask

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
            reactor.callLater(random.random() * 9, self.overlay.walk_to, self.experiment.get_peer_ip_port_by_id(peer_num))

    @experiment_callback
    def disable_max_peers(self):
        self.overlay.max_peers = -1

    @experiment_callback
    def set_ttl(self, ttl):
        self.overlay.settings.ttl = int(ttl)

    @experiment_callback
    def set_fanout(self, fanout):
        self.overlay.settings.fanout = int(fanout)

    @experiment_callback
    def set_match_window(self, window_size):
        self.overlay.settings.match_window = int(window_size)

    @experiment_callback
    def fix_broadcast_set(self):
        rand_peers = random.sample(self.overlay.network.verified_peers,
                                   min(len(self.overlay.network.verified_peers), self.overlay.settings.fanout))
        self.overlay.fixed_broadcast_set = rand_peers

    @experiment_callback
    def init_trader_lookup_table(self):
        """
        Initialize the lookup table for all traders so we do not have to use the DHT.
        """
        for peer_id in self.all_vars.iterkeys():
            target = self.all_vars[peer_id]
            address = (str(target['host']), target['port'])
            peer = Peer(self.all_vars[peer_id]['public_key'].decode("base64"), address=address)
            self.overlay.update_ip(TraderId(peer.mid), address)

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
    def write_stats(self):
        scenario_runner = self.experiment.scenario_runner
        trades = []

        # Parse trades
        for trade in self.overlay.trading_engine.completed_trades:
            partner_peer_id = self.overlay.lookup_ip(trade.order_id.trader_id)[1] - 12000
            if partner_peer_id < scenario_runner._peernumber:  # Only one peer writes the transaction
                trades.append((int(trade.timestamp) / 1000.0 - scenario_runner.exp_start_time,
                                     trade.assets.first.amount,
                                     trade.assets.second.amount,
                                     scenario_runner._peernumber, partner_peer_id))

        # Write trades
        with open('trades.log', 'w', 0) as trades_file:
            for transaction in trades:
                trades_file.write("%s,%s,%s,%s,%s\n" % transaction)

        # Write orders
        with open('orders.log', 'w', 0) as orders_file:
            for order in self.overlay.order_manager.order_repository.find_all():
                order_data = (int(order.timestamp) / 1000.0, order.order_id, scenario_runner._peernumber,
                              'ask' if order.is_ask() else 'bid',
                              'complete' if order.is_complete() else 'incomplete',
                              order.assets.first.amount, order.assets.second.amount, order.reserved_quantity,
                              order.traded_quantity,
                              (int(order.completed_timestamp) / 1000.0) if order.is_complete() else '-1')
                orders_file.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" % order_data)

        # Write ticks in order book
        with open('orderbook.txt', 'w', 0) as orderbook_file:
            orderbook_file.write(str(self.overlay.order_book))

        # Write known matchmakers
        with open('matchmakers.txt', 'w', 0) as matchmakers_file:
            for matchmaker in self.overlay.matchmakers:
                matchmakers_file.write("%s,%d\n" % (matchmaker.address[0], matchmaker.address[1]))

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

        # Write mid register
        with open('mid_register.log', 'w', 0) as mid_file:
            for trader_id, host in self.overlay.mid_register.iteritems():
                mid_file.write("%s,%s\n" % (trader_id.as_hex(), "%s:%d" % host))
