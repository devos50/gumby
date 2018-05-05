import hashlib
import json
import os
import random
import subprocess

import axolotl_curve25519 as curve
import base58
import pyblake2
import requests

import time

import sha3
import signal
from twisted.internet import reactor
from twisted.internet.task import LoopingCall
import pywaves

from gumby.experiment import experiment_callback
from gumby.modules.experiment_module import static_module, ExperimentModule


@static_module
class WavesModule(ExperimentModule):

    def __init__(self, experiment):
        super(WavesModule, self).__init__(experiment)
        self.asset_id_map = {}
        self.total_miners = int(os.environ["NUM_MINERS"])
        self.trade_lc = LoopingCall(self.create_random_order)
        self.create_ask = True

        # Each node picks a random matcher at the beginning
        self.picked_matcher_num = random.choice(range(1, self.total_miners + 1))
        self.matcher_account_info = self.generate_keys_from_seed("foo%d" % (self.picked_matcher_num - 1))

        self.start_network_port = 23000
        self.start_matcher_port = 25000
        self.start_restapi_port = 27000

        self.order_id_map = {}
        self.created_orders = []
        self.cancelled_orders = set()
        self.account_info = None
        self.waves_process = None

    def generate_keys_from_seed(self, seed_text):
        def hashChain(noncedSecret):
            b = pyblake2.blake2b(noncedSecret, digest_size=32).digest()
            return sha3.keccak_256(b).digest()

        seedHash = hashChain('\0\0\0\0' + seed_text)
        accountSeedHash = hashlib.sha256(seedHash).digest()

        private_key = curve.generatePrivateKey(accountSeedHash)
        public_key = curve.generatePublicKey(private_key)

        unhashedAddress = chr(1) + "L" + hashChain(public_key)[0:20]
        addressHash = hashChain(unhashedAddress)[0:4]
        address = base58.b58encode(unhashedAddress + addressHash)

        return {
            'pub_key': base58.b58encode(public_key),
            'priv_key': base58.b58encode(private_key),
            'address': address
        }

    def on_id_received(self):
        super(WavesModule, self).on_id_received()
        self.create_ask = (self.experiment.scenario_runner._peernumber % 2 == 0)

    @experiment_callback
    def start_waves(self):
        # First, we create a configuration file out of the template configuration
        with open("/home/pouwelse/waves-devnet-template.conf", "r") as template_conf_file:
            template_content = template_conf_file.read()

        # Set all variables right
        template_content = template_content.replace("<WAVES_DATA_DIRECTORY>", os.path.join(os.getcwd(), "blockchain_data"))
        template_content = template_content.replace("<WAVES_NETWORK_PORT>", "%d" % (self.start_network_port + self.experiment.scenario_runner._peernumber))
        template_content = template_content.replace("<WAVES_NODE_NAME>", "Waves node %d" % self.experiment.scenario_runner._peernumber)
        template_content = template_content.replace("<WAVES_MATCHER_PORT>", "%d" % (self.start_matcher_port + self.experiment.scenario_runner._peernumber))
        template_content = template_content.replace("<WAVES_RESTAPI_PORT>", "%d" % (self.start_restapi_port + self.experiment.scenario_runner._peernumber))
        if self.experiment.scenario_runner._peernumber == 1:
            known_peers = "[]"
            template_content = template_content.replace("<WAVES_INBOUND_CONNECTIONS>", "2000")
            template_content = template_content.replace("<WAVES_OUTBOUND_CONNECTIONS>", "2000")
        else:
            peer_1 = self.experiment.get_peer_ip_port_by_id(1)
            known_peers = "[\"%s:%d\"]" % (peer_1[0], self.start_network_port + 1)
            template_content = template_content.replace("<WAVES_INBOUND_CONNECTIONS>", "20")
            template_content = template_content.replace("<WAVES_OUTBOUND_CONNECTIONS>", "20")

        wallet_text_seed = ("foo%d" % (self.experiment.scenario_runner._peernumber - 1))
        wallet_seed = "seed = \"%s\"" % base58.b58encode(wallet_text_seed)
        template_content = template_content.replace("<WAVES_WALLET_SEED>", wallet_seed)
        template_content = template_content.replace("<WAVES_KNOWN_PEERS>", known_peers)

        self.account_info = self.generate_keys_from_seed(wallet_text_seed)

        # Become miner?
        if self.experiment.scenario_runner._peernumber <= self.total_miners:
            template_content = template_content.replace("<WAVES_MINER_ENABLED>", "yes")
            template_content = template_content.replace("<WAVES_MATCHER_ACCOUNT>", self.account_info['address'])
        else:
            template_content = template_content.replace("<WAVES_MINER_ENABLED>", "no")

        peer_address = self.experiment.get_peer_ip_port_by_id(self.experiment.scenario_runner._peernumber)
        template_content = template_content.replace("<WAVES_DECLARED_ADDRESS>", "%s:%d" % (peer_address[0], self.start_network_port + self.experiment.scenario_runner._peernumber))

        with open("waves-devnet.conf", "w") as conf_file:
            conf_file.write(template_content)

        output_file = open("waves_output.log", "w")
        self.waves_process = subprocess.Popen(['java', '-jar', '/home/pouwelse/Waves/waves-all-0.10.3.jar', 'waves-devnet.conf'], stdout=output_file)

        # Initialize PyWaves
        network_port = self.start_restapi_port + self.experiment.scenario_runner._peernumber
        pywaves.setNode("http://localhost:%d" % network_port, 'mychain', 'L')

        matcher_peer = self.experiment.get_peer_ip_port_by_id(self.picked_matcher_num)
        matcher_url = "http://%s:%d" % (matcher_peer[0], self.start_matcher_port + self.picked_matcher_num)
        pywaves.MATCHER = matcher_url
        pywaves.MATCHER_PUBLICKEY = self.matcher_account_info['pub_key']

    @experiment_callback
    def stop_waves(self):
        pid = self.waves_process.pid
        print "Killing process %s..." % pid
        os.kill(pid, signal.SIGINT)

    @experiment_callback
    def write_stats(self):
        stats = {}

        network_port = self.start_restapi_port + self.experiment.scenario_runner._peernumber
        wallet_address = requests.get("http://localhost:%d/addresses" % (network_port)).json()[0]
        stats["wallet_address"] = wallet_address

        waves_balance = requests.get("http://localhost:%d/addresses/balance/%s" % (network_port, wallet_address)).json()
        stats["waves_balance"] = waves_balance

        asset_balances = requests.get("http://localhost:%d/assets/balance/%s" % (network_port, wallet_address)).json()
        stats["asset_balances"] = asset_balances

        with open("stats.txt", "w") as stats_file:
            stats_file.write(json.dumps(stats))

        with open("connected_peers.txt", "w") as peers_file:
            connected_peers = requests.get("http://localhost:%d/peers/connected" % (network_port)).json()
            peers_file.write(json.dumps(connected_peers))

        with open("created_orders.txt", "w") as created_orders_file:
            for order_tup in self.created_orders:
                created_orders_file.write("%s,%s\n" % (order_tup[0], order_tup[1]))

        # If we're the last node, dump the blockchain
        if len(self.experiment.get_peers()) == self.experiment.scenario_runner._peernumber:
            self.dump_blockchain()

    @experiment_callback
    def issue_asset(self, asset_name):
        # Issue an asset with the given name
        network_port = self.start_restapi_port + self.experiment.scenario_runner._peernumber
        wallet_address = requests.get("http://localhost:%d/addresses" % (network_port)).json()[0]

        request_json = {'name': asset_name, 'quantity': 100000000, 'description': 'test 1', 'sender': wallet_address, 'decimals': 0, 'reissuable': True, 'fee': 100000000}
        response = requests.post("http://localhost:%d/assets/issue" % (network_port), json=request_json, headers={'api_key': 'test'}).json()
        self.asset_id_map[asset_name] = response['assetId']

        # Broadcast
        request_json['senderPublicKey'] = response['senderPublicKey']
        request_json['timestamp'] = response['timestamp']
        request_json['signature'] = response['signature']
        response = requests.post("http://localhost:%d/assets/broadcast/issue" % (network_port), json=request_json,
                                 headers={'api_key': 'test'}).json()

    @experiment_callback
    def transfer_asset_to_all_peers(self, asset_name):
        network_port = self.start_restapi_port + self.experiment.scenario_runner._peernumber
        wallet_address = requests.get("http://localhost:%d/addresses" % (network_port)).json()[0]

        amount = 1000000000 if asset_name == "WAVES" else 100000

        # Transfer asset to other peers
        for peer_id in self.experiment.get_peers():
            if int(peer_id) == self.experiment.scenario_runner._peernumber:
                continue

            peer_network_port = self.start_restapi_port + int(peer_id)
            peer_address = self.experiment.get_peer_ip_port_by_id(int(peer_id))
            print "Fetching address of peer %d" % int(peer_id)
            peer_wallet_address = requests.get("http://%s:%d/addresses" % (peer_address[0], peer_network_port)).json()[0]
            request_json = {'sender': wallet_address, 'recipient': peer_wallet_address,
                            'fee': 100000, 'amount': amount, 'attachment': ''}
            if asset_name != "WAVES":
                request_json['assetId'] = self.asset_id_map[asset_name]
            response = requests.post("http://localhost:%d/assets/transfer" % (network_port), json=request_json,
                                     headers={'api_key': 'test'}).json()
            print "Received transfer asset response: %s" % response

            # Broadcast
            request_json['senderPublicKey'] = response['senderPublicKey']
            request_json['timestamp'] = response['timestamp']
            request_json['signature'] = response['signature']
            response = requests.post("http://localhost:%d/assets/broadcast/transfer" % (network_port),
                                     json=request_json,
                                     headers={'api_key': 'test'}).json()
            print "Received transfer asset broadcast response: %s" % response

    def post_order(self, order_type, spend_asset_name, receive_asset_name, price, amount):
        spend_asset_id = None
        receive_asset_id = None
        max_timestamp = int(round(time.time() * 1000)) + 1000 * 3600  # One hour in the future
        matcher_fee = 1000000

        my_address = pywaves.Address(privateKey=str(self.account_info['priv_key']))

        # First, get the asset ID
        network_port = self.start_restapi_port + self.experiment.scenario_runner._peernumber
        wallet_address = requests.get("http://localhost:%d/addresses" % (network_port)).json()[0]
        asset_balances = requests.get("http://localhost:%d/assets/balance/%s" % (network_port, wallet_address)).json()
        for balance_dict in asset_balances["balances"]:
            if balance_dict['issueTransaction']['name'] == spend_asset_name:
                spend_asset_id = balance_dict['assetId']
            if balance_dict['issueTransaction']['name'] == receive_asset_name:
                receive_asset_id = balance_dict['assetId']

        print "Trading %s against %s" % (spend_asset_id, receive_asset_id)

        # if order_type == "ask":
        #     order = my_address.buy(assetPair=pair, amount=int(amount), price=int(price) * 100, maxLifetime=20 * 86400)
        # else:
        #     order = my_address.sell(assetPair=pair, amount=int(amount), price=int(price) * 100, maxLifetime=20 * 86400)
        # return order

        amount_asset = spend_asset_id if spend_asset_id < receive_asset_id else receive_asset_id
        price_asset = receive_asset_id if spend_asset_id < receive_asset_id else spend_asset_id
        print "Amount asset %s against price asset %s" % (amount_asset, price_asset)

        data = {
            "senderPublicKey": self.account_info['pub_key'],
            "matcherPublicKey": self.matcher_account_info['pub_key'],
            "matcherFee": matcher_fee,
            "expiration": max_timestamp,
            "orderType": 'sell' if order_type == 'ask' else 'buy',
            "amount": int(amount),
            "timestamp": int(round(time.time() * 1000)),
            "price": int(price) * 100000000,
            "assetPair": {
                "amountAsset": price_asset,
                "priceAsset": amount_asset,
            }
        }

        # Sign the order
        sign_response = requests.post("http://localhost:%d/assets/order" % network_port,
                                      headers={'api_key': 'test'}, json=data).json()
        print "Got order sign response: %s" % sign_response

        # Post the order
        matcher_peer = self.experiment.get_peer_ip_port_by_id(self.picked_matcher_num)
        response = requests.post("http://%s:%d/matcher/orderbook" % (matcher_peer[0], self.start_matcher_port + self.picked_matcher_num),
                                 json=sign_response).json()
        print "Got order post response: %s" % response

        # Store order
        self.created_orders.append((response["message"]["timestamp"], response["message"]["signature"]))

        return response

    @experiment_callback
    def start_creating_orders(self):
        """
        Start trading with random nodes
        """
        print "Starting with random order creation on %s" % int(round(time.time() * 1000))
        self.trade_lc.start(1)

    @experiment_callback
    def stop_creating_orders(self):
        """
        Stop trading with random nodes
        """
        self.trade_lc.stop()

    def create_random_order(self):
        if self.create_ask:
            self.ask("1", "DUM1", "1", "DUM2")
        else:
            self.bid("1", "DUM1", "1", "DUM2")
        self.create_ask = not self.create_ask

    @experiment_callback
    def ask(self, price, price_type, quantity, quantity_type, order_id=None):
        response = self.post_order('ask', price_type, quantity_type, price, quantity)

        #if order_id and order_id not in self.cancelled_orders:
        #    self.order_id_map[order_id] = response

    @experiment_callback
    def bid(self, price, price_type, quantity, quantity_type, order_id=None):
        response = self.post_order('bid', price_type, quantity_type, price, quantity)

        #if order_id and order_id not in self.cancelled_orders:
        #    self.order_id_map[order_id] = response

    @experiment_callback
    def cancel(self, order_id):
        if order_id not in self.order_id_map:
            self._logger.warning("Want to cancel order but order id %s not found!", order_id)
            return

        self.cancelled_orders.add(order_id)

        order = self.order_id_map[order_id]
        order.cancel()

    @experiment_callback
    def dump_blockchain(self):
        network_port = self.start_restapi_port + self.experiment.scenario_runner._peernumber
        height = requests.get("http://localhost:%d/blocks/height" % network_port).json()["height"]
        with open("blockchain.txt", "w") as blockchain_file:
            for block_ind in range(height):
                block = requests.get("http://localhost:%d/blocks/at/%d" % (network_port, block_ind + 1)).json()
                blockchain_file.write(json.dumps(block) + "\n")

    @experiment_callback
    def stop(self):
        print "Stopping..."
        self.waves_process.kill()
        reactor.stop()
