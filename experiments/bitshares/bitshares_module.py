import json
import os
import random
import shutil
import subprocess

import time
from twisted.internet import reactor
from twisted.internet.task import LoopingCall

from gumby.experiment import experiment_callback
from gumby.modules.experiment_module import static_module, ExperimentModule


@static_module
class BitsharesModule(ExperimentModule):

    def __init__(self, experiment):
        super(BitsharesModule, self).__init__(experiment)
        self.wallet_rpc = None
        self.wallet_rpc_port = None
        self.wallet_rpc_user = None
        self.wallet_rpc_password = "supersecret"
        self.brain_priv_key = None
        self.wif_priv_key = None
        self.pub_key = None
        self.trade_lc = LoopingCall(self.create_random_order)
        self.username = None
        self.bs_process = None
        self.bs_process_lc = None
        self.create_ask = True
        self.orders_info = []  # Keeps track of tuples: (order_creation_time, signature)

        self.order_id_map = {}
        self.cancelled_orders = set()

    @experiment_callback
    def start_bitshares(self):
        # Copy the data directory
        print("Current dir: %s" % os.getcwd())
        shutil.copytree('/var/scratch/pouwelse/bitshares/data-clean', 'data')

        account_number = self.experiment.scenario_runner._peernumber - 1
        self.username = "user%d" % account_number

        # Read the keys
        with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "keypairs.txt"), "r") as keys_file:
            lines = keys_file.readlines()
            for ind, line in enumerate(lines):
                if len(line) > 0 and ind == account_number:
                    parts = line.rstrip('\n').split(",")
                    self.brain_priv_key = parts[0]
                    self.pub_key = parts[1]
                    self.wif_priv_key = parts[2]

        # First, we create a configuration file out of the template configuration
        with open("/var/scratch/pouwelse/bitshares/data-clean/config-template.ini", "r") as template_conf_file:
            template_content = template_conf_file.read()

        peer_id = self.experiment.scenario_runner._peernumber
        self.wallet_rpc_user = "1.6.%d" % peer_id
        print("Assigned peer id: %d" % peer_id)
        template_content = template_content.replace("<BITSHARES_P2P_ENDPOINT>", "0.0.0.0:%d" % (11000 + peer_id))
        if peer_id != 1:
            peer_1 = self.experiment.get_peer_ip_port_by_id(1)
            template_content = template_content.replace("<BITSHARES_SEED_NODE>", "seed-node = %s:11001" % peer_1[0])
        else:
            template_content = template_content.replace("<BITSHARES_SEED_NODE>", "")
        template_content = template_content.replace("<BITSHARES_RPC_ENDPOINT>", "0.0.0.0:%d" % (12000 + peer_id))
        template_content = template_content.replace("<BITSHARES_WITNESS_ID>", self.wallet_rpc_user)
        template_content = template_content.replace("<BITSHARES_PUBLIC_KEY>", self.pub_key)
        template_content = template_content.replace("<BITSHARES_PRIVATE_KEY>", self.wif_priv_key)

        with open(os.path.join("data", "config.ini"), "w") as conf_file:
            conf_file.write(template_content)

        # Now we start the witness node
        if self.experiment.scenario_runner._peernumber == 1:
            self.start_bitshares_process()
        else:
            reactor.callLater(random.random() * 10, self.start_bitshares_process)

        self.bs_process_lc = LoopingCall(self.check_bs_process)
        self.bs_process_lc.start(12, now=False)

    def on_id_received(self):
        super(BitsharesModule, self).on_id_received()
        self.create_ask = (self.experiment.scenario_runner._peernumber % 2 == 0)

    def check_bs_process(self):
        """
        Check whether the main Bitshares process is alive. If it's not, restart it
        """
        if self.bs_process.poll():
            print("Bitshares process died - starting again!")
            self.start_bitshares_process()

    def start_bitshares_process(self):
        self.bs_process = subprocess.Popen(['/var/scratch/pouwelse/bitshares/witness_node --genesis-json=/var/scratch/pouwelse/bitshares/my-genesis.json --data-dir data --partial-operations true > %s 2>&1' % os.path.join(os.getcwd(), 'bitshares_output.log')], shell=True)

    @experiment_callback
    def start_cli_wallet(self):
        peer_id = self.experiment.scenario_runner._peernumber
        server_rpc_port = 12000 + peer_id
        self.wallet_rpc_port = 13000 + peer_id
        
        # Get the chain ID
        with open("/var/scratch/pouwelse/bitshares/chainid", "r") as chainid_file:
            chain_id = chainid_file.read().rstrip('\n')

        print("Starting CLI wallet with chain ID: %s" % chain_id)

        subprocess.Popen(['/var/scratch/pouwelse/bitshares/cli_wallet --wallet-file=my-wallet.json --chain-id %s --server-rpc-endpoint=ws://127.0.0.1:%d --server-rpc-user=%s --server-rpc-password=%s --rpc-endpoint=127.0.0.1:%d --daemon > %s 2>&1' % (chain_id, server_rpc_port, self.wallet_rpc_user, self.wallet_rpc_password, self.wallet_rpc_port, os.path.join(os.getcwd(), 'bitshares_wallet_output.log'))], shell=True)
        self.bs_process_lc.stop()

    @experiment_callback
    def unlock_cli_wallet(self):
        from grapheneapi.grapheneapi import GrapheneAPI
        self._logger.info("Will unlock wallet")
        self.wallet_rpc = GrapheneAPI("127.0.0.1", self.wallet_rpc_port, "", "")
        self.wallet_rpc.set_password("secret")
        self.wallet_rpc.unlock("secret")

        self.wallet_rpc.import_key(self.username, self.wif_priv_key)

    @experiment_callback
    def init_nathan(self):
        self.wallet_rpc.import_key("nathan", "5KQwrPbwdL6PhXujxW37FSSQZ1JiwsST4cqQzDeyXtP79zkvFD3")
        self.wallet_rpc.import_balance("nathan", ["5KQwrPbwdL6PhXujxW37FSSQZ1JiwsST4cqQzDeyXtP79zkvFD3"], True)

    @experiment_callback
    def transfer_asset_to_all_peers(self, asset_name):
        for peer_id in self.experiment.get_peers():
            response = self.wallet_rpc.transfer("nathan", "user%d" % (int(peer_id) - 1), 100000, asset_name, "transferral", True)
            print("Transferred %s assets to peer %d, response: %s" % (asset_name, int(peer_id), response))

    @experiment_callback
    def write_stats(self):
        with open("balances.txt", "w") as balances_file:
            balances_file.write(json.dumps(self.wallet_rpc.list_account_balances(self.username)))
        with open("global_settings.txt", "w") as settings_file:
            settings_file.write(json.dumps(self.wallet_rpc.get_global_properties()))
        with open("dynamic_settings.txt", "w") as dynamic_settings_file:
            dynamic_settings_file.write(json.dumps(self.wallet_rpc.get_dynamic_global_properties()))
        with open("orderbook.txt", "w") as orderbook_file:
            orderbook_file.write(json.dumps(self.wallet_rpc.get_order_book("DUM1", "DUM2", 50)))
        with open("orders.txt", "w") as orders_file:
            orders_file.write(json.dumps(self.wallet_rpc.get_limit_orders("DUM1", "DUM2", 10000)))
        with open("history.txt", "w") as history_file:
            history_file.write(json.dumps(self.wallet_rpc.get_account_history(self.username, 1000)))
        with open("witnesses.txt", "w") as witnesses_file:
            witnesses_file.write(json.dumps(self.wallet_rpc.list_witnesses("", 1000)))

        # Write a map of the order info
        with open("created_orders.txt", "w") as created_orders_file:
            for order_tup in self.orders_info:
                created_orders_file.write("%d,%s\n" % (order_tup[0], order_tup[1]))

        # Determine bandwidth usage
        total_up = 0
        total_down = 0
        connected_peers = self.wallet_rpc.network_get_connected_peers()
        for peer in connected_peers:
            total_up += peer["info"]["bytessent"]
            total_down += peer["info"]["bytesrecv"]

        with open("bandwidth.txt", "w") as bandwidth_file:
            bandwidth_file.write("%d,%d,%d" % (total_up, total_down, total_up + total_down))

        if self.experiment.scenario_runner._peernumber == 1:
            self.dump_blockchain()

    def post_order(self, order_type, spend_asset_name, receive_asset_name, price, amount):
        if order_type == "bid":
            spend_asset_name, receive_asset_name = receive_asset_name, spend_asset_name

        print("New order: spending %s, receiving %s, price: %d, amount: %d (account: %s)" % (spend_asset_name, receive_asset_name, int(price), int(amount), self.username))

        order_creation_time = int(round(time.time() * 1000))
        response = self.wallet_rpc.sell_asset(self.username, int(price), spend_asset_name, int(amount), receive_asset_name, 3600, False, True)
        self.orders_info.append((order_creation_time, response["signatures"][0]))
        print("Placed order, response: %s" % response)

        return response

    @experiment_callback
    def ask(self, price, price_type, quantity, quantity_type, order_id=None):
        response = self.post_order('ask', price_type, quantity_type, price, quantity)

        if order_id and order_id not in self.cancelled_orders:
            self.order_id_map[order_id] = response

    @experiment_callback
    def bid(self, price, price_type, quantity, quantity_type, order_id=None):
        response = self.post_order('bid', price_type, quantity_type, price, quantity)

        if order_id and order_id not in self.cancelled_orders:
            self.order_id_map[order_id] = response

    @experiment_callback
    def cancel(self, order_id):
        # TODO
        pass

    def create_random_order(self):
        if self.create_ask:
            self.ask("10", "DUM1", "10", "DUM2")
        else:
            self.bid("10", "DUM1", "10", "DUM2")
        self.create_ask = not self.create_ask

    @experiment_callback
    def start_creating_orders(self):
        print("Starting with random orders at %s" % int(round(time.time() * 1000)))
        self.trade_lc.start(1)

    @experiment_callback
    def stop_creating_orders(self):
        self.trade_lc.stop()

    @experiment_callback
    def dump_blockchain(self):
        dynamic_settings = self.wallet_rpc.get_dynamic_global_properties()
        head_block_nr = dynamic_settings["head_block_number"]
        with open("blockchain.txt", "w") as blockchain_file:
            for ind in range(1, head_block_nr + 1):
                blockchain_file.write(json.dumps(self.wallet_rpc.get_block(ind)) + "\n")

    @experiment_callback
    def stop(self):
        print("Stopping...")
        self.bs_process.kill()
        reactor.stop()
