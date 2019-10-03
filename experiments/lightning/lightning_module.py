import json
import os
import subprocess
import sys

import pexpect
from twisted.internet import reactor
from twisted.internet.task import deferLater

from gumby.experiment import experiment_callback
from gumby.modules.experiment_module import static_module, ExperimentModule


@static_module
class LightningModule(ExperimentModule):

    def __init__(self, experiment):
        super(LightningModule, self).__init__(experiment)
        self.btc_config_dir = os.path.join(os.getcwd(), "btc_config")
        self.lnd_config_dir = os.path.join(os.getcwd(), "lnd_config")
        self.btc_client = None
        self.lnd_client = None
        self.experiment.message_callback = self
        self.lnd_pub_keys = {}
        self.wallet_addresses = {}

    def on_message(self, from_id, msg_type, msg):
        if msg_type == b"pub_key":
            self.lnd_pub_keys[from_id] = msg
        elif msg_type == b"wallet":
            self.wallet_addresses[from_id] = msg

    @experiment_callback
    def init_btc_config(self):
        """
        Create the configuration files for the BTC node.
        """
        os.mkdir(self.btc_config_dir)

        with open(os.path.join(self.btc_config_dir, "bitcoin.conf"), "w") as btc_config_file:
            my_peer_id = self.experiment.scenario_runner._peernumber
            zmq_block_port = 28000 + my_peer_id
            zmq_tx_port = 31000 + my_peer_id
            config = """regtest=1
txindex=1
rpcauth=tribler:4cf903bfe49c1e240ff0d9120c619314$bcf1f7aa88a4576a64e837bace9498c3c7ba4fe5cae40193c4f8e2898a9a01e8
zmqpubrawblock=tcp://127.0.0.1:%d
zmqpubrawtx=tcp://127.0.0.1:%d
""" % (zmq_block_port, zmq_tx_port)
            btc_config_file.write(config)

    @experiment_callback
    def start_btc_client(self):
        """
        Start the BTC node.
        """
        my_peer_id = self.experiment.scenario_runner._peernumber
        cmd = "/home/pouwelse/bitcoin/bin/bitcoind -datadir=%s -port=%d --deprecatedrpc=generate > btc_node.out 2>&1" % (self.btc_config_dir, my_peer_id + 34000)
        self.btc_client = subprocess.Popen([cmd], shell=True)

    @experiment_callback
    def init_lnd_config(self):
        """
        Init the configuration for the lightning network.
        """
        os.mkdir(self.lnd_config_dir)

        with open(os.path.join(self.lnd_config_dir, "lnd.conf"), "w") as lnd_config_file:
            my_peer_id = self.experiment.scenario_runner._peernumber
            lnd_listen_port = 5000 + my_peer_id
            rpc_listen_port = 8000 + my_peer_id
            rest_listen_port = 11000 + my_peer_id
            zmq_block_port = 28000 + my_peer_id
            zmq_tx_port = 31000 + my_peer_id
            config = """[Application Options]

listen=0.0.0.0:%d
rpclisten=localhost:%d
restlisten=0.0.0.0:%d

[Bitcoin]

bitcoin.active=1
bitcoin.regtest=1
bitcoin.node=bitcoind

[Bitcoind]

bitcoind.rpchost=localhost
bitcoind.rpcuser=tribler
bitcoind.rpcpass=tribler
bitcoind.zmqpubrawblock=tcp://127.0.0.1:%d
bitcoind.zmqpubrawtx=tcp://127.0.0.1:%d""" % (lnd_listen_port, rpc_listen_port, rest_listen_port, zmq_block_port, zmq_tx_port)
            lnd_config_file.write(config)

    @experiment_callback
    def start_lnd_client(self):
        """
        Start the LND node.
        """
        cmd = "/home/pouwelse/gocode/bin/lnd --lnddir=%s > lnd_node.out 2>&1" % self.lnd_config_dir
        self.lnd_client = subprocess.Popen([cmd], shell=True)

    @experiment_callback
    def generate_blocks(self, num_blocks):
        num_blocks = int(num_blocks)
        cmd = "/home/pouwelse/bitcoin/bin/bitcoin-cli -datadir=%s generate %d" % (self.btc_config_dir, num_blocks)
        subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def get_lncli_cmd_prefix(self):
        my_peer_id = self.experiment.scenario_runner._peernumber
        return "/home/pouwelse/gocode/bin/lncli -n regtest --lnddir=%s --rpcserver=localhost:%d " % (self.lnd_config_dir, 8000 + my_peer_id)

    @experiment_callback
    def create_wallet(self):
        """
        Create a new LND wallet.
        """
        cmd = self.get_lncli_cmd_prefix() + "create"
        create_wallet_proc = pexpect.spawn(cmd)
        create_wallet_proc.logfile = sys.stdout.buffer
        create_wallet_proc.expect("Input wallet password", timeout=2)

        create_wallet_proc.sendline("secretpassword123")
        create_wallet_proc.expect("Confirm password", timeout=2)

        create_wallet_proc.sendline("secretpassword123")
        create_wallet_proc.expect("Do you have an existing cipher seed mnemonic you want to use", timeout=2)

        create_wallet_proc.sendline("n")
        create_wallet_proc.expect("Input your passphrase if you wish to encrypt it", timeout=2)

        create_wallet_proc.sendline()
        create_wallet_proc.expect("lnd successfully initialized!", timeout=5)

        def share_wallet_info():
            my_peer_id = self.experiment.scenario_runner._peernumber
            dest_peer = 1 if my_peer_id == 2 else 2

            client_identity_pubkey = self.get_client_info()["identity_pubkey"].encode()
            self.experiment.send_message(dest_peer, b"pub_key", client_identity_pubkey)

            # Also create a new wallet and share the address
            cmd = self.get_lncli_cmd_prefix() + "newaddress np2wkh"
            p = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            out, _ = p.communicate()
            wallet_address = json.loads(out)["address"].encode()

            self.experiment.send_message(dest_peer, b"wallet", wallet_address)

        deferLater(reactor, 8, share_wallet_info)

    @experiment_callback
    def btc_transfer(self, peer_id, btc_amount):
        """
        Transfer some BTC to another node using the conventional on-chain transfer operation.
        """
        peer_id = int(peer_id)
        btc_amount = int(btc_amount)

        if peer_id not in self.wallet_addresses:
            self._logger.error("Wallet address for peer %d not found, aborting transfer!", peer_id)
            return

        cmd = "/home/pouwelse/bitcoin/bin/bitcoin-cli -datadir=%s sendtoaddress %s %d" % (self.btc_config_dir, self.wallet_addresses[peer_id].decode(), btc_amount)
        p = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        print(out)
        print(err)

        cmd = "/home/pouwelse/bitcoin/bin/bitcoin-cli -datadir=%s generate 10" % self.btc_config_dir
        subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def get_client_info(self):
        cmd = self.get_lncli_cmd_prefix() + "getinfo"
        p = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, _ = p.communicate()
        return json.loads(out)

    @experiment_callback
    def print_info(self):
        """
        Print generic info of the LND node.
        """
        print(self.get_client_info())

    @experiment_callback
    def print_peers(self):
        """
        Print the connected peers of the LND node.
        """
        cmd = self.get_lncli_cmd_prefix() + "listpeers"
        p = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        print(out)

    @experiment_callback
    def print_balance(self):
        """
        Print the BTC balance of this peer.
        """
        cmd = "/home/pouwelse/bitcoin/bin/bitcoin-cli -datadir=%s getbalance" % self.btc_config_dir
        p = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        print(out)

    @experiment_callback
    def btc_peer_connect(self, peer_id):
        """
        Connect this peer to another btc instance with the given peer_id.
        """
        peer_id = int(peer_id)
        host, _ = self.experiment.get_peer_ip_port_by_id(peer_id)
        cmd = "/home/pouwelse/bitcoin/bin/bitcoin-cli -datadir=%s addnode %s:%d add" % (self.btc_config_dir, host, peer_id + 34000)
        p = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        print(out)
        print(err)

    @experiment_callback
    def lnd_peer_connect(self, peer_id):
        """
        Connect this peer to another lnd instance with the given peer_id.
        """
        peer_id = int(peer_id)
        if peer_id not in self.lnd_pub_keys:
            self._logger.error("Cannot find lnd pub key for peer %d, not connecting!", peer_id)
            return

        host, _ = self.experiment.get_peer_ip_port_by_id(peer_id)

        cmd = self.get_lncli_cmd_prefix() + "connect %s@%s:%d" % (self.lnd_pub_keys[peer_id].decode(), host, 5000 + peer_id)
        subprocess.Popen([cmd], shell=True)

    @experiment_callback
    def open_channel(self, peer_id, amount):
        """
        Open a payment channel to another peer with a specified amount.
        """
        peer_id = int(peer_id)
        amount = int(amount)

        if peer_id not in self.lnd_pub_keys:
            self._logger.error("Cannot find lnd pub key for peer %d, not opening channel!", peer_id)
            return

        cmd = self.get_lncli_cmd_prefix() + "openchannel %s %d" % (self.lnd_pub_keys[peer_id].decode(), amount)
        p = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        print(out)
        print(err)

        cmd = "/home/pouwelse/bitcoin/bin/bitcoin-cli -datadir=%s generate 5" % self.btc_config_dir
        subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    @experiment_callback
    def print_channels(self):
        """
        Print all channels opened by/to this peer.
        """
        cmd = self.get_lncli_cmd_prefix() + "listchannels"
        p = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        print(out)

    @experiment_callback
    def stop(self):
        print("Stopping Lightning...")
        if self.btc_client:
            self.btc_client.kill()
        if self.lnd_client:
            self.lnd_client.kill()
        reactor.stop()
