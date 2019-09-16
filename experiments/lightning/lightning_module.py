import os
import subprocess
import sys

import pexpect
from twisted.internet import reactor

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
        cmd = "/home/pouwelse/bitcoin/bin/bitcoind -datadir=%s --deprecatedrpc=generate > btc_node.out 2>&1" % self.btc_config_dir
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

    @experiment_callback
    def print_info(self):
        """
        Print generic info of the LND node.
        """
        cmd = self.get_lncli_cmd_prefix() + "getinfo"
        p = subprocess.Popen([cmd], shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = p.communicate()
        print(out)

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
    def stop(self):
        print("Stopping Lightning...")
        if self.btc_client:
            self.btc_client.kill()
        if self.lnd_client:
            self.lnd_client.kill()
        reactor.stop()
