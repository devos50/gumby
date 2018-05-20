import json
import os
import subprocess

import signal
from twisted.internet import reactor

from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException, EncodeDecimal

from gumby.experiment import experiment_callback
from gumby.modules.experiment_module import static_module, ExperimentModule


@static_module
class BitcoinModule(ExperimentModule):

    def __init__(self, experiment):
        super(BitcoinModule, self).__init__(experiment)

        self.bitcoind_process = None
        self.daemon_port = None
        self.rpc_port = None
        self.rpc_connection = None

    @experiment_callback
    def start_bitcoin(self):
        """
        Start the Bitcoin module
        """
        self.daemon_port = 12000 + self.experiment.scenario_runner._peernumber
        self.rpc_port = 14000 + self.experiment.scenario_runner._peernumber
        data_dir = os.path.join(os.getcwd(), "bitcoin_data")
        config_file_path = os.path.join(os.getcwd(), "bitcoin.conf")
        os.mkdir(data_dir)

        # First, we create a configuration file out of the template configuration
        with open("/home/pouwelse/bitcoin-template.conf", "r") as template_conf_file:
            template_content = template_conf_file.read()
            peer_1 = self.experiment.get_peer_ip_port_by_id(1)
            template_content = template_content.replace("<SEED_NODE_ADDRESS>", "%s:12001" % peer_1[0])

        with open("bitcoin.conf", "w") as conf_file:
            conf_file.write(template_content)

        start_cmd = "/home/pouwelse/bitcoin/src/bitcoind -port=%d -rpcport=%d -datadir=%s -conf=%s --assumevalid" % (self.daemon_port, self.rpc_port, data_dir, config_file_path)

        my_env = os.environ.copy()
        my_env["LD_LIBRARY_PATH"] = "/home/pouwelse/boost_1_67_0/stage/lib:/home/pouwelse/berkeley_db/lib:" + my_env["LD_LIBRARY_PATH"]

        print("Starting Bitcoin daemon with command: %s" % start_cmd)
        self.bitcoind_process = subprocess.Popen([start_cmd], env=my_env, shell=True)

    @experiment_callback
    def setup_rpc(self):
        self.rpc_connection = AuthServiceProxy("http://bitcoin:bitcoin@127.0.0.1:%d" % self.rpc_port)

    @experiment_callback
    def generate_blocks(self, amount):
        self.rpc_connection.generate(int(amount))

    @experiment_callback
    def write_stats(self):
        with open("blockchaininfo.txt", "w") as blockchain_file:
            blockchain_file.write(json.dumps(self.rpc_connection.getblockchaininfo(), default=EncodeDecimal))

    @experiment_callback
    def stop_bitcoin(self):
        pid = self.bitcoind_process.pid
        print("Killing process %s..." % pid)
        os.kill(pid, signal.SIGINT)

    @experiment_callback
    def stop(self):
        reactor.stop()
