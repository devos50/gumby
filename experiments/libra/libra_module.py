import os
import subprocess
import sys
import time

import pexpect as pexpect
import toml

from twisted.internet import reactor
from twisted.web import server

from experiments.libra.faucet_endpoint import FaucetEndpoint
from gumby.experiment import experiment_callback
from gumby.modules.experiment_module import static_module, ExperimentModule


@static_module
class LibraModule(ExperimentModule):

    def __init__(self, experiment):
        super(LibraModule, self).__init__(experiment)
        self.libra_validator_process = None
        self.faucet_process = None
        self.libra_client = None
        self.faucet_service = None
        self.libra_path = "/home/pouwelse/libra"
        self.num_validators = 0
        self.validator_config = None
        self.validator_ids = None
        self.wallet = None

    @experiment_callback
    def init_config(self):
        """
        Initialize the configuration. In particular, make sure the addresses of the seed nodes are correctly set.
        """
        with open(os.path.join(self.libra_path, "das_config", "seed_peers.config.toml"), "r") as seed_peers_file:
            content = seed_peers_file.read()
            seed_peers_config = toml.loads(content)
            validator_ids = sorted(list(seed_peers_config["seed_peers"].keys()))

        # Adjust
        for validator_index in range(len(validator_ids)):
            ip, _ = self.experiment.get_peer_ip_port_by_id(validator_index + 1)
            validator_id = validator_ids[validator_index]

            current_host = seed_peers_config["seed_peers"][validator_id][0]
            parts = current_host.split("/")
            listen_port = parts[4]

            seed_peers_config["seed_peers"][validator_id][0] = "/ip4/%s/tcp/%s" % (ip, listen_port)

        # Write
        with open(os.path.join(self.libra_path, "das_config", "seed_peers.config.toml"), "w") as seed_peers_file:
            seed_peers_file.write(toml.dumps(seed_peers_config))

    @experiment_callback
    def start_libra_validator(self):
        # Read the config
        with open(os.path.join(self.libra_path, "das_config", "seed_peers.config.toml"), "r") as seed_peers_file:
            content = seed_peers_file.read()
            self.validator_config = toml.loads(content)
            self.validator_ids = sorted(list(self.validator_config["seed_peers"].keys()))

        my_peer_id = self.experiment.scenario_runner._peernumber
        self.num_validators = len(self.validator_ids)
        if my_peer_id <= self.num_validators:
            # Start a validator
            my_libra_id = self.validator_ids[my_peer_id - 1]

            self._logger.info("Starting libra validator with id %s...", my_libra_id)
            self.libra_validator_process = subprocess.Popen(['/home/pouwelse/libra/target/release/libra_node -f %s > %s 2>&1' %
                                                             ('/home/pouwelse/libra/das_config/validator_%s.node.config.toml' % my_libra_id,
                                                              os.path.join(os.getcwd(), 'libra_output.log'))], shell=True)

    @experiment_callback
    def start_libra_client(self):
        my_peer_id = self.experiment.scenario_runner._peernumber
        validator_peer_id = (my_peer_id - 1) % self.num_validators + 1
        target_validator_id = self.validator_ids[validator_peer_id - 1]

        with open(os.path.join(self.libra_path, "das_config", "validator_%s.node.config.toml" % target_validator_id), "r") as validator_config_file:
            content = validator_config_file.read()
            validator_config = toml.loads(content)
            port = validator_config["admission_control"]["admission_control_service_port"]
            host, _ = self.experiment.get_peer_ip_port_by_id(validator_peer_id)

            # Get the faucet host
            faucet_host, _ = self.experiment.get_peer_ip_port_by_id(1)

            self._logger.info("Spawning client that connects to validator %s (host: %s, port %s)", target_validator_id, host, port)
            cmd = "/home/pouwelse/libra/target/release/client " \
                  "--host %s " \
                  "--port %s " \
                  "--validator_set_file /home/pouwelse/libra/das_config/consensus_peers.config.toml" % (host, port)

            if my_peer_id == 1:
                cmd += " -m /home/pouwelse/libra/single_config/mint.key"
            else:
                cmd += " --faucet_server %s:8000 " % faucet_host

            self.libra_client = pexpect.spawn(cmd)
            self.libra_client.logfile = sys.stdout.buffer
            self.libra_client.expect("Please, input commands", timeout=3)

            if my_peer_id == 1:
                # Also start the HTTP API for the faucet service
                self._logger.info("Starting faucet HTTP API...")
                faucet_endpoint = FaucetEndpoint(self.libra_client)
                site = server.Site(resource=faucet_endpoint)
                self.faucet_service = reactor.listenTCP(8000, site, interface="0.0.0.0")

    @experiment_callback
    def create_accounts(self):
        self._logger.info("Creating accounts...")
        self.libra_client.sendline("a c")
        self.libra_client.expect("Created/retrieved account", timeout=2)

        self.libra_client.sendline("a c")
        self.libra_client.expect("Created/retrieved account", timeout=2)

    @experiment_callback
    def mint(self):
        self.libra_client.sendline("q as 0")
        self.libra_client.expect("Latest account state is", timeout=2)

        self.libra_client.sendline("query sequence 0")
        self.libra_client.expect("Sequence number is", timeout=2)

        self.libra_client.sendline("a m 0 1000000")
        self.libra_client.expect("Mint request submitted", timeout=2)

    @experiment_callback
    def print_balance(self, account_nr):
        self.libra_client.sendline("q b %s" % account_nr)
        self.libra_client.expect("Balance", timeout=2)

        self.libra_client.sendline("query txn_acc_seq 0 1 true")
        self.libra_client.expect("Balance", timeout=2)

    @experiment_callback
    def transfer(self):
        for _ in range(10):
            self.libra_client.sendline("t 0 1 100")
            self.libra_client.expect("Transaction submitted to validator", timeout=10)

    @experiment_callback
    def stop(self):
        print("Stopping Libra...")
        if self.libra_validator_process:
            self.libra_validator_process.kill()
        if self.faucet_process:
            self.faucet_process.kill()
        reactor.stop()
