import os
import random
import time

import six
import toml

import libra
from libra import Client, RawTransaction, SignedTransaction, AccountError, TransactionError
from libra.proto.admission_control_pb2 import SubmitTransactionRequest

from twisted.internet import reactor
from twisted.internet.defer import fail
from twisted.internet.task import deferLater, LoopingCall
from twisted.web import server, http
from twisted.web.client import readBody, WebClientContextFactory, Agent
from twisted.web.http_headers import Headers

from gumby.experiment import experiment_callback
from gumby.modules.experiment_module import static_module, ExperimentModule


def http_request(uri, method):
    """
    Performs a HTTP request
    :param uri: The URL to perform a HTTP request to
    :return: A deferred firing the body of the response.
    :raises HttpError: When the HTTP response code is not OK (i.e. not the HTTP Code 200)
    """
    def _on_response(response):
        if response.code == http.OK:
            return readBody(response)
        raise Exception(response)

    try:
        uri = six.ensure_binary(uri)
    except AttributeError:
        pass
    try:
        contextFactory = WebClientContextFactory()
        agent = Agent(reactor, contextFactory)
        headers = Headers({'User-Agent': ['Tribler 1.2.3']})
        deferred = agent.request(method, uri, headers, None)
        deferred.addCallback(_on_response)
        return deferred
    except:
        return fail()


@static_module
class LibraModule(ExperimentModule):

    def __init__(self, experiment):
        super(LibraModule, self).__init__(experiment)
        self.libra_validator_process = None
        self.faucet_process = None
        self.libra_client = None
        self.faucet_client = None
        self.host_config_dir = "/home/pouwelse/libra_config"
        self.num_validators = int(os.environ["NUM_VALIDATORS"])
        self.num_clients = int(os.environ["NUM_CLIENTS"])
        self.tx_rate = int(os.environ["TX_RATE"])
        self.validator_config = None
        self.validator_id = None
        self.validator_peer_id = None
        self.validator_ids = None
        self.wallet = None
        self.tx_info = {}
        self.last_tx_confirmed = -1

        self.tx_lc = None
        self.monitor_lc = None
        self.current_seq_num = 0

    @experiment_callback
    def generate_docker_config(self):
        """
        Generate the docker configuration files.
        """
        pass

    @experiment_callback
    def start_containers(self):
        """
        Start all Docker containers.
        """
        self._logger.info("Starting containers...")
        cmd = "docker-compose -f /home/pouwelse/libra_docker/libra-compose.yml up -d"
        os.system(cmd)

    @experiment_callback
    def generate_config(self):
        """
        Generate the initial configuration files.
        """
        self._logger.info("Removing old config...")

        # Step 1: remove configuration from previous run
        cmd = "docker exec peer1.libra.com rm -rf /etc/libra/config/*"
        os.system(cmd)

        self._logger.info("Generating new configuration...")

        # Step 2: generate new configuration
        cmd = "docker exec peer1.libra.com /opt/libra/bin/libra-config -b /libra/config/data/configs/node.config.toml -m /libra/terraform/validator-sets/dev/mint.key -o /etc/libra/config -n %d" % self.num_validators
        os.system(cmd)

        # Make sure we can edit these files
        cmd = "docker exec peer1.libra.com chmod -R 777 /etc/libra/config"
        os.system(cmd)

    @experiment_callback
    def init_config(self):
        """
        Initialize the configuration. In particular, make sure the addresses of the seed nodes are correctly set.
        """
        my_peer_id = self.experiment.scenario_runner._peernumber
        self.validator_id = my_peer_id - 1
        if not self.is_client():
            with open(os.path.join(self.host_config_dir, "%d" % self.validator_id, "node.config.toml"), "r") as node_config_file:
                content = node_config_file.read()
                node_config = toml.loads(content)
                self.validator_peer_id = node_config["networks"][0]["peer_id"]

                parts = node_config["networks"][0]["listen_address"].split("/")
                parts[1] = parts[1].replace("ip6", "ip4")
                parts[2] = parts[2].replace("::1", "0.0.0.0")
                parts[4] = "%d" % (12000 + my_peer_id)
                node_config["networks"][0]["listen_address"] = "/".join(parts)
                node_config["networks"][0]["advertised_address"] = "/".join(parts)

                node_config["admission_control"]["address"] = "0.0.0.0"
                node_config["mempool"]["capacity_per_user"] = 10000
                node_config["execution"]["genesis_file_location"] = "/libra/terraform/validator-sets/dev/genesis.blob"

                # Fix data directories
                node_config["base"]["data_dir_path"] = os.getcwd()
                node_config["storage"]["dir"] = os.path.join(os.getcwd(), "libradb", "db")

            # Write the updated node configuration
            with open(os.path.join(self.host_config_dir, "%d" % self.validator_id, "node.config.toml"), "w") as node_config_file:
                node_config_file.write(toml.dumps(node_config))

            # Update the seed configuration
            with open(os.path.join(self.host_config_dir, "%d" % self.validator_id, "%s.seed_peers.config.toml" % self.validator_peer_id), "r") as seed_peers_file:
                content = seed_peers_file.read()
                seed_peers_config = toml.loads(content)
                self.validator_ids = sorted(list(seed_peers_config["seed_peers"].keys()))

            # Adjust
            for validator_index in range(len(self.validator_ids)):
                ip, _ = self.experiment.get_peer_ip_port_by_id(validator_index + 1)
                validator_id = self.validator_ids[validator_index]

                listen_port = "%d" % (12000 + validator_index + 1)
                seed_peers_config["seed_peers"][validator_id][0] = "/ip4/%s/tcp/%s" % (ip, listen_port)

            # Write
            with open(os.path.join(self.host_config_dir, "%d" % self.validator_id, "%s.seed_peers.config.toml" % self.validator_peer_id), "w") as seed_peers_file:
                seed_peers_file.write(toml.dumps(seed_peers_config))

    @experiment_callback
    def start_libra_validator(self):
        # Read the config
        my_peer_id = self.experiment.scenario_runner._peernumber
        if self.is_client():
            return

        # Start a validator
        my_libra_id = self.validator_ids[my_peer_id - 1]

        self._logger.info("Starting libra validator with id %s...", my_libra_id)
        cmd = "docker exec -d peer%d.libra.com /opt/libra/bin/libra-node -f /etc/libra/config/%d/node.config.toml" % (my_peer_id, my_peer_id - 1)
        os.system(cmd)

    @experiment_callback
    def get_validator_config(self, validator_id):
        with open(os.path.join(self.host_config_dir, "%d" % validator_id, "node.config.toml"), "r") as validator_config_file:
            content = validator_config_file.read()
            validator_config = toml.loads(content)

        return validator_config

    @experiment_callback
    def start_minter(self):
        """
        Start the minting service.
        """
        self._logger.info("Starting minting")
        validator_config = self.get_validator_config(0)
        port = validator_config["admission_control"]["admission_control_service_port"]

        # First, copy the required files to /opt/libra/etc
        cmd = "docker exec peer1.libra.com cp /libra/terraform/validator-sets/dev/mint.key /opt/libra/etc"
        os.system(cmd)
        cmd = "docker exec peer1.libra.com cp /etc/libra/config/0/consensus_peers.config.toml /opt/libra/etc"
        os.system(cmd)

        # Start the minter
        cmd = "docker exec -d -e AC_HOST=127.0.0.1 -e AC_PORT=%d peer1.libra.com bash -c \"cd /opt/libra/bin && gunicorn --bind 0.0.0.0:8000 server\"" % port
        os.system(cmd)

    @experiment_callback
    def start_libra_client(self):
        my_peer_id = self.experiment.scenario_runner._peernumber
        validator_peer_id = (my_peer_id - 1) % self.num_validators
        validator_config = self.get_validator_config(validator_peer_id)
        port = validator_config["admission_control"]["admission_control_service_port"]
        host, _ = self.experiment.get_peer_ip_port_by_id(validator_peer_id + 1)

        # Get the faucet host
        faucet_host, _ = self.experiment.get_peer_ip_port_by_id(1)

        if self.is_client():
            self._logger.info("Spawning client that connects to validator %s (host: %s, port %s)", validator_peer_id, host, port)
            self.libra_client = Client.new(host, port, os.path.join(self.host_config_dir, "%d" % validator_peer_id, "consensus_peers.config.toml"))
            self.libra_client.faucet_host = faucet_host + ":8000"

    @experiment_callback
    def create_accounts(self):
        if not self.is_client():
            return

        self._logger.info("Creating accounts...")
        self.wallet = libra.WalletLibrary.new()
        self.wallet.new_account()
        self.wallet.new_account()

    @experiment_callback
    def mint(self):
        if not self.is_client():
            return

        random_wait = 5 * random.random()

        def perform_mint_request():
            faucet_host, _ = self.experiment.get_peer_ip_port_by_id(1)
            address = self.wallet.accounts[0].address.hex()
            deferred = http_request("http://" + faucet_host + ":8000/?amount=%d&address=%s" % (1000000, address), b'POST')
            print("Mint request performed!")

        deferLater(reactor, random_wait, perform_mint_request)

    @experiment_callback
    def print_balance(self, account_nr):
        if not self.is_client():
            return

        address = self.wallet.accounts[int(account_nr)].address
        print(self.libra_client.get_balance(address))

    @experiment_callback
    def transfer(self):
        receiver_address = self.wallet.accounts[1].address
        sender_account = self.wallet.accounts[0]
        raw_tx = RawTransaction.gen_transfer_transaction(sender_account.address, self.current_seq_num,
                                                         receiver_address, 100, 140_000, 0, 100)
        signed_txn = SignedTransaction.gen_from_raw_txn(raw_tx, sender_account)
        request = SubmitTransactionRequest()
        request.signed_txn.signed_txn = signed_txn.serialize()
        submit_time = int(round(time.time() * 1000))

        try:
            self.libra_client.submit_transaction(request, raw_tx, False)
        except TransactionError:
            self._logger.exception("Failed to submit transaction to validator!")

        self.tx_info[self.current_seq_num] = (submit_time, -1)
        self.current_seq_num += 1

    @experiment_callback
    def start_creating_transactions(self):
        """
        Start with submitting transactions.
        """
        if not self.is_client():
            return

        self._logger.info("Starting transactions...")
        self.tx_lc = LoopingCall(self.transfer)

        # Depending on the tx rate and number of clients, wait a bit
        individual_tx_rate = self.tx_rate / self.num_clients
        self._logger.info("Individual tx rate: %f" % individual_tx_rate)

        def start_lc():
            self._logger.info("Starting tx lc...")
            self.tx_lc.start(1.0 / individual_tx_rate)

        my_peer_id = self.experiment.scenario_runner._peernumber
        deferLater(reactor, (1.0 / self.num_clients) * (my_peer_id - 1), start_lc)

    def is_client(self):
        my_peer_id = self.experiment.scenario_runner._peernumber
        return my_peer_id > self.num_validators

    @experiment_callback
    def start_monitor(self):
        if not self.is_client():
            return

        self.monitor_lc = LoopingCall(self.monitor)
        self.monitor_lc.start(0.1)

    def monitor(self):
        """
        Monitor the transactions.
        """
        request_time = int(round(time.time() * 1000))

        try:
            ledger_seq_num = self.libra_client.get_sequence_number(self.wallet.accounts[0].address)
        except AccountError:
            self._logger.warning("Empty account blob!")
            return

        for seq_num in range(self.last_tx_confirmed, ledger_seq_num):
            if seq_num == -1:
                continue
            self.tx_info[seq_num] = (self.tx_info[seq_num][0], request_time)

        self.last_tx_confirmed = ledger_seq_num - 1

    @experiment_callback
    def stop_monitor(self):
        if not self.is_client():
            return

        self.monitor_lc.stop()

    @experiment_callback
    def stop_creating_transactions(self):
        """
        Stop with submitting transactions.
        """
        if not self.is_client():
            return

        self._logger.info("Stopping transactions...")
        self.tx_lc.stop()

    @experiment_callback
    def write_tx_stats(self):
        # Write transaction data
        with open("transactions.txt", "w") as tx_file:
            for tx_num, tx_info in self.tx_info.items():
                tx_file.write("%d,%d,%d\n" % (tx_num, tx_info[0], tx_info[1]))

    @experiment_callback
    def stop_containers(self):
        """
        Stop the started Docker containers.
        """
        self._logger.info("Stopping containers...")
        cmd = "docker-compose -f /home/pouwelse/libra_docker/libra-compose.yml down"
        os.system(cmd)

    @experiment_callback
    def stop(self):
        print("Stopping Libra...")
        reactor.stop()
