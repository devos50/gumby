import os
import subprocess
import sys
from threading import Thread
from urllib.parse import quote_plus

import requests

import treq

from stellar_base import Keypair, Horizon
from stellar_base.builder import Builder

from twisted.internet import reactor
from twisted.internet.task import LoopingCall, deferLater

from gumby.experiment import experiment_callback
from gumby.modules.experiment_module import static_module, ExperimentModule


@static_module
class StellarModule(ExperimentModule):

    def __init__(self, experiment):
        super(StellarModule, self).__init__(experiment)
        self.db_path = None
        self.postgres_process = None
        self.validator_process = None
        self.horizon_process = None
        self.experiment.message_callback = self

        self.num_validators = int(os.environ["NUM_VALIDATORS"])
        self.num_clients = int(os.environ["NUM_CLIENTS"])
        self.tx_rate = int(os.environ["TX_RATE"])

        self.num_accounts_per_client = 10

        self.sender_keypairs = [None] * self.num_accounts_per_client
        self.receiver_keypair = None
        self.tx_lc = None
        self.sequence_numbers = [25769803776] * self.num_accounts_per_client
        self.current_tx_num = 0

        # Make sure our postgres can be found
        sys.path.append("/home/pouwelse/postgres/bin")

    def on_message(self, from_id, msg_type, msg):
        self._logger.info("Received message with type %s from peer %d", msg_type, from_id)
        if msg_type.startswith(b"send_account_seed"):
            account_nr = int(msg_type.split(b"_")[-1])
            self.sender_keypairs[account_nr] = Keypair.from_seed(msg)
        elif msg_type == b"receive_account_seed":
            self.receiver_keypair = Keypair.from_seed(msg)

    def is_client(self):
        my_peer_id = self.experiment.scenario_runner._peernumber
        return my_peer_id > self.num_validators

    def is_responsible_validator(self):
        """
        Return whether this validator is the responsible validator to setup/init databases on this machine.
        This can only be conducted by a single process.
        """
        if self.is_client():
            return False

        my_peer_id = self.experiment.scenario_runner._peernumber
        my_host, _ = self.experiment.get_peer_ip_port_by_id(my_peer_id)

        is_responsible = True
        for peer_id in self.experiment.all_vars.keys():
            if self.experiment.all_vars[peer_id]['host'] == my_host and int(peer_id) < my_peer_id:
                is_responsible = False
                break

        return is_responsible

    @experiment_callback
    def init_db(self):
        """
        Start the postgres daemon.
        """
        if self.is_client() or not self.is_responsible_validator():
            return

        peer_id = self.experiment.scenario_runner._peernumber
        ip, _ = self.experiment.get_peer_ip_port_by_id(peer_id)

        self.db_path = os.path.join(os.environ["WORKSPACE"], "postgres", ip)
        os.makedirs(self.db_path)

        os.system("/home/pouwelse/postgres/bin/initdb %s" % self.db_path)

    @experiment_callback
    def start_db(self):
        if self.is_client() or not self.is_responsible_validator():
            return

        os.environ["PGDATA"] = self.db_path
        cmd = "/home/pouwelse/postgres/bin/pg_ctl start"
        self.postgres_process = subprocess.Popen([cmd], shell=True)

    @experiment_callback
    def setup_db(self):
        if self.is_client() or not self.is_responsible_validator():
            return

        # Create users and table
        cmd = "CREATE USER tribler WITH PASSWORD 'tribler';"
        os.system('/home/pouwelse/postgres/bin/psql postgres -c "%s"' % cmd)

        cmd = "ALTER USER tribler WITH SUPERUSER;"
        os.system('/home/pouwelse/postgres/bin/psql postgres -c "%s"' % cmd)

    @experiment_callback
    def create_db(self):
        if self.is_client():
            return

        peer_id = self.experiment.scenario_runner._peernumber

        cmd = "CREATE DATABASE stellar_%d_db;" % peer_id
        os.system('/home/pouwelse/postgres/bin/psql postgres -c "%s"' % cmd)

        cmd = "GRANT ALL PRIVILEGES ON DATABASE stellar_%d_db TO tribler;" % peer_id
        os.system('/home/pouwelse/postgres/bin/psql postgres -c "%s"' % cmd)

        cmd = "CREATE DATABASE stellar_horizon_%d_db;" % peer_id
        os.system('/home/pouwelse/postgres/bin/psql postgres -c "%s"' % cmd)

        cmd = "GRANT ALL PRIVILEGES ON DATABASE stellar_horizon_%d_db TO tribler;" % peer_id
        os.system('/home/pouwelse/postgres/bin/psql postgres -c "%s"' % cmd)

    @experiment_callback
    def init_config(self):
        """
        Initialize the Stellar configurations.
        """
        if self.is_client():
            return

        cmd = "/home/pouwelse/stellar-core/stellar-core gen-seed"
        proc = subprocess.Popen([cmd], stdout=subprocess.PIPE, shell=True)
        out, _ = proc.communicate()
        out = out.decode()

        lines = out.split("\n")
        seed = lines[0].split(" ")[-1]

        with open("/home/pouwelse/stellar-core/stellar-core-template.cfg", "r") as template_file:
            template_content = template_file.read()

        my_peer_id = self.experiment.scenario_runner._peernumber
        template_content = template_content.replace("<HTTP_PORT>", str(11000 + my_peer_id))
        template_content = template_content.replace("<NODE_SEED>", seed)
        template_content = template_content.replace("<DB_NAME>", "stellar_%d_db" % my_peer_id)
        template_content = template_content.replace("<PEER_PORT>", str(14000 + my_peer_id))

        # Fill in the known peers
        other_peers = []
        for other_peer_id in range(1, self.num_validators + 1):
            int_peer_id = int(other_peer_id)
            if int_peer_id == my_peer_id:
                continue

            ip, _ = self.experiment.get_peer_ip_port_by_id(int_peer_id)
            other_peers.append('"%s:%d"' % (ip, 14000 + int_peer_id))

        peers_str = ",".join(other_peers)

        template_content = template_content.replace("<KNOWN_PEERS>", '[%s]' % peers_str)

        with open("stellar-core.cfg", "w") as config_file:
            config_file.write(template_content)

    @experiment_callback
    def init_validators(self):
        """
        Initialize all validators.
        """
        if self.is_client():
            return

        cmd = "/home/pouwelse/stellar-core/stellar-core new-db"
        os.system(cmd)  # Blocking execution

        cmd = "/home/pouwelse/stellar-core/stellar-core force-scp"
        os.system(cmd)  # Blocking execution

    @experiment_callback
    def start_validators(self):
        """
        Start all Stellar validators.
        """
        if self.is_client():
            return

        cmd = "/home/pouwelse/stellar-core/stellar-core run 2>&1"
        self.validator_process = subprocess.Popen([cmd], shell=True, stdout=subprocess.DEVNULL)

    @experiment_callback
    def start_horizon(self):
        """
        Start the horizon interface.
        """
        if self.is_client():
            return

        my_peer_id = self.experiment.scenario_runner._peernumber
        db_name = "stellar_%d_db" % my_peer_id
        horizon_db_name = "stellar_horizon_%d_db" % my_peer_id
        cmd = '/home/pouwelse/horizon/horizon --port %d ' \
              '--ingest ' \
              '--db-url "postgresql://tribler:tribler@localhost:5432/%s?sslmode=disable" ' \
              '--stellar-core-db-url "postgresql://tribler:tribler@localhost:5432/%s?sslmode=disable" ' \
              '--stellar-core-url "http://127.0.0.1:%d" ' \
              '--network-passphrase="Standalone Pramati Network ; Oct 2018" ' \
              '--apply-migrations > horizon.out ' \
              '--log-level=info ' \
              '--per-hour-rate-limit 0 2>&1' % (19000 + my_peer_id, horizon_db_name, db_name, 11000 + my_peer_id)

        self.horizon_process = subprocess.Popen([cmd], shell=True)

    @experiment_callback
    def upgrade_tx_set_size(self):
        if self.is_client():
            return

        self._logger.info("Upgrading tx size limit")

        my_peer_id = self.experiment.scenario_runner._peernumber
        response = requests.get("http://127.0.0.1:%d/upgrades?mode=set&upgradetime=1970-01-01T00:00:00Z&maxtxsize=10000" % (11000 + my_peer_id,))
        self._logger.info("Response: %s", response.text)

    @experiment_callback
    def create_accounts(self):
        """
        Create accounts for every client. Send the secret seeds to the clients.
        """
        self._logger.info("Creating accounts...")

        my_peer_id = self.experiment.scenario_runner._peernumber
        validator_peer_id = ((my_peer_id - 1) % self.num_validators) + 1
        host, _ = self.experiment.get_peer_ip_port_by_id(validator_peer_id)
        horizon_uri = "http://%s:%d" % (host, 19000 + validator_peer_id)

        def append_create_account_op(builder, receiver_pub_key, amount):
            builder.append_create_account_op(receiver_pub_key, amount)
            if len(builder.ops) == 100:
                self._logger.info("Sending create transaction ops...")
                builder.sign()
                treq.post(horizon_uri + "/transactions/", data={"tx": builder.gen_xdr()})
                builder = builder.next_builder()

            return builder

        builder = Builder(secret="SDJ5AQWLIAYT22TCYSKOQALI3SNUMPAR63SEL73ASALDP6PYDN54FARM",
                          horizon_uri=horizon_uri,
                          network="Standalone Pramati Network ; Oct 2018")

        for client_index in range(self.num_validators + 1, self.num_validators + self.num_clients + 1):
            receiver_keypair = Keypair.random()
            receiver_pub_key = receiver_keypair.address().decode()
            builder = append_create_account_op(builder, receiver_pub_key, "100000000")
            self.experiment.send_message(client_index, b"receive_account_seed", receiver_keypair.seed())

            # Create the sender accounts
            for account_ind in range(self.num_accounts_per_client):
                sender_keypair = Keypair.random()
                sender_pub_key = sender_keypair.address().decode()
                builder = append_create_account_op(builder, sender_pub_key, "100000000")
                self.experiment.send_message(client_index, b"send_account_seed_%d" % account_ind, sender_keypair.seed())

        if len(builder.ops):
            self._logger.info("Sending create transaction ops...")
            builder.sign()
            treq.post(horizon_uri + "/transactions/", data={"tx": builder.gen_xdr()})


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

    @experiment_callback
    def transfer(self):
        if not self.is_client():
            return

        my_peer_id = self.experiment.scenario_runner._peernumber
        validator_peer_id = ((my_peer_id - 1) % self.num_validators) + 1
        host, _ = self.experiment.get_peer_ip_port_by_id(validator_peer_id)

        source_account_nr = self.current_tx_num % self.num_accounts_per_client
        self._logger.info("Will transfer from account %d, sq num: %d", source_account_nr, self.sequence_numbers[source_account_nr])

        builder = Builder(secret=self.sender_keypairs[source_account_nr].seed(),
                          horizon_uri="http://%s:%d" % (host, 19000 + validator_peer_id),
                          network="Standalone Pramati Network ; Oct 2018",
                          sequence=self.sequence_numbers[source_account_nr],
                          fee=100)
        builder.horizon.request_timeout = 60

        builder.append_payment_op(self.receiver_keypair.address(), '100', 'XLM')
        builder.sign()

        self._logger.info("Submitting transaction with id %d", self.sequence_numbers[source_account_nr])

        def send_transaction(tx, seq_num, account_nr):
            response = requests.get("http://%s:%d/tx?blob=%s" % (host, 11000 + validator_peer_id, quote_plus(tx.decode())))
            self._logger.info("Received response for transaction with id %d: %s", seq_num, response.text)
            if response.status_code != 200 or response.json()["status"] != "PENDING":
                # Restore seq num
                new_seq_num = builder.get_sequence()
                self.sequence_numbers[account_nr] = new_seq_num
                self._logger.info("Reset sequence number of account %d from %d to %d", account_nr, seq_num, new_seq_num)

        t = Thread(target=send_transaction, args=(builder.gen_xdr(), self.sequence_numbers[source_account_nr], source_account_nr))
        t.daemon = True
        t.start()

        self.sequence_numbers[source_account_nr] += 1
        self.current_tx_num += 1

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
    def print_metrics(self):
        if self.is_client():
            return

        my_peer_id = self.experiment.scenario_runner._peernumber
        horizon = Horizon("http://127.0.0.1:%d" % (19000 + my_peer_id),)
        metrics = horizon.metrics()
        print("Horizon metrics: %s" % metrics)

    @experiment_callback
    def print_ledgers(self):
        if self.is_client():
            return

        my_peer_id = self.experiment.scenario_runner._peernumber
        horizon = Horizon("http://127.0.0.1:%d" % (19000 + my_peer_id), )
        ledgers = horizon.ledgers(limit=100)
        print("Ledgers: %s" % ledgers)

    @experiment_callback
    def stop(self):
        print("Stopping Stellar...")
        if self.postgres_process:
            self._logger.info("Killing postgres")
            self.postgres_process.kill()
        if self.validator_process:
            self._logger.info("Killing validator")
            self.validator_process.kill()
        if self.horizon_process:
            self._logger.info("Killing horizon")
            self.horizon_process.kill()
        reactor.stop()
