import random

from schwifty import IBAN
from twisted.internet import reactor
from twisted.internet.task import LoopingCall, deferLater

from experiments.internetofmoney.bank_server import BankServer
from experiments.internetofmoney.managers import Dummy1Manager, Dummy2Manager, Dummy3Manager, Dummy4Manager, Dummy5Manager
from gumby.experiment import experiment_callback
from gumby.modules.community_experiment_module import CommunityExperimentModule
from gumby.modules.experiment_module import static_module


from internetofmoney.database import InternetOfMoneyDB
from internetofmoney.address import Address
from internetofmoney.moneycommunity.community import MoneyCommunity
from internetofmoney.utils.iban import IBANUtil


@static_module
class IOMModule(CommunityExperimentModule):

    def __init__(self, experiment):
        super(IOMModule, self).__init__(experiment, MoneyCommunity)

        self.bank_server = None
        self.candidate_connections_lc = None
        self.candidate_connections_lc_tick = 1
        self.candidate_connections_history = []

        self.stolen_lc = None
        self.stolen_lc_tick = 1
        self.stolen_history = []

        self.total_payments = 0
        self.my_banks = []

    @experiment_callback
    def init_db(self):
        self.community.database = InternetOfMoneyDB('.')
        self.community.split_parts = 5

    @experiment_callback
    def initialize_manager(self, mgr_name):
        self._logger.info("Initializing manager %s", mgr_name)
        if mgr_name == 'DUMA':
            dummy_mgr = Dummy1Manager(self.community.database)
        elif mgr_name == 'DUMB':
            dummy_mgr = Dummy2Manager(self.community.database)
        elif mgr_name == 'DUMC':
            dummy_mgr = Dummy3Manager(self.community.database)
        elif mgr_name == 'DUMD':
            dummy_mgr = Dummy4Manager(self.community.database)
        elif mgr_name == 'DUME':
            dummy_mgr = Dummy5Manager(self.community.database)

        peer_num = self.experiment.scenario_runner._peernumber
        iban = IBAN.generate('NL', dummy_mgr.get_bank_id(), str(peer_num).zfill(9))

        # Set the right IP of the bank server
        server_ip, _ = self.experiment.get_peer_ip_port_by_id(1)
        dummy_mgr.bank_ip = server_ip

        dummy_mgr.persistent_storage['is_switching'] = True
        dummy_mgr.iban = str(iban)
        self.community.bank_managers[dummy_mgr.get_bank_id()] = dummy_mgr

        self.my_banks.append(mgr_name)
        deferLater(reactor, random.random() * 20, dummy_mgr.register, str(iban))  # Reduce load on bank server

    @experiment_callback
    def start_log_connections_loop(self):
        self._logger.info("Starting write connections loop")
        self.candidate_connections_history.append("0 ")
        self.candidate_connections_lc = LoopingCall(self.write_connections)
        self.candidate_connections_lc.start(5, now=False)

    @experiment_callback
    def start_log_stolen_loop(self):
        self._logger.info("Starting write stolen loop")
        self.stolen_history.append("0 0\n")
        self.stolen_lc = LoopingCall(self.write_stolen)
        self.stolen_lc.start(5, now=False)

    @experiment_callback
    def set_malicious(self, percentage):
        self._logger.info("Making node malicious")
        self.community.is_malicious = True
        self.community.steal_probability = float(percentage)

    def perform_random_payment(self):
        eligible_candidates = []
        for candidate, services_map in self.community.candidate_services_map.iteritems():
            if len(services_map.keys()) == 1 and services_map.keys()[0] not in self.my_banks:
                eligible_candidates.append(candidate)

        if len(eligible_candidates) == 0:
            self._logger.error("Unable to find suitable target for random payment!")
            return

        random_candidate = random.choice(eligible_candidates)
        _, random_account = random.choice(self.community.candidate_services_map[random_candidate].items())
        _, source_manager = random.choice(self.community.bank_managers.items())
        random_amount = round(random.uniform(0.01, 1000), 2)
        self.perform_fast_payment(source_manager.get_address().address, random_account, random_amount, target_candidate=random_candidate, target_circuits=2)

        deferLater(reactor, 5 + random.random() * 5, self.perform_random_payment)

    @experiment_callback
    def start_transactions(self):
        """
        Start performing random transactions to others if we only own one bank account
        """
        self.perform_random_payment()

    def stop_log_connections_loop(self):
        self.candidate_connections_lc.stop()

    def stop_log_stolen_loop(self):
        self.stolen_lc.stop()

    def write_connections(self):
        connected_peers = []
        self._logger.info("Writing candidate connections: %s", str(self.community.candidate_services_map))
        for candidate, _ in self.community.candidate_services_map.iteritems():
            connected_peers.append(str(candidate.sock_addr[1] - 12000))

        self.candidate_connections_history.append("%d %s" % (self.candidate_connections_lc_tick * 5, ' '.join(connected_peers)))
        self.candidate_connections_lc_tick += 1

    def write_stolen(self):
        self._logger.info("Logging stolen money...")
        self.stolen_history.append("%d %.2f\n" % (self.stolen_lc_tick * 5, self.community.stolen_amount))
        self.stolen_lc_tick += 1

    @experiment_callback
    def connect_to_all_peers(self):
        for peer_num in self.experiment.get_peers():
            self._logger.info("Connecting to peer %s", peer_num)
            host, port = self.experiment.get_peer_ip_port_by_id(peer_num)
            walk_candidate = self.community.create_or_update_walkcandidate((host, port), ('0.0.0.0', 0),
                                                                           (host, port), False, u'unknown')
            walk_candidate.set_keepalive(self.community)
            self.community.create_introduction_request(walk_candidate, self.community.dispersy_enable_bloom_filter_sync)

    @experiment_callback
    def start_bank_server(self):
        self.bank_server = BankServer()
        self.bank_server.start()

    @experiment_callback
    def stop_bank_server(self):
        self.bank_server.stop()

    def log_circuit(self, circuit):
        """
        Log the details of the established circuit
        """
        self._logger.info("Built circuit with router paths:")
        router_paths = circuit.get_router_paths()
        for path in router_paths:
            self._logger.info(' -> '.join(['%s:%d (%s to %s)' % (router.sock_addr[0], router.sock_addr[1], addresses["in"], addresses["out"]) for router, addresses in path]))

    @experiment_callback
    def perform_fast_payment(self, target_peer_id, amount, target_bank):
        peer_num = self.experiment.scenario_runner._peernumber
        from_iban = IBAN.generate('NL', self.my_banks[0], str(peer_num).zfill(9))
        to_iban = IBAN.generate('NL', target_bank, str(target_peer_id).zfill(9))

        from_address = Address(self.my_banks[0], str(from_iban))
        to_address = Address(target_bank, str(to_iban))

        target_sock_addr = self.experiment.get_peer_ip_port_by_id(int(target_peer_id))

        if from_address.bank_id not in self.community.bank_managers.keys():
            self._logger.error("Address %s not found!", from_address)

        from_manager = self.community.bank_managers[from_address.bank_id]

        def on_circuit(circuit):
            if not circuit:
                self._logger.error("Unable to build circuit for transfer from %s to %s!", from_address, to_address)
                return circuit
            else:
                self.log_circuit(circuit)

            self.total_payments += float(amount)
            self.community.send_money_using_circuit(circuit, from_manager, float(amount), to_address, 'experiment', destination_sock_addr=target_sock_addr)

        def on_circuit_error(failure):
            self._logger.error("Error when building circuit: %s", str(failure))

        self.community.build_money_circuit(from_address, to_address, target_circuits=1, destination_sock_addr=target_sock_addr)\
            .addCallbacks(on_circuit, on_circuit_error)

    @experiment_callback
    def write_stats(self):
        with open('candidate_connections_evolve.log', 'w') as candidates_file:
            self._logger.info("Writing %d lines to evolve connections", len(self.candidate_connections_history))
            for line in self.candidate_connections_history:
                candidates_file.write("%s\n" % line)

        with open('candidate_services.log', 'w', 0) as services_file:
            for candidate, services_map in self.community.candidate_services_map.iteritems():
                services_file.write("(%s:%d) -> %s\n" % (candidate.sock_addr[0], candidate.sock_addr[1], ' '.join(services_map.keys())))

        with open('stolen.log', 'w') as stolen_file:
            for line in self.stolen_history:
                stolen_file.write("%s" % line)

        with open('total_payments.log', 'w') as payments_file:
            payments_file.write('%.2f' % self.total_payments)

        if self.bank_server:
            self.bank_server.write_stats()
