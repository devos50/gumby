import json
import os
import shutil
import subprocess
from asyncio import get_event_loop

import toml

import yaml

from gumby.experiment import experiment_callback
from gumby.modules.blockchain_module import BlockchainModule
from gumby.modules.experiment_module import static_module


@static_module
class BurrowModule(BlockchainModule):

    def __init__(self, experiment):
        super(BurrowModule, self).__init__(experiment)
        self.burrow_process = None
        self.validator_address = None
        self.contract_address = None
        self.experiment.message_callback = self
        self.households_addresses = []
        self.clearing_results = []
        self.clearing_results_sw = None
        self.clearing_results_nb = None

    def is_household(self):
        return self.experiment.my_id != 1

    def get_deploy_command(self, script_name):
        return "burrow deploy --local-abi --address %s --chain 127.0.0.1:%d --bin-path /home/pouwelse/energy_trading_smart_contract/bin %s" % (self.validator_address, 16000 + self.experiment.my_id, script_name)

    def on_id_received(self):
        super(BurrowModule, self).on_id_received()

        if self.is_household():
            # Load clearing results
            household_nr = (self.experiment.my_id - 2) % 6 + 1
            tables_dir = os.path.join(os.path.dirname(__file__), "energytrading")
            file_path = os.path.join(tables_dir, "agent%d.txt" % household_nr)
            if not os.path.exists(file_path):
                self._logger.warning("Data file %s does not exist!", file_path)
                return

            with open(file_path, "r") as results_file:
                for line in results_file.readlines():
                    if not line:
                        continue

                    parts = line.strip().split(" ")
                    if len(parts) == 2:
                        self.clearing_results_sw = int(float(parts[0]) * 10E3)
                        self.clearing_results_nb = int(float(parts[1]) * 10E3)
                        continue

                    clearing_results = [int(float(clearing_result) * 10E5) for clearing_result in parts]
                    self.clearing_results = clearing_results

    def on_message(self, from_id, msg_type, msg):
        self._logger.info("Received message with type %s from peer %d", msg_type, from_id)
        if msg_type == b"validator_address":
            validator_address = msg.decode()
            self.households_addresses.append(validator_address)

    @experiment_callback
    def generate_config(self):
        """
        Generate the initial configuration files.
        """
        self._logger.info("Generating Burrow config...")

        # Remove old config directory
        shutil.rmtree("/home/pouwelse/energy_trading_data", ignore_errors=True)

        os.mkdir("/home/pouwelse/energy_trading_data")

        cmd = "cd /home/pouwelse/energy_trading_data && burrow spec --validator-accounts=6 --full-accounts=1 > genesis-spec.json"
        process = subprocess.Popen([cmd], shell=True, cwd='/home/pouwelse/energy_trading_data')
        process.wait()

        cmd = "burrow configure --genesis-spec=genesis-spec.json --pool"
        process = subprocess.Popen([cmd], shell=True, cwd='/home/pouwelse/energy_trading_data')
        process.wait()

    @experiment_callback
    def start_burrow(self):
        """
        Start Hyperledger Burrow.
        """
        config_path = os.path.join(os.getcwd(), "burrow_data")
        shutil.copytree("/home/pouwelse/energy_trading_data", config_path)

        burrow_config_file_name = "burrow00%d.toml" % (self.experiment.my_id - 1)
        burrow_config_file_path = os.path.join(config_path, burrow_config_file_name)

        with open(os.path.join(config_path, burrow_config_file_path), "r") as burrow_config_file:
            content = burrow_config_file.read()
            node_config = toml.loads(content)
            node_config["Tendermint"]["ListenPort"] = "%d" % (10000 + self.experiment.my_id)
            node_config["Tendermint"]["ListenHost"] = "0.0.0.0"
            node_config["RPC"]["Web3"]["ListenPort"] = "%d" % (12000 + self.experiment.my_id)
            node_config["RPC"]["Info"]["ListenPort"] = "%d" % (14000 + self.experiment.my_id)
            node_config["RPC"]["GRPC"]["ListenPort"] = "%d" % (16000 + self.experiment.my_id)

            self.validator_address = node_config["ValidatorAddress"]
            self._logger.info("Acting with validator address %s", self.validator_address)

            if self.experiment.my_id != 1:
                self.experiment.send_message(1, b"validator_address", self.validator_address.encode())

            # Fix the persistent peers
            persistent_peers = node_config["Tendermint"]["PersistentPeers"].split(",")
            for peer_ind in range(len(persistent_peers)):
                persistent_peer = persistent_peers[peer_ind]
                parts = persistent_peer.split(":")
                parts[-1] = "%d" % (10000 + peer_ind + 1)
                persistent_peer = ':'.join(parts)

                # Replace localhost IP
                host, _ = self.experiment.get_peer_ip_port_by_id(peer_ind + 1)
                persistent_peer = persistent_peer.replace("127.0.0.1", host)
                persistent_peers[peer_ind] = persistent_peer

            persistent_peers = ','.join(persistent_peers)
            node_config["Tendermint"]["PersistentPeers"] = persistent_peers

        with open(os.path.join(config_path, burrow_config_file_path), "w") as burrow_config_file:
            burrow_config_file.write(toml.dumps(node_config))

        cmd = "burrow start --index %d --config %s > output.log 2>&1" % (self.experiment.my_id - 1, burrow_config_file_name)
        self.burrow_process = subprocess.Popen([cmd], shell=True, cwd=config_path)

        self._logger.info("Burrow started...")

    @experiment_callback
    def deploy_contract(self):
        print("Deploying contract...")

        cmd = "burrow deploy --address %s --chain 127.0.0.1:16001 deploy.yaml" % self.validator_address
        process = subprocess.Popen([cmd], shell=True, cwd='/home/pouwelse/energy_trading_smart_contract')
        process.wait()

    def get_contract_address(self):
        """
        Read the contract address from the deployment output.
        """
        if self.contract_address:
            return self.contract_address

        with open("/home/pouwelse/energy_trading_smart_contract/deploy.output.json", "r") as deploy_output_file:
            content = deploy_output_file.read()
            json_content = json.loads(content)

        self.contract_address = json_content["deployEnergyTradingSmartContract"]
        return self.contract_address

    @experiment_callback
    def register_households_and_mint(self):
        """
        Register all households and mint some tokens for them.
        """
        self._logger.info("Registering %d households...", len(self.households_addresses))
        contract_address = self.get_contract_address()

        household_index = 1
        for household_address in self.households_addresses:
            yaml_json = {
                "jobs": []
            }

            register_job = {
                "name": "registerHousehold%d" % household_index,
                "call": {
                    "destination": contract_address,
                    "function": "registerHousehold",
                    "data": [household_address]
                }
            }

            mint_job = {
                "name": "mintTokensForHousehold%d" % household_index,
                "call": {
                    "destination": contract_address,
                    "function": "mintEuroToken",
                    "data": [household_address, 1000000000000000]
                }
            }

            yaml_json["jobs"].append(register_job)
            yaml_json["jobs"].append(mint_job)
            household_index += 1

            deploy_file_name = "registerhousehold%d.yaml" % household_index
            with open(deploy_file_name, "w") as out_file:
                out_file.write(yaml.dump(yaml_json))

            process = subprocess.Popen([self.get_deploy_command(deploy_file_name)], shell=True)

    @experiment_callback
    def print_euro_token_balance(self):
        self._logger.info("Getting euro token balance...")
        contract_address = self.get_contract_address()
        yaml_json = {
            "jobs": [{
                "name": "getEuroTokenBalance",
                "call": {
                    "destination": contract_address,
                    "function": "balanceOf",
                    "data": [self.validator_address]
                }
            }]
        }

        with open("getbalance.yaml", "w") as out_file:
            out_file.write(yaml.dump(yaml_json))

        process = subprocess.Popen([self.get_deploy_command("getbalance.yaml")], shell=True)

    @experiment_callback
    def set_role(self, role):
        if role not in ["buyer", "seller"]:
            return

        self._logger.info("Setting role to %s", role)

        role_bool = True if role == "buyer" else False
        contract_address = self.get_contract_address()
        yaml_json = {
            "jobs": [{
                "name": "setRole",
                "call": {
                    "destination": contract_address,
                    "function": "initializeRole",
                    "data": [role_bool]
                }
            }]
        }

        with open("setrole.yaml", "w") as out_file:
            out_file.write(yaml.dump(yaml_json))

        process = subprocess.Popen([self.get_deploy_command("setrole.yaml")], shell=True)

    @experiment_callback
    def post_clearing_results(self):
        if not self.is_household():
            return

        if not self.clearing_results:
            self._logger.warning("No clearing results to post!")
            return

        self._logger.info("Posting clearing results on blockchain")
        contract_address = self.get_contract_address()
        yaml_json = {
            "jobs": [{
                "name": "storeClearingResults",
                "call": {
                    "destination": contract_address,
                    "function": "storeClearingResults",
                    "data": [self.clearing_results, self.clearing_results_sw, self.clearing_results_nb]
                }
            }]
        }

        with open("postresults.yaml", "w") as out_file:
            out_file.write(yaml.dump(yaml_json))

        process = subprocess.Popen([self.get_deploy_command("postresults.yaml")], shell=True)

    @experiment_callback
    def received_energy(self):
        if not self.is_household():
            return

        self._logger.info("Notifying smart contract of received energy")
        contract_address = self.get_contract_address()
        yaml_json = {
            "jobs": [{
                "name": "receivedEnergy",
                "call": {
                    "destination": contract_address,
                    "function": "receivedEnergy"
                }
            }]
        }

        with open("receivedenergy.yaml", "w") as out_file:
            out_file.write(yaml.dump(yaml_json))

        process = subprocess.Popen([self.get_deploy_command("receivedenergy.yaml")], shell=True)

    @experiment_callback
    def stop_burrow(self):
        print("Stopping Burrow...")

        if self.burrow_process:
            self.burrow_process.terminate()

        loop = get_event_loop()
        loop.stop()
