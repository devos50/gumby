import os

from ruamel.yaml import YAML, RoundTripDumper, round_trip_dump
from ruamel.yaml.comments import CommentedMap
from twisted.internet import reactor

from gumby.experiment import experiment_callback
from gumby.modules.experiment_module import static_module, ExperimentModule


@static_module
class HyperledgerModule(ExperimentModule):

    def __init__(self, experiment):
        super(HyperledgerModule, self).__init__(experiment)
        self.config_path = "/home/pouwelse/hyperledger-network-template"
        self.num_validators = int(os.environ["NUM_VALIDATORS"])

    @experiment_callback
    def generate_config(self):
        """
        Generate the initial configuration files.
        """

        # Change crypto-config.yaml and add organizations
        yaml = YAML()
        with open(os.path.join(self.config_path, "crypto-config-template.yaml"), "r") as crypto_config_file:
            config = yaml.load(crypto_config_file)

        config["OrdererOrgs"][0]["Specs"] = []
        for orderer_index in range(1, self.num_validators + 1):
            config["OrdererOrgs"][0]["Specs"].append({"Hostname": "orderer%d" % orderer_index})

        config["PeerOrgs"] = []
        for organization_index in range(1, self.num_validators + 1):
            organization_config = {
                "Name": "Org%d" % organization_index,
                "Domain": "org%d.example.com" % organization_index,
                "EnableNodeOUs": True,
                "Template": {
                    "Count": 1
                },
                "Users": {
                    "Count": 1
                }
            }
            config["PeerOrgs"].append(organization_config)

        with open(os.path.join(self.config_path, "crypto-config.yaml"), "w") as crypto_config_file:
            yaml.dump(config, crypto_config_file)

        # Change configtx.yaml
        yaml = YAML()
        with open(os.path.join(self.config_path, "configtx-template.yaml"), "r") as configtx_file:
            config = yaml.load(configtx_file)

        config["Profiles"]["TwoOrgsChannel"]["Application"]["Organizations"] = []
        config["Profiles"]["SampleMultiNodeEtcdRaft"]["Consortiums"]["SampleConsortium"]["Organizations"] = []

        for organization_index in range(1, self.num_validators + 1):
            org_admin = "Org%dMSP.admin" % organization_index
            org_peer = "Org%dMSP.peer" % organization_index
            org_client = "Org%dMSP.client" % organization_index

            organization_config = {
                "Name": "Org%dMSP" % organization_index,
                "ID": "Org%dMSP" % organization_index,
                "MSPDir": "crypto-config/peerOrganizations/org%d.example.com/msp" % organization_index,
                "Policies": {
                    "Readers": {
                        "Type": "Signature",
                        "Rule": "OR('%s', '%s', '%s')" % (org_admin, org_peer, org_client)
                    },
                    "Writers": {
                        "Type": "Signature",
                        "Rule": "OR('%s', '%s')" % (org_admin, org_client)
                    },
                    "Admins": {
                        "Type": "Signature",
                        "Rule": "OR('%s')" % (org_admin)
                    }
                },
                "AnchorPeers": [{
                    "Host": "peer0.org%d.example.com" % organization_index,
                    "Port": 7050 + organization_index
                }]
            }

            commented_map = CommentedMap(organization_config)
            commented_map.yaml_set_anchor("Org%d" % organization_index, always_dump=True)
            config["Organizations"].append(commented_map)
            config["Profiles"]["TwoOrgsChannel"]["Application"]["Organizations"].append(commented_map)
            config["Profiles"]["SampleMultiNodeEtcdRaft"]["Consortiums"]["SampleConsortium"]["Organizations"].append(commented_map)

        config["Profiles"]["SampleMultiNodeEtcdRaft"]["Orderer"]["EtcdRaft"]["Consenters"] = []
        config["Profiles"]["SampleMultiNodeEtcdRaft"]["Orderer"]["Addresses"] = []

        for organization_index in range(1, self.num_validators + 1):
            consenter_info = {
                "Host": "orderer%d.example.com" % organization_index,
                "Port": 7050,
                "ClientTLSCert": "crypto-config/ordererOrganizations/example.com/orderers/orderer%d.example.com/tls/server.crt" % organization_index,
                "ServerTLSCert": "crypto-config/ordererOrganizations/example.com/orderers/orderer%d.example.com/tls/server.crt" % organization_index
            }
            config["Profiles"]["SampleMultiNodeEtcdRaft"]["Orderer"]["EtcdRaft"]["Consenters"].append(consenter_info)
            config["Profiles"]["SampleMultiNodeEtcdRaft"]["Orderer"]["Addresses"].append("orderer%d.example.com:7050" % organization_index)

        with open(os.path.join(self.config_path, "configtx.yaml"), "w") as configtx_file:
            round_trip_dump(config, configtx_file, Dumper=RoundTripDumper)

        # Change docker-composer for orderers
        yaml = YAML()
        with open(os.path.join(self.config_path, "docker-compose-orderers-template.yaml"), "r") as composer_file:
            config = yaml.load(composer_file)

        config["volumes"] = {}
        config["services"] = {}
        for orderer_index in range(1, self.num_validators + 1):
            name = "orderer%d.example.com" % orderer_index
            config["volumes"][name] = None

            orderer_info = {
                "extends": {
                    "file": "base/peer-base.yaml",
                    "service": "orderer-base"
                },
                "container_name": name,
                "networks": ["byfn"],
                "volumes": [
                    "./channel-artifacts/genesis.block:/var/hyperledger/orderer/orderer.genesis.block",
                    "./crypto-config/ordererOrganizations/example.com/orderers/orderer%d.example.com/msp:/var/hyperledger/orderer/msp" % orderer_index,
                    "./crypto-config/ordererOrganizations/example.com/orderers/orderer%d.example.com/tls/:/var/hyperledger/orderer/tls" % orderer_index,
                    "orderer%d.example.com:/var/hyperledger/production/orderer" % orderer_index
                ],
                "ports": ["%d:7050" % (7050 + 1000 * (orderer_index - 1))]
            }

            config["services"][name] = orderer_info

        with open(os.path.join(self.config_path, "docker-compose-orderers.yaml"), "w") as composer_file:
            yaml.dump(config, composer_file)

        # Change docker-composer for peers
        yaml = YAML()
        with open(os.path.join(self.config_path, "docker-compose-cli-template.yaml"), "r") as composer_file:
            config = yaml.load(composer_file)

        config["volumes"] = {}
        for organization_index in range(1, self.num_validators + 1):
            name = "peer0.org%d.example.com" % organization_index
            config["volumes"][name] = None

            peer_info = {
                "container_name": name,
                "extends": {
                    "file": "base/peer-base.yaml",
                    "service": "peer-base",
                },
                "environment": [
                    "CORE_PEER_ID=peer0.org%d.example.com" % organization_index,
                    "CORE_PEER_ADDRESS=peer0.org%d.example.com:%d" % (organization_index, 6051 + 1000 * organization_index),
                    "CORE_PEER_LISTENADDRESS=0.0.0.0:%d" % (6051 + 1000 * organization_index),
                    "CORE_PEER_CHAINCODEADDRESS=peer0.org%d.example.com:%d" % (organization_index, 6052 + 1000 * organization_index),
                    "CORE_PEER_CHAINCODELISTENADDRESS=0.0.0.0:%d" % (6052 + 1000 * organization_index),
                    "CORE_PEER_GOSSIP_BOOTSTRAP=peer0.org1.example.com:7051",
                    "CORE_PEER_GOSSIP_EXTERNALENDPOINT=peer0.org1.example.com:7051",
                    "CORE_PEER_LOCALMSPID=Org%dMSP" % organization_index,
                    "CORE_LEDGER_STATE_STATEDATABASE=CouchDB",
                    "CORE_LEDGER_STATE_COUCHDBCONFIG_COUCHDBADDRESS=couchdb%d:5984" % (organization_index - 1),
                    "CORE_LEDGER_STATE_COUCHDBCONFIG_USERNAME=",
                    "CORE_LEDGER_STATE_COUCHDBCONFIG_PASSWORD="

                ],
                "volumes": [
                    "/var/run/:/host/var/run/",
                    "./crypto-config/peerOrganizations/org%d.example.com/peers/peer0.org%d.example.com/msp:/etc/hyperledger/fabric/msp" % (organization_index, organization_index),
                    "./crypto-config/peerOrganizations/org%d.example.com/peers/peer0.org%d.example.com/tls:/etc/hyperledger/fabric/tls" % (organization_index, organization_index),
                    "peer0.org%d.example.com:/var/hyperledger/production" % organization_index
                ],
                "ports": ["%d:%d" % (6051 + 1000 * organization_index, 6051 + 1000 * organization_index)],
                "networks": ["byfn"],
                "depends_on": ["couchdb%d" % (organization_index - 1)]
            }

            config["services"][name] = peer_info

        with open(os.path.join(self.config_path, "docker-compose-cli.yaml"), "w") as composer_file:
            yaml.dump(config, composer_file)

        # Change docker-composer for couchdb
        yaml = YAML()
        with open(os.path.join(self.config_path, "docker-compose-couch-template.yaml"), "r") as composer_file:
            config = yaml.load(composer_file)

        config["services"] = {}
        for organization_index in range(1, self.num_validators + 1):
            couch_name = "couchdb%d" % (organization_index - 1)
            couchdb_info = {
                "container_name": couch_name,
                "image": "hyperledger/fabric-couchdb",
                "environment": [
                    "COUCHDB_USER=",
                    "COUCHDB_PASSWORD="
                ],
                "ports": ["%d:5984" % (5984 + 1000 * (organization_index - 1))],
                "networks": ["byfn"]
            }

            config["services"][couch_name] = couchdb_info

        with open(os.path.join(self.config_path, "docker-compose-couch.yaml"), "w") as composer_file:
            yaml.dump(config, composer_file)

        # Generate the configuration
        os.system(os.path.join(self.config_path, "generate.sh") + " %d" % self.num_validators)

    @experiment_callback
    def stop(self):
        print("Stopping Hyperledger...")
        reactor.stop()
