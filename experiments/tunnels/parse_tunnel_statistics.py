#!/usr/bin/env python2
import json
import os
import re
import sys


class TunnelStatisticsParser(object):
    """
    This class is responsible for parsing statistics of the tunnels
    """

    def __init__(self, node_directory):
        self.node_directory = node_directory
        self.circuits_info = []
        self.relays_info = []

    def yield_files(self, file_to_check='market_stats.log'):
        pattern = re.compile('[0-9]+')

        # TODO: this only contains support for a localhost structure
        for peer in os.listdir(self.node_directory):
            peerdir = os.path.join(self.node_directory, peer)
            if os.path.isdir(peerdir) and pattern.match(peer):
                peer_nr = int(peer)

                filename = os.path.join(self.node_directory, peer, file_to_check)
                if os.path.exists(filename) and os.stat(filename).st_size > 0:
                    yield peer_nr, filename, peerdir

    def aggregate_trustchain_balances(self):
        with open('trustchain_balances.csv', 'w', 0) as balances_file:
            balances_file.write('peer,total_up,total_down,balance\n')
            for peer_nr, filename, dir in self.yield_files(file_to_check='triblerchain.txt'):
                with open(filename) as tc_file:
                    tc_json = json.loads(tc_file.read())
                    total_up = 0
                    total_down = 0
                    balance = 0
                    if 'latest_block' in tc_json:
                        total_up = tc_json['latest_block']['transaction']['total_up']
                        total_down = tc_json['latest_block']['transaction']['total_down']
                        balance = total_up - total_down
                    balances_file.write('%s,%d,%d,%d\n' % (peer_nr, total_up, total_down, balance))

    def aggregate_introduction_points(self):
        with open('introduction_points.csv', 'w', 0) as ips_file:
            ips_file.write("peer,infohash\n")
            for peer_nr, filename, dir in self.yield_files(file_to_check='introduction_points.txt'):
                with open(filename) as ip_file:
                    ips_file.write(ip_file.read())

    def aggregate_rendezvous_points(self):
        with open('rendezvous_points.csv', 'w', 0) as rps_file:
            rps_file.write("peer,cookie\n")
            for peer_nr, filename, dir in self.yield_files(file_to_check='rendezvous_points.txt'):
                with open(filename) as rp_file:
                    rps_file.write(rp_file.read())

    def aggregate_downloads_history(self):
        with open('downloads_history.csv', 'w', 0) as downloads_file:
            downloads_file.write('peer,time,infohash,progress,status,total_up,total_down,speed_up,speed_down\n')
            for peer_nr, filename, dir in self.yield_files(file_to_check='downloads_history.txt'):
                with open(filename) as individual_downloads_file:
                    lines = individual_downloads_file.readlines()
                    for line in lines:
                        downloads_file.write('%s,%s' % (peer_nr, line))

    def aggregate_circuits(self):
        with open('circuits.csv', 'w', 0) as circuits_file:
            circuits_file.write('peer,circuit_id,state,hops,bytes_up,bytes_down,creation_time,type,first_hop\n')
            for peer_nr, filename, dir in self.yield_files(file_to_check='circuits.txt'):
                with open(filename) as individual_circuits_file:
                    lines = individual_circuits_file.readlines()
                    for line in lines:
                        circuits_file.write('%s,%s' % (peer_nr, line))

                        # Get the information about the circuit
                        parts = line.split(',')
                        circuit_id = parts[0]
                        circuit_type = parts[6]
                        first_hop = parts[7]
                        bytes_transferred = int(parts[3]) + int(parts[4])
                        self.circuits_info.append((peer_nr, circuit_id, circuit_type, first_hop, bytes_transferred))

    def aggregate_relays(self):
        with open('relays.csv', 'w', 0) as relays_file:
            relays_file.write('peer,circuit_id_1,circuit_id_2,destination,bytes_up\n')
            for peer_nr, filename, dir in self.yield_files(file_to_check='relays.txt'):
                with open(filename) as individual_relays_file:
                    lines = individual_relays_file.readlines()
                    for line in lines:
                        relays_file.write('%s,%s' % (peer_nr, line))

                        # Get the information about the relay
                        parts = line.split(',')
                        circuit_id_1 = parts[0]
                        circuit_id_2 = parts[1]
                        destination = parts[2]
                        bytes_up = int(parts[3])
                        self.relays_info.append((peer_nr, circuit_id_1, circuit_id_2, destination, bytes_up))

    def get_peer_id_from_address(self, address):
        return int(address.split(":")[1]) - 12000

    def get_relay_circuit_totals(self, circuit_id):
        total = 0
        for relay_peer, circuit_id_1, circuit_id_2, destination, bytes_up in self.relays_info:
            if circuit_id_1 == circuit_id:
                total += bytes_up
        return total

    def build_circuits_graph(self):
        edges = []  # Keep track of all edges (from, to, circuit_num, type)
        cur_circuit_num = 1
        for peer_nr, circuit_id, circuit_type, first_hop, bytes_transferred in self.circuits_info:
            if circuit_type == "RENDEZVOUS":
                continue

            edges.append((peer_nr, self.get_peer_id_from_address(first_hop), cur_circuit_num, circuit_type, bytes_transferred))

            cur_circuit_id = circuit_id
            cur_circuit_peer = self.get_peer_id_from_address(first_hop)
            while True:  # Iterate over relays of this particular circuit
                found = False
                for relay_peer, circuit_id_1, circuit_id_2, destination, _ in self.relays_info:
                    if circuit_id_1 == cur_circuit_id and relay_peer == cur_circuit_peer:
                        relay_dest_peer = self.get_peer_id_from_address(destination)
                        edges.append((cur_circuit_peer, relay_dest_peer, cur_circuit_num, circuit_type, self.get_relay_circuit_totals(cur_circuit_id)))
                        cur_circuit_id = circuit_id_2
                        cur_circuit_peer = relay_dest_peer
                        found = True

                if not found:
                    break

            cur_circuit_num += 1

        # Write circuits to file
        with open('circuits_graph.csv', 'w', 0) as circuits_graph_file:
            circuits_graph_file.write('from,to,circuit_num,type,bytes_transferred\n')
            for from_peer, to_peer, circuit_num, type, bytes_transferred in edges:
                circuits_graph_file.write('%s,%s,%s,%s,%d\n' % (from_peer, to_peer, circuit_num, type, bytes_transferred))

    def run(self):
        self.aggregate_trustchain_balances()
        self.aggregate_introduction_points()
        self.aggregate_rendezvous_points()
        self.aggregate_downloads_history()
        self.aggregate_circuits()
        self.aggregate_relays()
        self.build_circuits_graph()


# cd to the output directory
os.chdir(os.environ['OUTPUT_DIR'])

parser = TunnelStatisticsParser(sys.argv[1])
parser.run()
