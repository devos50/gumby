import os
import subprocess

from jrpc import JRPCClientEndpoint, JRPCClientProtocol

from twisted.internet import reactor

from gumby.experiment import experiment_callback
from gumby.modules.experiment_module import static_module, ExperimentModule


class CallProtocol(JRPCClientProtocol):

    def onResponse(self, msg):
        print("DONEff")
        print(msg)

    def onError(self, msg):
        print(msg)

    def onOpen(self):
        d = self.request("mesh_getOrders", 1, 100, "").addCallback(self.onResponse).addErrback(self.onError)


@static_module
class MeshModule(ExperimentModule):

    def __init__(self, experiment):
        super(MeshModule, self).__init__(experiment)
        self.rpc_port = None
        self.rpc = None
        self.mesh_client = None

    @experiment_callback
    def start_mesh_client(self):
        my_env = os.environ.copy()
        my_env['VERBOSITY'] = "5"
        my_env['ETHEREUM_RPC_URL'] = "http://localhost:8546"
        my_env['ETHEREUM_NETWORK_ID'] = "50"
        self.rpc_port = 14000 + self.experiment.scenario_runner._peernumber
        my_env['RPC_PORT'] = str(self.rpc_port)
        self.mesh_client = subprocess.Popen(['/home/pouwelse/gocode/bin/mesh > %s 2>&1' % os.path.join(os.getcwd(), 'mesh_output.log')], shell=True, env=my_env)

    @experiment_callback
    def open_rpc_connection(self):
        self.rpc = JRPCClientEndpoint(CallProtocol, port=self.rpc_port)

    @experiment_callback
    def add_order(self):
        self.rpc.GetStats()

    @experiment_callback
    def stop(self):
        print("Stopping Mesh...")
        if self.mesh_client:
            self.mesh_client.kill()
        reactor.stop()
