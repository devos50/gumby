from gumby.modules.community_experiment_module import IPv8OverlayExperimentModule
from gumby.modules.experiment_module import static_module

from ipv8.attestation.backbone.community import NoodleCommunity


@static_module
class PlexusModule(IPv8OverlayExperimentModule):

    def __init__(self, experiment):
        super(PlexusModule, self).__init__(experiment, NoodleCommunity)

    def on_id_received(self):
        super(PlexusModule, self).on_id_received()
        self.tribler_config.set_trustchain_enabled(False)

    def on_ipv8_available(self, _):
        self.overlay._use_main_thread = False
