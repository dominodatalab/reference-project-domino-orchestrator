from .api import DominoAPISession

def get_hardware_tier_id(tier_name):
    hw_tier_id = None

    if tier_name:
        domino_api = DominoAPISession.instance()

        for hardware_tier in domino_api.hardware_tiers_list():
            if tier_name.lower() == hardware_tier["hardwareTier"]["name"].lower():
                hw_tier_id = hardware_tier["hardwareTier"]["id"]

    return hw_tier_id