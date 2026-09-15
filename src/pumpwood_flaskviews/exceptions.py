from pumpwood_communication.exceptions import PumpWoodException


class PumpWoodFlaskViewEndPointFoundError(PumpWoodException):
    """Raised for an unknown view endpoint or HTTP method combination."""

    status_code = 404

