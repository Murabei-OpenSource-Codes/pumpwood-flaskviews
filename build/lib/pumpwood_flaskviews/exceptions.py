from pumpwood_communication.exceptions import PumpWoodException


class PumpWoodFlaskViewEndPointFoundError(PumpWoodException):
    """Raised when a route is not found."""
    status_code = 404

