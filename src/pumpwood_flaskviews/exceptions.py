from pumpwood_communication.exceptions import PumpWoodException


class PumpWoodFlaskViewEndPointFoundError(PumpWoodException):
    """Raised when a Pumpwood FlaskViews route is not registered."""

    status_code = 404

