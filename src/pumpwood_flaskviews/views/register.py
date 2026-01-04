"""Functions to register views on Pumpwood."""
import os
import psycopg2
import sqlalchemy
from loguru import logger
from flask import jsonify
from marshmallow import ValidationError
from pumpwood_communication import exceptions
from pumpwood_database_error.psycopg2_error import TreatPsycopg2Error
from pumpwood_database_error.sqlalchemy_error import TreatSQLAlchemyError


def register_pumpwood_view(app, view, service_object: dict):
    """Register a pumpwood view.

    Args:
        app (Flask App):
            Flask app to register the PumpWood View
        view (PumpWoodFlaskView or PumpWoodDataFlaskView):
            View to be registered
        suffix (str):
            Sufix to be added to the begging of the of the model
            name.
        service_object (dict):
            Serialized object associated with service.

    Raises:
        No particular raises.

    """
    view.create_route_object(service_object=service_object)

    model_class_name = view.model_class.__name__
    suffix = os.getenv('ENDPOINT_SUFFIX', '')
    model_class_name = suffix + model_class_name

    url_no_args = '/rest/%s/<end_point>/' % model_class_name.lower()
    url_1_args = '/rest/%s/<end_point>/<first_arg>/' % model_class_name.lower()
    url_2_args = '/rest/%s/<end_point>/<first_arg>/<second_arg>/' % \
        model_class_name.lower()

    view_func = view.as_view()
    app.add_url_rule(url_no_args, view_func=view_func)
    app.add_url_rule(url_1_args, view_func=view_func)
    app.add_url_rule(url_2_args, view_func=view_func)

    @app.errorhandler(500)
    @logger.catch
    def handle_500_error(e):
        return "Internal Server Error", 500

    # Error handlers
    @app.errorhandler(exceptions.PumpWoodException)
    def handle_pumpwood_errors(error):
        response = jsonify(error.to_dict())
        response.status_code = error.status_code
        return response

    # Python errors
    @app.errorhandler(TypeError)
    def handle_type_errors(error):
        pump_exc = exceptions.PumpWoodException(message=str(error))
        response = jsonify(pump_exc.to_dict())
        response.status_code = pump_exc.status_code
        return response

    # SQLAlchemy errors
    @app.errorhandler(sqlalchemy.exc.SQLAlchemyError)
    def handle_sqlalchemy_programmingerror_errors(error):
        error_dict = TreatSQLAlchemyError.treat(
            error=error, connection_url=app.config['SQLALCHEMY_DATABASE_URI'])
        ErrorClass = exceptions.exceptions_dict.get(error_dict['type']) # NOQA
        if ErrorClass is None:
            msg = (
                "Error class returned by 'TreatSQLAlchemyError' [{type}] "
                "is not implemented on PumpwoodCommunication package.")\
                .format(type=error_dict['type'])
            raise NotImplementedError(msg)

        pump_exc = ErrorClass(
            message=error_dict['message'], payload=error_dict['payload'])
        response = jsonify(pump_exc.to_dict())
        response.status_code = pump_exc.status_code
        return response

    # psycopg2 error handlers
    @app.errorhandler(psycopg2.Error)
    def handle_psycopg2_error(error):
        error_dict = TreatPsycopg2Error.treat(
            error=error, connection_url=app.config['SQLALCHEMY_DATABASE_URI'])
        ErrorClass = exceptions.exceptions_dict.get(error_dict['type']) # NOQA
        pump_exc = ErrorClass(
            message=error_dict['message'], payload=error_dict['payload'])
        response = jsonify(pump_exc.to_dict())
        response.status_code = pump_exc.status_code
        return response

    # marshmallow errors
    @app.errorhandler(ValidationError)
    def handle_marshmallow_validationerror(error):
        message = "Error when saving object"
        messages_dict = error.messages
        pump_exc = exceptions.PumpWoodObjectSavingException(
            message=message, payload=messages_dict)
        response = jsonify(pump_exc.to_dict())
        response.status_code = pump_exc.status_code
        return response
