# -*- coding: utf-8 -*-
"""Classes and function for authetication and permission."""
import copy
import time
import requests
import urllib.parse
from loguru import logger
from flask import g
from flask import request as flask_request
from pumpwood_communication import exceptions
from pumpwood_communication.cache import default_cache
from pumpwood_flaskviews.config import (
    MICROSERVICE_URL, AUTHORIZATION_CACHE_TIMEOUT)


AUTH_CHECK_URL = urllib.parse.urljoin(
    MICROSERVICE_URL, 'rest/registration/check/')


class AuthFactory:
    """Create an auth decorator using the server_url provided.

    Args:
        server_url (str): Full path to auth server url, including conection
                          method and port (https://www.auth_server.com:5521/)
    Kwargs:
        No extra arguments

    Returns:
        func: Decorator to validate token auth.

    Raises:
        PumpWoodUnauthorized (Token autorization failed)
            If is not possible to validate token.

    Example:
        >>> pumpwood_auth = auth_factory(
                server_url='https://www.auth-server.com:80/')
        >>>
        >>> @pumpwood_auth
        >>> def view_function():
        >>>     ....

    """
    server_url = MICROSERVICE_URL
    auth_check_url = AUTH_CHECK_URL
    """Url that will be used to check if user is logged and if it has the right
       permissions"""
    dummy_auth = False

    @classmethod
    def _get_authenticated_user(cls, auth_header: dict) -> dict:
        """Request authenticated user from Auth App."""
        url = urllib.parse.urljoin(
            AuthFactory.server_url,
            '/rest/registration/retrieveauthenticateduser/')

        # Retry 3 times
        user_response = None
        for _ in range(3):
            user_response = requests.get(
                url, headers=auth_header, timeout=10)
            if user_response.status_code in [200, 401]:
                break

            # Wait before retry
            time.sleep(0.01)

        if user_response.status_code != 200:
            raise exceptions.PumpWoodUnauthorized('Token autorization failed')
        user_data = user_response.json()
        return user_data

    @classmethod
    def _get_user_all_row_permisson(cls, auth_header: dict) -> dict:
        """Request all row permissions associated with user."""
        url = urllib.parse.urljoin(
            AuthFactory.server_url,
            '/rest/userprofile/actions/self_row_permissions/')

        # Retry 3 times
        row_permission_response = None
        for _ in range(3):
            row_permission_response = requests.post(
                url, headers=auth_header, timeout=10, json={})
            if row_permission_response.status_code in [200, 401]:
                break

            # Wait before retry
            time.sleep(0.01)

        if row_permission_response.status_code != 200:
            raise exceptions.PumpWoodException(
                'It was not possible to retrieve user row permissions',
                payload=row_permission_response.json())
        resp_data = row_permission_response.json()
        return resp_data['result']

    @classmethod
    def set_server_url(cls, server_url: str = None):
        """Set server url after inicialization."""
        logger.warning(
            "Use of set_server_url at app startup is deprected, instead use "
            "`MICROSERVICE_URL` env variable")

    @classmethod
    def set_as_dummy(cls):
        """Set server url after inicialization."""
        cls.dummy_auth = True

    @classmethod
    def pumpwood_auth(cls, f):
        """Decorate function to check authorization."""
        def wrapped(*args, **kwargs):
            resp = cls.check_authorization()

# function_permitions = getattr(f, 'permitions')
# if function_permitions is not None:
#     intersection_perms = set(function_permitions).intersection(
#           set(data['all_permissions']))
#     if len(intersection_perms) == 0:
#         raise exceptions.PumpWoodUnauthorized(
#               'User do not have' + ' permition to execute this action')

            kwargs.update({'user': resp.json()})
            return f(*args, **kwargs)
        return wrapped

    @classmethod
    def get_authorization_hash_dict(cls, token: str, ingress_request: str,
                                    request_method: str = None,
                                    path: str = None,
                                    end_point: str = None,
                                    first_arg: str = None,
                                    second_arg: str = None) -> dict:
        """Build a dictonary to be used as hash dict for diskcache."""
        return {
            'context': 'authorization',
            'token': token,
            'ingress_request': ingress_request,
            'request_method': request_method,
            'path': path,
            'end_point': end_point,
            'first_arg': first_arg,
            'second_arg': second_arg}

    @classmethod
    def check_authorization(cls, request_method: str = None, path: str = None,
                            end_point: str = None, first_arg: str = None,
                            second_arg: str = None, payload_text: str = None):
        """Check if user is authenticated using Auth API.

        Raises:
            PumpWoodUnauthorized (Token autorization failed)
                If is not possible to validate token.

        Returns:
            bool: True if success.

        """
        if cls.dummy_auth is True:
            return "Dummy auth"

        if AuthFactory.server_url is None:
            raise Exception("AuthFactory.server_url not set")

        token = flask_request.headers.get('Authorization')
        if not token:
            raise exceptions.PumpWoodUnauthorized(
                'No Authorization header provided')
        ingress_request = flask_request.headers.get(
            'X-PUMPWOOD-Ingress-Request', 'NOT-EXTERNAL')

        hash_dict = cls.get_authorization_hash_dict(
            token=token, ingress_request=ingress_request,
            request_method=request_method, path=path,
            end_point=end_point, first_arg=first_arg,
            second_arg=second_arg)
        cache_result = default_cache.get(hash_dict)
        if cache_result is not None:
            logger.info('get cached authorization')
            return cache_result

        # Backward compatibility with previous Authorization Check
        auth_headers = {'Authorization': token}
        if ingress_request is not None:
            auth_headers['X-PUMPWOOD-Ingress-Request'] = ingress_request
        if request_method is None:
            resp = requests.get(
                cls.auth_check_url, headers=auth_headers, timeout=60)
        else:
            resp = requests.post(
                cls.auth_check_url, json={
                    'request_method': request_method,
                    'path': path, 'end_point': end_point,
                    'first_arg': first_arg, 'second_arg': second_arg,
                    'ingress_request': ingress_request},
                headers=auth_headers, timeout=60)

        # Raise PumpWoodUnauthorized is token is not valid
        if resp.status_code != 200:
            raise exceptions.PumpWoodUnauthorized(
                message='Token autorization failed',
                payload=resp.json())

        authorization_data = resp.json()
        default_cache.set(
            hash_dict=hash_dict, value=authorization_data,
            expire=AUTHORIZATION_CACHE_TIMEOUT)
        return flask_request

    @classmethod
    def get_user_hash_dict(cls, token: str) -> dict:
        """Get user hash dict for logged user."""
        return {
            'context': 'logged-user',
            'token': token
        }

    @classmethod
    def retrieve_authenticated_user(cls):
        """Retrieve user data using Auth API.

        Args:
            token (str): Token used in authentication.
        """
        # Raise errors if token is not set
        if AuthFactory.server_url is None:
            raise exceptions.PumpWoodUnauthorized(
                "AuthFactory.server_url not set")

        auth_header = cls.get_auth_header()
        if auth_header['Authorization'] is None:
            raise exceptions.PumpWoodUnauthorized(
                'No authorization token provided')

        # Try to fech logged user from g object (intra request cache)
        user = getattr(g, 'user', None)
        if user is not None:
            logger.info('get g cached user')
            return user

        # Try to get user from cache
        hash_dict = cls.get_user_hash_dict(
            token=auth_header['Authorization'])
        user = default_cache.get(hash_dict=hash_dict)
        if user is not None:
            logger.info('get diskcached user')
            g.user = user
            return user

        user_data = cls._get_authenticated_user(auth_header=auth_header)

        # Add all row permission to authenticated user
        all_row_permisson = cls._get_user_all_row_permisson(
            auth_header=auth_header)
        user_data['all_row_permisson_set'] = all_row_permisson

        # Set inforamtion on cache and g object #
        g.user = user_data
        default_cache.set(
            hash_dict=hash_dict, value=user_data,
            expire=AUTHORIZATION_CACHE_TIMEOUT)
        return user_data

    @classmethod
    def get_auth_header(cls):
        """Return auth header to use on microservice."""
        token = flask_request.headers.get('Authorization', None)
        return copy.deepcopy({'Authorization': token})

    @classmethod
    def user_has_row_permission(cls, row_permission_id: int,
                                raise_error: bool = True) -> bool:
        """Retrieve user data using Auth API.

        Args:
            row_permission_id (str):
                Check if user has row permission.
            raise_error (bool):
                Raise PumpWoodUnauthorized error if user does not have
                access to row_permission_id.

        Return:
            Returns True is user has access.
        """
        authenticated_user = cls.retrieve_authenticated_user()
        all_row_permisson_set = authenticated_user['all_row_permisson_set']

        all_row_permisson_ids = [
            permission['pk'] for permission in all_row_permisson_set]
        does_user_has_permission = row_permission_id in all_row_permisson_ids

        if raise_error and not does_user_has_permission:
            msg = (
                "User does not have access to this row permission "
                "[{row_permission_id}]")
            raise exceptions.PumpWoodUnauthorized(
                message=msg,
                payload={"row_permission_id": row_permission_id})
