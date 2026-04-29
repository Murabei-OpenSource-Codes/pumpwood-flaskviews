# -*- coding: utf-8 -*-
"""Classes and functions for authentication and permission."""
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
    """Factory to handle authentication and authorization checks.

    Provides utility methods to validate tokens, retrieve authenticated
    user data, and check row-level permissions through the
    authorization microservice.

    Example:
        >>> AuthFactory.set_as_dummy()
        >>> AuthFactory.check_authorization()
    """
    server_url = MICROSERVICE_URL
    auth_check_url = AUTH_CHECK_URL
    """Url that will be used to check if user is logged and if it has the right
       permissions"""
    dummy_auth = False

    @classmethod
    def _get_authenticated_user(cls, auth_header: dict) -> dict:
        """Request authenticated user data from the Auth microservice.

        Args:
            auth_header (dict):
                HTTP headers containing the Authorization token.

        Returns:
            dict:
                The user data returned by the authentication service.

        Raises:
            PumpWoodUnauthorized:
                If the token authorization fails after retries.
        """
        url = urllib.parse.urljoin(
            AuthFactory.server_url,
            '/rest/registration/retrieveauthenticateduser/')

        # Retry 3 times
        user_response = None
        for _ in range(3):
            user_response = requests.get(
                url, headers=auth_header, timeout=60)
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
        """Request all row permissions associated with the user.

        Args:
            auth_header (dict):
                HTTP headers containing the Authorization token.

        Returns:
            dict:
                A list of row permission objects assigned to the user.

        Raises:
            PumpWoodException:
                If the service fails to return the permissions.
        """
        url = urllib.parse.urljoin(
            AuthFactory.server_url,
            '/rest/userprofile/actions/self_row_permissions/')

        # Retry 3 times
        row_permission_response = None
        for _ in range(3):
            row_permission_response = requests.post(
                url, headers=auth_header, timeout=60, json={})
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
        """Set the authorization server URL.

        .. warning::
            This method is deprecated. Use the `MICROSERVICE_URL`
            environment variable instead.
        """
        logger.warning(
            "Use of set_server_url at app startup is deprected, instead use "
            "`MICROSERVICE_URL` env variable")

    @classmethod
    def set_as_dummy(cls):
        """Enable dummy authentication for testing purposes."""
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
        """Build a dictionary to be used for authorization caching.

        Args:
            token (str):
                The authorization token.
            ingress_request (str):
                The ingress request identifier.
            request_method (str):
                The HTTP method of the request.
            path (str):
                The request path.
            end_point (str):
                The specific endpoint being accessed.
            first_arg (str):
                The first argument of the endpoint.
            second_arg (str):
                The second argument of the endpoint.

        Returns:
            dict:
                The dictionary used for cache hashing.
        """
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
                            second_arg: str = None,
                            payload_text: str = None) -> object:
        """Check if the user is authenticated and authorized for the request.

        Args:
            request_method (str):
                The HTTP method.
            path (str):
                The request path.
            end_point (str):
                The target endpoint.
            first_arg (str):
                The first URL argument.
            second_arg (str):
                The second URL argument.
            payload_text (str):
                The request payload (logged if authorization fails).

        Returns:
            object:
                The Flask request object on success.

        Raises:
            PumpWoodUnauthorized:
                If the token authorization fails or is missing.
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
        """Create a dictionary for user data caching.

        Args:
            token (str):
                The authorization token.

        Returns:
            dict:
                The cache hash dictionary.
        """
        return {
            'context': 'logged-user',
            'token': token
        }

    @classmethod
    def retrieve_authenticated_user(cls) -> dict:
        """Retrieve authenticated user data from the cache or Auth API.

        Queries the authorization service for profile and permission data,
        caching the result in the current request (`g`) and the global
        cache.

        Returns:
            dict:
                The full authenticated user profile including permissions.

        Raises:
            PumpWoodUnauthorized:
                If the server URL is missing or authorization is invalid.
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
    def get_auth_header(cls) -> dict:
        """Extract the authorization header from the current Flask request.

        Returns:
            dict:
                A dictionary containing the 'Authorization' header.
        """
        token = flask_request.headers.get('Authorization', None)
        return copy.deepcopy({'Authorization': token})

    @classmethod
    def user_has_row_permission(cls, row_permission_id: int,
                                raise_error: bool = True) -> bool:
        """Check if the authenticated user has a specific row permission.

        Args:
            row_permission_id (int):
                The ID of the row permission to check.
            raise_error (bool):
                If True, raises a PumpWoodUnauthorized error if the
                permission is missing.

        Returns:
            bool:
                True if the user has the required permission.

        Raises:
            PumpWoodUnauthorized:
                If raise_error is True and permission is missing.
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
