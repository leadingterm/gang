import gc
import io
import json
import unittest
import warnings
from email.message import Message
from unittest.mock import patch
from urllib.error import HTTPError, URLError

from github_api import GitHubApi, GitHubApiError


class Response:
    def __init__(self, body=b'{}', status=200, link=None):
        self.body = body
        self.status = status
        self.headers = Message()
        if link is not None:
            self.headers['Link'] = link

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def read(self):
        return self.body


class FakeOpener:
    def __init__(self, *responses):
        self.responses = iter(responses)
        self.requests = []

    def open(self, request, timeout):
        self.requests.append((request, timeout))
        response = next(self.responses)
        if isinstance(response, Exception):
            raise response
        return response


class GitHubApiTest(unittest.TestCase):
    def api(self, *responses, **kwargs):
        opener = FakeOpener(*responses)
        return GitHubApi('example/repo', 'secret-token', opener=opener, **kwargs), opener

    def test_request_sends_json_and_returns_decoded_value(self):
        api, opener = self.api(Response(b'{"number": 4}'), timeout_seconds=7)
        self.assertEqual(api.repository, 'example/repo')
        self.assertEqual(api.request('POST', '/repos/example/repo/issues', {'title': 'hello'}), {'number': 4})
        request, timeout = opener.requests[0]
        self.assertEqual(request.full_url, 'https://api.github.com/repos/example/repo/issues')
        self.assertEqual(request.method, 'POST')
        self.assertEqual(json.loads(request.data), {'title': 'hello'})
        self.assertEqual(request.get_header('Authorization'), 'Bearer secret-token')
        self.assertEqual(request.get_header('Content-type'), 'application/json')
        self.assertEqual(timeout, 7)

    def test_empty_success_and_json_types(self):
        api, _ = self.api(Response(b'', 204), Response(b'[1, null, true]'))
        self.assertIsNone(api.request('DELETE', '/labels/x'))
        self.assertEqual(api.request('GET', '/list'), [1, None, True])

    def test_environment_precedence_and_defaults(self):
        with patch.dict('os.environ', {'GH_TOKEN': 'one', 'GITHUB_TOKEN': 'two', 'GH_REPO': 'a/b', 'GITHUB_REPOSITORY': 'c/d', 'GITHUB_API_URL': 'https://enterprise.example/api/v3'}, clear=True):
            api = GitHubApi.from_env()
            self.assertEqual(api.repository, 'a/b')
            self.assertEqual(api._route_url('/repos'), 'https://enterprise.example/api/v3/repos')
            self.assertEqual(api._token, 'one')
        with patch.dict('os.environ', {'GITHUB_TOKEN': 'two', 'GITHUB_REPOSITORY': 'c/d'}, clear=True):
            api = GitHubApi.from_env()
            self.assertEqual(api.repository, 'c/d')
            self.assertEqual(api._token, 'two')
            self.assertEqual(api._route_url('/repos'), 'https://api.github.com/repos')
        with patch.dict('os.environ', {}, clear=True), self.assertRaises(GitHubApiError):
            GitHubApi.from_env()

    def test_rejects_invalid_configuration_and_routes(self):
        for repo in ('', 'owner', '/repo', 'a/b/c', 'a/..', 'a/b\n'):
            with self.subTest(repo=repo), self.assertRaises(GitHubApiError):
                GitHubApi(repo, 'token')
        for url in ('https://user:pass@example.com', 'file:///tmp', 'https://example.com?x=1', 'https://example.com:bad'):
            with self.subTest(url=url), self.assertRaises(GitHubApiError):
                GitHubApi('a/b', 'token', api_url=url)
        for token in ('', 'token\nsecret'):
            with self.subTest(token=token), self.assertRaises(GitHubApiError):
                GitHubApi('a/b', token)
        api, opener = self.api()
        for route in ('repos', '//evil.example/x', '/x#fragment', '/x\nheader', '/\\evil'):
            with self.subTest(route=route), self.assertRaises(GitHubApiError):
                api.request('GET', route)
        self.assertEqual(opener.requests, [])

    def test_http_api_url_is_rejected_without_sending_token(self):
        opener = FakeOpener()
        with self.assertRaises(GitHubApiError):
            GitHubApi('example/repo', 'secret-token', api_url='http://api.example.com', opener=opener)
        self.assertEqual(opener.requests, [])

    def test_http_errors_have_status_and_do_not_leak_secrets(self):
        body = io.BytesIO(b'secret-token')
        error = HTTPError('https://example.test/secret-token', 403, 'secret-token', {}, body)
        api, _ = self.api(error, Response(b'secret-token', 429), URLError('secret-token'))
        for expected in (403, 429, None):
            with self.subTest(status=expected), self.assertRaises(GitHubApiError) as raised:
                api.request('GET', '/x')
            self.assertEqual(raised.exception.status, expected)
            self.assertNotIn('secret-token', str(raised.exception))
        self.assertTrue(error.closed)
        self.assertTrue(body.closed)

    def test_http_error_cleanup_emits_no_resource_warning(self):
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter('always', ResourceWarning)
            body = io.BytesIO(b'secret-token')
            error = HTTPError('https://example.test/', 403, 'forbidden', {}, body)
            opener = FakeOpener(error)
            api = GitHubApi('example/repo', 'secret-token', opener=opener)
            with self.assertRaises(GitHubApiError):
                api.request('GET', '/x')
            self.assertTrue(body.closed)
            del api, opener, error, body
            gc.collect()
        self.assertFalse([warning for warning in captured if issubclass(warning.category, ResourceWarning)])

    def test_invalid_json_or_payload_fails_without_leaking_contents(self):
        api, _ = self.api(Response(b'secret-token'), Response(b'NaN'), Response(b''))
        for _ in range(3):
            with self.assertRaises(GitHubApiError) as raised:
                api.request('GET', '/x')
            self.assertNotIn('secret-token', str(raised.exception))
        for value in (float('nan'), object()):
            with self.assertRaises(GitHubApiError):
                api.request('POST', '/x', value)

    def test_pagination_follows_relative_and_absolute_next_links(self):
        api, opener = self.api(
            Response(b'[1]', link='<https://api.github.com/items?page=2>; rel="next", <https://api.github.com/items?page=3>; rel="last"'),
            Response(b'[2]', link='<?page=3>; rel="prev next"'),
            Response(b'[3]'),
        )
        self.assertEqual(list(api.pages('/items?page=1')), [[1], [2], [3]])
        self.assertEqual([req.full_url for req, _ in opener.requests], [
            'https://api.github.com/items?page=1', 'https://api.github.com/items?page=2', 'https://api.github.com/items?page=3'])

    def test_pagination_rejects_cycles_and_untrusted_links(self):
        for link in ('<https://evil.example/items>; rel="next"', '<http://api.github.com/items>; rel="next"', '<https://api.github.com/items?page=1>; rel="next"', '<https://api.github.com/items#x>; rel="next"'):
            with self.subTest(link=link):
                api, opener = self.api(Response(b'[]', link=link))
                with self.assertRaises(GitHubApiError):
                    list(api.pages('/items?page=1'))
                self.assertEqual(len(opener.requests), 1)

    def test_enterprise_pagination_stays_inside_api_base(self):
        api, opener = self.api(Response(b'[]', link='<https://enterprise.example/other>; rel="next"'), api_url='https://enterprise.example/api/v3')
        with self.assertRaises(GitHubApiError):
            list(api.pages('/repos'))
        self.assertEqual(len(opener.requests), 1)

    def test_malformed_and_ambiguous_links_fail_closed(self):
        for link in ('not a link', '<https://api.github.com/x>; rel="next", <https://api.github.com/y>; rel="next"'):
            with self.subTest(link=link):
                api, _ = self.api(Response(b'[]', link=link))
                with self.assertRaises(GitHubApiError):
                    list(api.pages('/items'))


if __name__ == '__main__':
    unittest.main()
