"""Small authenticated GitHub JSON transport for CI tooling."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Iterator
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


class GitHubApiError(Exception):
    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


def _origin(url: str) -> tuple[str, str, int]:
    try:
        parts = urlsplit(url)
        if parts.scheme != "https" or not parts.hostname or parts.username is not None or parts.password is not None:
            raise ValueError
        return parts.scheme, parts.hostname.lower(), parts.port or 443
    except ValueError:
        raise GitHubApiError("invalid GitHub API URL") from None


def _reject_constant(value: str) -> None:
    raise ValueError("invalid JSON constant")


class _SameOriginRedirect(HTTPRedirectHandler):
    def __init__(self, origin: tuple[str, str, int]) -> None:
        self.origin = origin

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if _origin(newurl) != self.origin:
            raise GitHubApiError("cross-origin GitHub API redirect rejected")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class GitHubApi:
    def __init__(
        self,
        repository: str,
        token: str,
        api_url: str = "https://api.github.com",
        timeout_seconds: float = 30,
        opener=None,
    ) -> None:
        if not isinstance(repository, str) or not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository) or any(part in (".", "..") for part in repository.split("/")):
            raise GitHubApiError("invalid GitHub repository; expected owner/repo")
        if not isinstance(token, str) or not token or any(c in token for c in "\r\n"):
            raise GitHubApiError("missing or invalid GitHub token")
        if not isinstance(api_url, str):
            raise GitHubApiError("invalid GitHub API URL")
        self._origin = _origin(api_url)
        parts = urlsplit(api_url)
        if parts.query or parts.fragment:
            raise GitHubApiError("invalid GitHub API URL")
        if not isinstance(timeout_seconds, (int, float)) or isinstance(timeout_seconds, bool) or not 0 < timeout_seconds < float("inf"):
            raise GitHubApiError("invalid GitHub API timeout")
        self.repository = repository
        self._token = token
        self._base = api_url.rstrip("/")
        self._base_path = parts.path.rstrip("/")
        self._timeout = timeout_seconds
        self._opener = opener if opener is not None else build_opener(_SameOriginRedirect(self._origin))

    @classmethod
    def from_env(cls) -> GitHubApi:
        return cls(
            os.environ.get("GH_REPO") or os.environ.get("GITHUB_REPOSITORY", ""),
            os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN", ""),
            os.environ.get("GITHUB_API_URL") or "https://api.github.com",
        )

    def _route_url(self, route: str) -> str:
        if not isinstance(route, str) or not route.startswith("/") or route.startswith("//") or "#" in route or "\\" in route or any(ord(c) < 32 or ord(c) == 127 for c in route):
            raise GitHubApiError("invalid GitHub API relative route")
        return self._base + route

    def _trusted_url(self, url: str) -> str:
        parts = urlsplit(url)
        if _origin(url) != self._origin or parts.fragment or (self._base_path and not (parts.path == self._base_path or parts.path.startswith(self._base_path + "/"))):
            raise GitHubApiError("untrusted GitHub API pagination link")
        return url

    def _request(self, method: str, url: str, payload: object = None) -> tuple[object, str | None]:
        self._trusted_url(url)
        if not isinstance(method, str) or not re.fullmatch(r"[A-Z]+", method):
            raise GitHubApiError("invalid GitHub API method")
        try:
            body = None if payload is None else json.dumps(payload, allow_nan=False).encode("utf-8")
        except (TypeError, ValueError, UnicodeError):
            raise GitHubApiError("invalid GitHub API JSON payload") from None
        headers = {"Accept": "application/vnd.github+json", "Authorization": f"Bearer {self._token}", "X-GitHub-Api-Version": "2022-11-28"}
        if body is not None:
            headers["Content-Type"] = "application/json"
        request = Request(url, data=body, headers=headers, method=method)
        try:
            open_request = self._opener.open if hasattr(self._opener, "open") else self._opener
            with open_request(request, timeout=self._timeout) as response:
                raw = response.read()
                link = response.headers.get("Link")
                status = response.status
        except HTTPError as error:
            raise GitHubApiError(f"GitHub API HTTP {error.code}", status=error.code) from None
        except (URLError, OSError, ValueError):
            raise GitHubApiError("GitHub API request failed") from None
        if not 200 <= status < 300:
            raise GitHubApiError(f"GitHub API HTTP {status}", status=status)
        if not raw and status in (204, 205):
            return None, link
        try:
            value: object = json.loads(raw, parse_constant=_reject_constant)
        except (ValueError, UnicodeError, TypeError):
            raise GitHubApiError("invalid GitHub API JSON response") from None
        return value, link

    def request(self, method: str, relative_route: str, payload: object = None) -> object:
        return self._request(method, self._route_url(relative_route), payload)[0]

    def pages(self, relative_route: str) -> Iterator[object]:
        url = self._route_url(relative_route)
        seen: set[str] = set()
        while True:
            self._trusted_url(url)
            if url in seen:
                raise GitHubApiError("GitHub API pagination cycle")
            seen.add(url)
            value, link = self._request("GET", url)
            yield value
            if not link:
                return
            next_url = None
            for entry in re.split(r',(?=\s*<)', link):
                match = re.fullmatch(r'\s*<([^>]*)>\s*((?:;\s*[^;]+)*)\s*', entry)
                if not match:
                    raise GitHubApiError("invalid GitHub API pagination link")
                for parameter in match.group(2).split(";"):
                    rel = re.fullmatch(r'\s*rel\s*=\s*(?:"([^"]*)"|([^\s]+))\s*', parameter, re.IGNORECASE)
                    if rel and "next" in (rel.group(1) or rel.group(2)).split():
                        if next_url is not None:
                            raise GitHubApiError("ambiguous GitHub API pagination link")
                        next_url = urljoin(url, match.group(1))
            if next_url is None:
                return
            url = self._trusted_url(next_url)
