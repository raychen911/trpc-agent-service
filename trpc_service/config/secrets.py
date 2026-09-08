import asyncio
import base64
import os
from dataclasses import dataclass
from typing import Protocol
from urllib.parse import urlparse

import httpx


class SecretResolver(Protocol):
    async def resolve(self, reference: str) -> str: ...


class EnvironmentSecretResolver:
    """Resolve env://NAME references without exposing values in config objects or logs."""

    async def resolve(self, reference: str) -> str:
        parsed = urlparse(reference)
        if parsed.scheme != "env":
            raise ValueError(f"unsupported secret reference scheme: {parsed.scheme}")
        variable = (parsed.netloc + parsed.path).lstrip("/")
        if not variable:
            raise ValueError("environment secret reference is missing a variable name")
        value = os.environ.get(variable)
        if value is None:
            raise ValueError(f"required secret environment variable is not set: {variable}")
        return value


@dataclass(slots=True)
class VaultSecretResolver:
    address: str
    token: str
    namespace: str | None = None

    async def resolve(self, reference: str) -> str:
        parsed = urlparse(reference)
        if parsed.scheme != "vault":
            raise ValueError("Vault resolver requires vault:// references")
        path = (parsed.netloc + parsed.path).strip("/")
        fragment = parsed.fragment or "value"
        headers = {"X-Vault-Token": self.token}
        if self.namespace:
            headers["X-Vault-Namespace"] = self.namespace
        async with httpx.AsyncClient(base_url=self.address, timeout=10) as client:
            response = await client.get(f"/v1/{path}", headers=headers)
            response.raise_for_status()
        data = response.json().get("data", {})
        values = data.get("data", data)
        if fragment not in values:
            raise KeyError(f"Vault secret field not found: {fragment}")
        return str(values[fragment])


@dataclass(slots=True)
class AwsKmsSecretResolver:
    region: str | None = None

    async def resolve(self, reference: str) -> str:
        parsed = urlparse(reference)
        if parsed.scheme != "aws-kms":
            raise ValueError("KMS resolver requires aws-kms:// references")
        ciphertext = (parsed.netloc + parsed.path).lstrip("/")
        if not ciphertext:
            raise ValueError("KMS reference is missing base64 ciphertext")

        def decrypt() -> str:
            try:
                import boto3
            except ImportError as error:
                raise RuntimeError("install the security extra to use AWS KMS") from error
            client = boto3.client("kms", region_name=self.region)
            result = client.decrypt(CiphertextBlob=base64.urlsafe_b64decode(ciphertext))
            return bytes(result["Plaintext"]).decode("utf-8")

        return await asyncio.to_thread(decrypt)


class CompositeSecretResolver:
    def __init__(
        self,
        environment: EnvironmentSecretResolver | None = None,
        vault: VaultSecretResolver | None = None,
        kms: AwsKmsSecretResolver | None = None,
    ) -> None:
        self._resolvers = {"env": environment or EnvironmentSecretResolver()}
        if vault is not None:
            self._resolvers["vault"] = vault
        if kms is not None:
            self._resolvers["aws-kms"] = kms

    async def resolve(self, reference: str) -> str:
        scheme = urlparse(reference).scheme
        resolver = self._resolvers.get(scheme)
        if resolver is None:
            raise ValueError(f"unsupported or unconfigured secret scheme: {scheme}")
        return await resolver.resolve(reference)
