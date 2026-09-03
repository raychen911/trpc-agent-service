from setuptools import setup, find_packages

setup(
    name="trpc_service",
    version="0.1.0",
    description="Multi-tenant Node-based Agent Deployment Platform based on tRPC-Agent",
    author="tRPC-Agent Team",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "trpc-agent = trpc_service._cli:main",
        ],
    },
    install_requires=[
        "pydantic>=2.0.0",
        "fastapi>=0.100.0",
        "uvicorn>=0.20.0",
        "sqlalchemy>=2.0.0",
        "prometheus_client>=0.16.0",
        "aiohttp>=3.8.0",
    ],
    python_requires=">=3.9",
)
