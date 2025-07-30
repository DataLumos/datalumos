from contextlib import asynccontextmanager

from agents.mcp import MCPServerStdio


@asynccontextmanager
async def postgres_mcp_server(config):
    """Context manager for PostgreSQL MCP server connection"""
    postgres_mcp_params = {
        "command": "npx",
        "args": [
            "-y",
            "@modelcontextprotocol/server-postgres",
            config.postgres_config.connection_string,
            "--access-mode=restricted",
        ],
    }

    async with MCPServerStdio(params=postgres_mcp_params) as mcp_server:
        yield mcp_server
