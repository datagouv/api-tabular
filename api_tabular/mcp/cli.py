#!/usr/bin/env python3
"""
CLI script to run the MCP server.
"""

import asyncio
import sys

from .server import main

if __name__ == "__main__":
    asyncio.run(main())
