"""
D3 chart rendering via a short-lived Node.js subprocess.

The user's D3 code is expected to write the SVG string to stdout via
`process.stdout.write(...)`. The subprocess has a 10-second timeout.

Requires: node (system) + d3-node package (npm install in backend root).
"""
import asyncio
import os
import subprocess
import tempfile
from pathlib import Path

# node_modules lives at the backend repo root (one level above app/)
_BACKEND_ROOT = Path(__file__).resolve().parent.parent.parent


async def render_chart(d3_code: str) -> dict:
    """
    Execute D3 code in a Node.js subprocess and capture the SVG output.

    Returns:
        {"success": bool, "svg": str | None, "errors": list[str]}
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _run_node, d3_code)


def _run_node(d3_code: str) -> dict:
    with tempfile.NamedTemporaryFile(suffix=".js", mode="w", delete=False) as f:
        f.write(d3_code)
        tmp = f.name

    env = os.environ.copy()
    # Ensure node_modules in backend root is on the module resolution path
    env["NODE_PATH"] = str(_BACKEND_ROOT / "node_modules")

    try:
        result = subprocess.run(
            ["node", "-e", f'require("module").Module._initPaths(); require("{tmp}")'],
            capture_output=True,
            text=True,
            timeout=10,
            env=env,
        )
        if result.returncode == 0:
            svg = result.stdout.strip() or None
            return {"success": True, "svg": svg, "errors": []}
        else:
            errors = [line for line in result.stderr.strip().splitlines() if line]
            return {"success": False, "svg": None, "errors": errors}

    except subprocess.TimeoutExpired:
        return {"success": False, "svg": None, "errors": ["Render timeout: Node.js subprocess exceeded 10 seconds."]}
    except Exception as e:
        return {"success": False, "svg": None, "errors": [str(e)]}
    finally:
        os.unlink(tmp)


_RENDER_CHART_TOOL = {
    "name": "render_chart",
    "description": (
        "Execute D3 JavaScript code in an isolated Node.js subprocess and return the resulting SVG string. "
        "The code must write the SVG to stdout via process.stdout.write(). "
        "Use this to verify that D3 code renders correctly before presenting it to the user."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "d3_code": {
                "type": "string",
                "description": "D3 JavaScript code that writes an SVG string to stdout.",
            },
        },
        "required": ["d3_code"],
    },
}
