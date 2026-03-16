"""
Static D3 code validation.

Design decision: uses `node --check` for JS syntax validation (fast, no execution)
rather than esprima/pyjsparser (not installed). SVG presence uses regex.
See context/decisions.md for rationale.
"""
import re
import subprocess
import tempfile
import os

# Matches svg in: d3.select("svg"), d3.select('#el').append('svg'), etc.
_SVG_PATTERN = re.compile(r"""(?:select|append)\s*\(\s*['"`][^'"` ]*svg[^'"` ]*['"`]\s*\)""", re.IGNORECASE)


def validate_d3(code: str) -> dict:
    """
    Perform static checks on D3 JavaScript code.

    Returns:
        {"valid": bool, "errors": list[str]}
    """
    errors: list[str] = []

    if not code or not code.strip():
        errors.append("Code is empty — provide a D3 snippet.")
        return {"valid": False, "errors": errors}

    # Syntax check via node --check
    syntax_error = _check_js_syntax(code)
    if syntax_error:
        errors.append(f"Syntax error: {syntax_error}")

    # SVG presence check
    if not _SVG_PATTERN.search(code):
        errors.append("No SVG reference found — code must select or append an 'svg' element.")

    return {"valid": len(errors) == 0, "errors": errors}


def _check_js_syntax(code: str) -> str | None:
    """Run `node --check` on a temp file. Returns error message or None."""
    with tempfile.NamedTemporaryFile(suffix=".js", mode="w", delete=False) as f:
        f.write(code)
        tmp = f.name
    try:
        result = subprocess.run(
            ["node", "--check", tmp],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            # Strip the temp file path from the error for cleaner output
            msg = result.stderr.strip().replace(tmp, "<code>")
            return msg
        return None
    except Exception as e:
        return str(e)
    finally:
        os.unlink(tmp)


_VALIDATE_D3_TOOL = {
    "name": "validate_d3",
    "description": (
        "Perform fast static checks on D3 JavaScript code before sandbox execution. "
        "Checks for syntax errors and the presence of an SVG element. "
        "Use this after generating or modifying D3 code to catch obvious issues early."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "code": {
                "type": "string",
                "description": "The D3 JavaScript code to validate.",
            },
        },
        "required": ["code"],
    },
}
