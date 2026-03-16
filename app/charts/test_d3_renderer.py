import subprocess

import pytest

from app.charts.d3_renderer import render_chart, _RENDER_CHART_TOOL

VALID_D3 = """
const { D3Node } = require('d3-node');
const d3n = new D3Node();
const svg = d3n.createSVG(100, 100);
svg.append('circle').attr('r', 10);
process.stdout.write(d3n.svgString());
"""


class TestRenderChart:
    @pytest.mark.anyio
    async def test_should_return_svg_when_valid_d3_code(self):
        # Arrange / Act
        result = await render_chart(VALID_D3)
        # Assert
        assert result["success"] is True
        assert result["svg"] is not None
        assert "<svg" in result["svg"]
        assert result["errors"] == []

    @pytest.mark.anyio
    async def test_should_return_error_when_syntax_error(self):
        # Arrange
        code = "{{{ broken syntax ///"
        # Act
        result = await render_chart(code)
        # Assert
        assert result["success"] is False
        assert len(result["errors"]) > 0

    @pytest.mark.anyio
    async def test_should_return_error_on_runtime_exception(self):
        # Arrange
        code = "throw new Error('boom');"
        # Act
        result = await render_chart(code)
        # Assert
        assert result["success"] is False
        assert any("boom" in e for e in result["errors"])

    @pytest.mark.anyio
    async def test_should_handle_timeout(self, monkeypatch):
        # Arrange — patch subprocess.run to raise TimeoutExpired
        def fake_run(*args, **kwargs):
            raise subprocess.TimeoutExpired(cmd="node", timeout=10)

        monkeypatch.setattr("app.charts.d3_renderer.subprocess.run", fake_run)
        # Act
        result = await render_chart(VALID_D3)
        # Assert
        assert result["success"] is False
        assert any("timeout" in e.lower() for e in result["errors"])


class TestRenderChartTool:
    def test_should_expose_tool_schema_as_dict(self):
        assert isinstance(_RENDER_CHART_TOOL, dict)
        assert "name" in _RENDER_CHART_TOOL
        assert "description" in _RENDER_CHART_TOOL
        assert "input_schema" in _RENDER_CHART_TOOL

    def test_tool_schema_input_schema_has_d3_code_property(self):
        schema = _RENDER_CHART_TOOL["input_schema"]
        assert "d3_code" in schema["properties"]
        assert "d3_code" in schema["required"]
