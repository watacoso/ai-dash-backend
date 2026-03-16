import pytest

from app.charts.d3_validator import validate_d3, _VALIDATE_D3_TOOL


class TestValidateD3:
    def test_should_pass_when_valid_d3_snippet(self):
        # Arrange
        code = "const svg = d3.select('svg'); svg.append('circle').attr('r', 10);"
        # Act
        result = validate_d3(code)
        # Assert
        assert result["valid"] is True
        assert result["errors"] == []

    def test_should_fail_when_code_is_empty_string(self):
        # Arrange
        code = ""
        # Act
        result = validate_d3(code)
        # Assert
        assert result["valid"] is False
        assert any("empty" in e.lower() for e in result["errors"])

    def test_should_fail_when_code_is_whitespace_only(self):
        # Arrange
        code = "   \n  "
        # Act
        result = validate_d3(code)
        # Assert
        assert result["valid"] is False
        assert any("empty" in e.lower() for e in result["errors"])

    def test_should_fail_when_no_svg_call_present(self):
        # Arrange
        code = "d3.csv('data.csv').then(function(data) { console.log(data); });"
        # Act
        result = validate_d3(code)
        # Assert
        assert result["valid"] is False
        assert any("svg" in e.lower() for e in result["errors"])

    def test_should_pass_when_svg_append_used(self):
        # Arrange
        code = "const svg = d3.select('#chart').append('svg').attr('width', 400);"
        # Act
        result = validate_d3(code)
        # Assert
        assert result["valid"] is True
        assert result["errors"] == []

    def test_should_fail_when_syntax_error_present(self):
        # Arrange
        code = "d3.select('svg') {{{ broken syntax ///"
        # Act
        result = validate_d3(code)
        # Assert
        assert result["valid"] is False
        assert any("syntax" in e.lower() for e in result["errors"])

    def test_should_accumulate_multiple_errors(self):
        # Arrange — code that has syntax issues (invalid JS)
        code = "{{{"
        # Act
        result = validate_d3(code)
        # Assert
        assert result["valid"] is False
        assert len(result["errors"]) >= 1


class TestValidateD3Tool:
    def test_should_expose_tool_schema_as_dict(self):
        # Arrange / Act / Assert
        assert isinstance(_VALIDATE_D3_TOOL, dict)
        assert "name" in _VALIDATE_D3_TOOL
        assert "description" in _VALIDATE_D3_TOOL
        assert "input_schema" in _VALIDATE_D3_TOOL

    def test_tool_schema_input_schema_has_code_property(self):
        # Arrange
        schema = _VALIDATE_D3_TOOL["input_schema"]
        # Act / Assert
        assert "code" in schema["properties"]
        assert "code" in schema["required"]
