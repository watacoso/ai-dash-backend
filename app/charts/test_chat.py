import uuid
from unittest.mock import MagicMock

import pytest

from app.charts.conftest import auth_headers  # noqa: F401 (imported for IDE; pytest uses conftest auto-discovery)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _make_text_response(text: str) -> MagicMock:
    block = MagicMock()
    block.type = "text"
    block.text = text
    resp = MagicMock()
    resp.stop_reason = "end_turn"
    resp.content = [block]
    return resp


def _make_tool_use_response(tool_name: str, tool_id: str, tool_input: dict) -> MagicMock:
    block = MagicMock()
    block.type = "tool_use"
    block.id = tool_id
    block.name = tool_name
    block.input = tool_input
    resp = MagicMock()
    resp.stop_reason = "tool_use"
    resp.content = [block]
    return resp


def _mock_anthropic(mocker, responses, module: str = "app.charts.router"):
    """responses: list of MagicMock responses returned sequentially."""
    mock_create = MagicMock(side_effect=responses)
    mocker.patch(
        f"{module}.anthropic.Anthropic",
        return_value=MagicMock(messages=MagicMock(create=mock_create)),
    )
    return mock_create


def _chat_payload(cl_id, datasource_id, messages=None, d3_code=""):
    return {
        "claude_connection_id": str(cl_id),
        "datasource_id": str(datasource_id),
        "messages": messages or [{"role": "user", "content": "create a bar chart"}],
        "d3_code": d3_code,
    }


# ── _extract_d3 unit tests ─────────────────────────────────────────────────────

class TestExtractD3Code:
    def test_should_extract_from_d3_fence(self):
        from app.charts.router import _extract_d3
        text = "Here is the chart:\n```d3\nd3.select('svg');\n```"
        assert _extract_d3(text) == "d3.select('svg');"

    def test_should_extract_from_js_fence(self):
        from app.charts.router import _extract_d3
        text = "Here:\n```js\nconst svg = d3.select('svg');\n```"
        assert _extract_d3(text) == "const svg = d3.select('svg');"

    def test_should_return_none_when_no_fence(self):
        from app.charts.router import _extract_d3
        assert _extract_d3("plain text response") is None


# ── Ad-hoc chat ────────────────────────────────────────────────────────────────

class TestChartChatAdHoc:
    async def test_should_return_chat_response_adhoc(
        self, client, admin_token, dataset, cl_connection, mocker
    ):
        # Arrange
        _mock_anthropic(mocker, [_make_text_response("Here:\n```d3\nd3.select('svg');\n```")])
        # Act
        r = await client.post(
            "/charts/chat",
            json=_chat_payload(cl_connection.id, dataset.id),
            headers=auth_headers(admin_token),
        )
        # Assert
        assert r.status_code == 200
        body = r.json()
        assert body["role"] == "assistant"
        assert body["d3_code_update"] == "d3.select('svg');"

    async def test_should_return_404_when_claude_connection_not_found(
        self, client, admin_token, dataset
    ):
        # Arrange
        payload = _chat_payload(uuid.uuid4(), dataset.id)
        # Act
        r = await client.post(
            "/charts/chat",
            json=payload,
            headers=auth_headers(admin_token),
        )
        # Assert
        assert r.status_code == 404

    async def test_should_return_max_iterations_message_when_loop_exhausted(
        self, client, admin_token, dataset, cl_connection, mocker
    ):
        # Arrange — Claude always returns tool_use, never end_turn
        tool_resp = _make_tool_use_response("validate_d3", "tid1", {"code": "x"})
        text_after_tool = MagicMock()
        text_after_tool.type = "text"
        text_after_tool.text = "ok"
        tool_resp.content = [tool_resp.content[0]]
        _mock_anthropic(mocker, [tool_resp] * 10)
        # Act
        r = await client.post(
            "/charts/chat",
            json=_chat_payload(cl_connection.id, dataset.id),
            headers=auth_headers(admin_token),
        )
        # Assert
        assert r.status_code == 200
        assert "iterations limit" in r.json()["content"]

    async def test_should_call_validate_d3_tool_when_requested(
        self, client, admin_token, dataset, cl_connection, mocker
    ):
        # Arrange — first call returns tool_use for validate_d3, second returns end_turn
        tool_resp = _make_tool_use_response("validate_d3", "tid1", {"code": "d3.select('svg');"})
        final_resp = _make_text_response("Looks good!")
        mock_create = _mock_anthropic(mocker, [tool_resp, final_resp])
        # Act
        r = await client.post(
            "/charts/chat",
            json=_chat_payload(cl_connection.id, dataset.id),
            headers=auth_headers(admin_token),
        )
        # Assert
        assert r.status_code == 200
        assert mock_create.call_count == 2

    async def test_should_call_render_chart_tool_when_requested(
        self, client, admin_token, dataset, cl_connection, mocker
    ):
        # Arrange
        tool_resp = _make_tool_use_response("render_chart", "tid2", {"d3_code": "d3.select('svg');"})
        final_resp = _make_text_response("Chart rendered!")
        mock_create = _mock_anthropic(mocker, [tool_resp, final_resp])
        # Act
        r = await client.post(
            "/charts/chat",
            json=_chat_payload(cl_connection.id, dataset.id),
            headers=auth_headers(admin_token),
        )
        # Assert
        assert r.status_code == 200
        assert mock_create.call_count == 2


# ── Saved chart chat ───────────────────────────────────────────────────────────

class TestChartChatSaved:
    async def test_should_append_version_on_saved_chart_chat(
        self, client, admin_token, chart, cl_connection, session, mocker
    ):
        # Arrange
        _mock_anthropic(mocker, [_make_text_response("Done!\n```d3\nd3.select('svg');\n```")])
        # Act
        r = await client.post(
            f"/charts/{chart.id}/chat",
            json=_chat_payload(cl_connection.id, chart.datasource_id),
            headers=auth_headers(admin_token),
        )
        # Assert
        assert r.status_code == 200
        await session.refresh(chart)
        assert len(chart.versions) == 1
        assert chart.versions[0]["d3_code"] == "d3.select('svg');"

    async def test_should_return_404_when_chart_not_found(
        self, client, admin_token, cl_connection, dataset, mocker
    ):
        # Arrange
        _mock_anthropic(mocker, [_make_text_response("ok")])
        # Act
        r = await client.post(
            f"/charts/{uuid.uuid4()}/chat",
            json=_chat_payload(cl_connection.id, dataset.id),
            headers=auth_headers(admin_token),
        )
        # Assert
        assert r.status_code == 404
