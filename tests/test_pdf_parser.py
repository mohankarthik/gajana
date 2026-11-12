import base64
import io
import json
from unittest.mock import MagicMock, patch

import pytest
from pypdf import PdfWriter

from src.llm_client import LLMGatewayError, LLMUnavailableError
from src.pdf_parser import PDFParser, _model_to_family


def get_minimal_pdf_bytes(encrypted=False, password=""):
    writer = PdfWriter()
    writer.add_blank_page(width=100, height=100)
    if encrypted:
        writer.encrypt(password)
    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


def make_llm_response(transactions: list, summary: dict | None = None) -> dict:
    payload: dict = {"transactions": transactions}
    if summary is not None:
        payload["summary"] = summary
    return {"ok": True, "data": payload, "text": json.dumps(payload)}


@pytest.fixture(autouse=True)
def mock_gateway_available(monkeypatch):
    from src import pdf_parser

    monkeypatch.setattr(pdf_parser.llm_client, "is_available", lambda *a, **kw: True)


def test_model_to_family_mapping():
    assert _model_to_family("gemini") == "gemini"
    assert _model_to_family("gemini/gemini-2.5-flash") == "gemini"
    assert _model_to_family("claude") == "claude"
    assert _model_to_family("claude-sonnet-4-6") == "claude"
    assert _model_to_family("anthropic/claude-sonnet-4-6") == "claude"
    assert _model_to_family("unknown-model") is None
    assert _model_to_family("") is None


def test_pdf_parser_no_api_key(monkeypatch):
    from src import pdf_parser

    monkeypatch.setattr(pdf_parser.llm_client, "is_available", lambda *a, **kw: False)
    parser = PDFParser()
    assert parser.parse_pdf(b"dummy pdf bytes") == []


@patch("src.pdf_parser.llm_client.complete")
def test_pdf_parser_success(mock_complete):
    mock_complete.return_value = make_llm_response(
        [
            {
                "date": "2026-05-02",
                "description": "uber ride",
                "debit": "200.00",
                "credit": "",
            },
            {
                "date": "2026-05-03",
                "description": "salary",
                "debit": "",
                "credit": "50000.00",
            },
        ]
    )

    txns = PDFParser().parse_pdf(get_minimal_pdf_bytes())

    assert len(txns) == 2
    assert txns[0]["description"] == "uber ride"
    assert txns[0]["debit"] == "200.00"
    assert txns[1]["credit"] == "50000.00"
    mock_complete.assert_called_once()
    assert mock_complete.call_args.kwargs["family"] == "gemini"
    assert mock_complete.call_args.kwargs["title"] == "gajana_pdf"


@patch("src.pdf_parser.llm_client.complete")
def test_call_llm_with_pdf_accepts_base64_str(mock_complete):
    mock_complete.return_value = {
        "ok": True,
        "data": {
            "transactions": [
                {
                    "date": "2026-05-02",
                    "description": "b64 caller",
                    "debit": "10.00",
                    "credit": "",
                }
            ]
        },
    }
    raw = get_minimal_pdf_bytes()
    b64_str = base64.b64encode(raw).decode("ascii")
    txns, _summary = PDFParser()._call_llm_with_pdf("gemini", b64_str)
    assert len(txns) == 1
    assert txns[0]["description"] == "b64 caller"
    att = mock_complete.call_args.kwargs["attachments"][0]
    assert att["data"] == raw


@patch("src.pdf_parser.llm_client.complete")
def test_pdf_parser_encrypted_success(mock_complete):
    mock_complete.return_value = make_llm_response(
        [
            {
                "date": "2026-05-02",
                "description": "uber ride",
                "debit": "200.00",
                "credit": "",
            }
        ]
    )

    txns = PDFParser().parse_pdf(
        get_minimal_pdf_bytes(encrypted=True, password="correctpassword"),
        password="correctpassword",
    )

    assert len(txns) == 1
    assert txns[0]["description"] == "uber ride"


@patch("src.pdf_parser.llm_client.complete")
def test_pdf_parser_encrypted_wrong_password(mock_complete):
    txns = PDFParser().parse_pdf(
        get_minimal_pdf_bytes(encrypted=True, password="correctpassword"),
        password="wrongpassword",
    )
    assert txns == []
    mock_complete.assert_not_called()


@patch("src.pdf_parser.llm_client.complete")
def test_pdf_parser_encrypted_backup_password(mock_complete):
    """A rotated password: the current one is listed first, the statement is
    still locked with the old one, and the backup opens it."""
    mock_complete.return_value = make_llm_response(
        [
            {
                "date": "2026-05-02",
                "description": "uber ride",
                "debit": "200.00",
                "credit": "",
            }
        ]
    )

    txns = PDFParser().parse_pdf(
        get_minimal_pdf_bytes(encrypted=True, password="oldpassword"),
        password=["newpassword", "oldpassword"],
    )

    assert len(txns) == 1
    assert txns[0]["description"] == "uber ride"


@patch("src.pdf_parser.llm_client.complete")
def test_pdf_parser_encrypted_all_passwords_wrong(mock_complete):
    txns = PDFParser().parse_pdf(
        get_minimal_pdf_bytes(encrypted=True, password="correctpassword"),
        password=["wrong1", "wrong2"],
    )
    assert txns == []
    mock_complete.assert_not_called()


def test_password_candidates_normalizes():
    from src.pdf_parser import password_candidates

    assert password_candidates(None) == []
    assert password_candidates("") == []
    assert password_candidates("pw") == ["pw"]
    assert password_candidates(["a", "", "b"]) == ["a", "b"]
    assert password_candidates([]) == []


@patch("src.pdf_parser.llm_client.complete")
def test_pdf_parser_encrypted_no_password(mock_complete):
    txns = PDFParser().parse_pdf(
        get_minimal_pdf_bytes(encrypted=True, password="somepassword"),
        password="",
    )
    assert txns == []
    mock_complete.assert_not_called()


@patch("src.pdf_parser.llm_client.complete")
def test_pdf_parser_primary_fails_fallback_succeeds(mock_complete):
    fallback_response = make_llm_response(
        [
            {
                "date": "2026-05-02",
                "description": "fallback txn",
                "debit": "20.00",
                "credit": "",
            }
        ]
    )
    mock_complete.side_effect = [
        LLMUnavailableError("Primary unavailable"),
        fallback_response,
    ]

    txns = PDFParser().parse_pdf(get_minimal_pdf_bytes())

    assert len(txns) == 1
    assert txns[0]["description"] == "fallback txn"
    assert mock_complete.call_count == 2
    families = [c.kwargs["family"] for c in mock_complete.call_args_list]
    assert families == ["gemini", "claude"]


@patch("src.pdf_parser.PdfReader")
@patch("src.pdf_parser.llm_client.complete")
def test_pdf_parser_both_models_fail_text_extraction_succeeds(
    mock_complete, mock_reader_class
):
    mock_reader = MagicMock()
    mock_reader_class.return_value = mock_reader
    mock_reader.is_encrypted = False
    mock_page = MagicMock()
    mock_page.extract_text.return_value = "mocked extracted transaction text"
    mock_reader.pages = [mock_page]

    text_response = make_llm_response(
        [
            {
                "date": "2026-05-02",
                "description": "text txn",
                "debit": "50.00",
                "credit": "",
            }
        ]
    )
    mock_complete.side_effect = [
        LLMGatewayError("Primary PDF failed"),
        LLMGatewayError("Fallback PDF failed"),
        text_response,
    ]

    txns = PDFParser().parse_pdf(b"pdf_bytes")

    assert len(txns) == 1
    assert txns[0]["description"] == "text txn"
    assert mock_complete.call_count == 3
    assert mock_complete.call_args_list[2].kwargs["title"] == "gajana_pdf_text"


@patch("src.pdf_parser.PdfReader")
@patch("src.pdf_parser.llm_client.complete")
def test_pdf_parser_all_fail_empty_text(mock_complete, mock_reader_class):
    mock_reader = MagicMock()
    mock_reader_class.return_value = mock_reader
    mock_reader.is_encrypted = False
    mock_page = MagicMock()
    mock_page.extract_text.return_value = ""
    mock_reader.pages = [mock_page]

    mock_complete.side_effect = [
        LLMGatewayError("Primary failed"),
        LLMGatewayError("Fallback failed"),
    ]

    txns = PDFParser().parse_pdf(b"pdf_bytes")
    assert txns == []


@patch("src.pdf_parser.llm_client.complete")
def test_pdf_parser_json_decode_error(mock_complete):
    mock_complete.return_value = {"ok": True, "text": "not valid json"}

    txns = PDFParser().parse_pdf(get_minimal_pdf_bytes())
    assert txns == []


@patch("src.pdf_parser.PdfReader")
def test_pdf_parser_general_exception(mock_reader_class):
    mock_reader_class.side_effect = Exception("Unexpected corrupt PDF")

    txns = PDFParser().parse_pdf(b"dummy pdf bytes")
    assert txns == []


@patch("src.pdf_parser.llm_client.complete")
def test_pdf_parser_response_as_bare_array(mock_complete):
    """_parse_response handles bare JSON arrays (not wrapped in object)."""
    bare = [
        {
            "date": "2026-05-01",
            "description": "bare array txn",
            "debit": "10",
            "credit": "",
        }
    ]
    mock_complete.return_value = {"ok": True, "data": bare, "text": json.dumps(bare)}

    txns = PDFParser().parse_pdf(get_minimal_pdf_bytes())
    assert len(txns) == 1
    assert txns[0]["description"] == "bare array txn"


@patch("src.pdf_parser.llm_client.complete")
def test_parse_pdf_with_text_returns_oracle_text(mock_complete):
    """parse_pdf_with_text returns (txns, extracted_text, summary); text is the
    validation oracle and must come back even on a successful vision parse."""
    mock_complete.return_value = make_llm_response(
        [{"date": "06/06/2026", "description": "shop", "debit": "80", "credit": ""}]
    )

    txns, text, summary = PDFParser().parse_pdf_with_text(get_minimal_pdf_bytes())

    assert len(txns) == 1
    assert txns[0]["date"] == "06/06/2026"
    assert isinstance(text, str)
    assert isinstance(summary, dict)


@patch("src.pdf_parser.llm_client.complete")
def test_parse_pdf_with_text_extracts_summary(mock_complete):
    """The statement's printed totals are surfaced as the summary cross-check."""
    mock_complete.return_value = make_llm_response(
        [{"date": "06/06/2026", "description": "shop", "debit": "80"}],
        summary={"total_debit": "80", "total_credit": "0"},
    )

    txns, _text, summary = PDFParser().parse_pdf_with_text(get_minimal_pdf_bytes())

    assert len(txns) == 1
    assert summary == {"total_debit": "80", "total_credit": "0"}


@patch("src.pdf_parser.llm_client.complete")
def test_parse_pdf_with_text_model_override(mock_complete):
    """Passing models=[...] drives the retry path (fallback model first)."""
    mock_complete.return_value = make_llm_response(
        [{"date": "06/06/2026", "description": "shop", "debit": "80", "credit": ""}]
    )

    parser = PDFParser()
    parser.parse_pdf_with_text(get_minimal_pdf_bytes(), models=[parser.fallback_model])

    assert mock_complete.call_args.kwargs["family"] == "claude"
