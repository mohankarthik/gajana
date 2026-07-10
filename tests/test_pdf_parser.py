import io
import json
import pytest
from unittest.mock import MagicMock, patch
from pypdf import PdfWriter
from src.pdf_parser import PDFParser


def get_minimal_pdf_bytes(encrypted=False, password=""):
    writer = PdfWriter()
    writer.add_blank_page(width=100, height=100)
    if encrypted:
        writer.encrypt(password)
    buf = io.BytesIO()
    writer.write(buf)
    return buf.getvalue()


def make_llm_response(transactions: list) -> MagicMock:
    mock = MagicMock()
    mock.choices[0].message.content = json.dumps({"transactions": transactions})
    return mock


@pytest.fixture(autouse=True)
def mock_configure_api_keys(monkeypatch):
    from src import pdf_parser

    monkeypatch.setattr(pdf_parser, "configure_api_keys", lambda: None)


def test_pdf_parser_no_api_key(monkeypatch):
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    parser = PDFParser()
    assert parser.parse_pdf(b"dummy pdf bytes") == []


@patch("litellm.completion")
def test_pdf_parser_success(mock_completion, monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "fake_key")
    mock_completion.return_value = make_llm_response(
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
    mock_completion.assert_called_once()


@patch("litellm.completion")
def test_pdf_parser_encrypted_success(mock_completion, monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "fake_key")
    mock_completion.return_value = make_llm_response(
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


@patch("litellm.completion")
def test_pdf_parser_encrypted_wrong_password(mock_completion, monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "fake_key")
    txns = PDFParser().parse_pdf(
        get_minimal_pdf_bytes(encrypted=True, password="correctpassword"),
        password="wrongpassword",
    )
    assert txns == []
    mock_completion.assert_not_called()


@patch("litellm.completion")
def test_pdf_parser_encrypted_no_password(mock_completion, monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "fake_key")
    txns = PDFParser().parse_pdf(
        get_minimal_pdf_bytes(encrypted=True, password="somepassword"),
        password="",
    )
    assert txns == []
    mock_completion.assert_not_called()


@patch("litellm.completion")
def test_pdf_parser_primary_fails_fallback_succeeds(mock_completion, monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "fake_key")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "fake_anthropic_key")

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
    mock_completion.side_effect = [Exception("Primary unavailable"), fallback_response]

    txns = PDFParser().parse_pdf(get_minimal_pdf_bytes())

    assert len(txns) == 1
    assert txns[0]["description"] == "fallback txn"
    assert mock_completion.call_count == 2


@patch("src.pdf_parser.PdfReader")
@patch("litellm.completion")
def test_pdf_parser_both_models_fail_text_extraction_succeeds(
    mock_completion, mock_reader_class, monkeypatch
):
    monkeypatch.setenv("GEMINI_API_KEY", "fake_key")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "fake_anthropic_key")

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
    mock_completion.side_effect = [
        Exception("Primary PDF failed"),
        Exception("Fallback PDF failed"),
        text_response,
    ]

    txns = PDFParser().parse_pdf(b"pdf_bytes")

    assert len(txns) == 1
    assert txns[0]["description"] == "text txn"
    assert mock_completion.call_count == 3


@patch("src.pdf_parser.PdfReader")
@patch("litellm.completion")
def test_pdf_parser_all_fail_empty_text(
    mock_completion, mock_reader_class, monkeypatch
):
    monkeypatch.setenv("GEMINI_API_KEY", "fake_key")

    mock_reader = MagicMock()
    mock_reader_class.return_value = mock_reader
    mock_reader.is_encrypted = False
    mock_page = MagicMock()
    mock_page.extract_text.return_value = ""
    mock_reader.pages = [mock_page]

    mock_completion.side_effect = [
        Exception("Primary failed"),
        Exception("Fallback failed"),
    ]

    txns = PDFParser().parse_pdf(b"pdf_bytes")
    assert txns == []


@patch("litellm.completion")
def test_pdf_parser_json_decode_error(mock_completion, monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "fake_key")
    bad_response = MagicMock()
    bad_response.choices[0].message.content = "not valid json"
    mock_completion.return_value = bad_response

    txns = PDFParser().parse_pdf(get_minimal_pdf_bytes())
    assert txns == []


@patch("src.pdf_parser.PdfReader")
def test_pdf_parser_general_exception(mock_reader_class, monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "fake_key")
    mock_reader_class.side_effect = Exception("Unexpected corrupt PDF")

    txns = PDFParser().parse_pdf(b"dummy pdf bytes")
    assert txns == []


@patch("litellm.completion")
def test_pdf_parser_response_as_bare_array(mock_completion, monkeypatch):
    """_parse_response handles bare JSON arrays (not wrapped in object)."""
    monkeypatch.setenv("GEMINI_API_KEY", "fake_key")
    bare_response = MagicMock()
    bare_response.choices[0].message.content = json.dumps(
        [
            {
                "date": "2026-05-01",
                "description": "bare array txn",
                "debit": "10",
                "credit": "",
            }
        ]
    )
    mock_completion.return_value = bare_response

    txns = PDFParser().parse_pdf(get_minimal_pdf_bytes())
    assert len(txns) == 1
    assert txns[0]["description"] == "bare array txn"


@patch("litellm.completion")
def test_parse_pdf_with_text_returns_oracle_text(mock_completion, monkeypatch):
    """parse_pdf_with_text returns (txns, extracted_text, summary); text is the
    validation oracle and must come back even on a successful vision parse."""
    monkeypatch.setenv("GEMINI_API_KEY", "fake_key")
    mock_completion.return_value = make_llm_response(
        [{"date": "06/06/2026", "description": "shop", "debit": "80", "credit": ""}]
    )

    txns, text, summary = PDFParser().parse_pdf_with_text(get_minimal_pdf_bytes())

    assert len(txns) == 1
    assert txns[0]["date"] == "06/06/2026"
    assert isinstance(text, str)
    assert isinstance(summary, dict)


@patch("litellm.completion")
def test_parse_pdf_with_text_extracts_summary(mock_completion, monkeypatch):
    """The statement's printed totals are surfaced as the summary cross-check."""
    monkeypatch.setenv("GEMINI_API_KEY", "fake_key")
    resp = MagicMock()
    resp.choices[0].message.content = json.dumps(
        {
            "transactions": [
                {"date": "06/06/2026", "description": "shop", "debit": "80"}
            ],
            "summary": {"total_debit": "80", "total_credit": "0"},
        }
    )
    mock_completion.return_value = resp

    txns, _text, summary = PDFParser().parse_pdf_with_text(get_minimal_pdf_bytes())

    assert len(txns) == 1
    assert summary == {"total_debit": "80", "total_credit": "0"}


@patch("litellm.completion")
def test_parse_pdf_with_text_model_override(mock_completion, monkeypatch):
    """Passing models=[...] drives the retry path (fallback model first)."""
    monkeypatch.setenv("GEMINI_API_KEY", "fake_key")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "fake_key")
    mock_completion.return_value = make_llm_response(
        [{"date": "06/06/2026", "description": "shop", "debit": "80", "credit": ""}]
    )

    parser = PDFParser()
    parser.parse_pdf_with_text(get_minimal_pdf_bytes(), models=[parser.fallback_model])

    called_model = mock_completion.call_args.kwargs["model"]
    assert called_model == parser.fallback_model
