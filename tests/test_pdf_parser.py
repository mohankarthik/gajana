import io
import os
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


@pytest.fixture(autouse=True)
def mock_load_gemini_api_key(monkeypatch):
    from src import pdf_parser

    monkeypatch.setattr(
        pdf_parser,
        "load_gemini_api_key",
        lambda: os.environ.get("GEMINI_API_KEY", ""),
    )


def test_pdf_parser_no_api_key(monkeypatch):
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    parser = PDFParser()
    assert parser.model is None
    res = parser.parse_pdf(b"dummy pdf bytes")
    assert res == []


@patch("google.generativeai.GenerativeModel")
@patch("google.generativeai.configure")
def test_pdf_parser_success(mock_configure, mock_model_class, monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "fake_key")
    mock_model = MagicMock()
    mock_model_class.return_value = mock_model

    mock_response = MagicMock()
    mock_response.text = json.dumps(
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
    mock_model.generate_content.return_value = mock_response

    parser = PDFParser()
    pdf_bytes = get_minimal_pdf_bytes()
    txns = parser.parse_pdf(pdf_bytes)

    assert len(txns) == 2
    assert txns[0]["description"] == "uber ride"
    assert txns[0]["debit"] == "200.00"
    assert txns[1]["credit"] == "50000.00"

    mock_configure.assert_called_once_with(api_key="fake_key")


@patch("google.generativeai.GenerativeModel")
def test_pdf_parser_encrypted_success(mock_model_class, monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "fake_key")
    mock_model = MagicMock()
    mock_model_class.return_value = mock_model

    mock_response = MagicMock()
    mock_response.text = json.dumps(
        [
            {
                "date": "2026-05-02",
                "description": "uber ride",
                "debit": "200.00",
                "credit": "",
            }
        ]
    )
    mock_model.generate_content.return_value = mock_response

    parser = PDFParser()
    pdf_bytes = get_minimal_pdf_bytes(encrypted=True, password="correctpassword")
    txns = parser.parse_pdf(pdf_bytes, password="correctpassword")

    assert len(txns) == 1
    assert txns[0]["description"] == "uber ride"


@patch("google.generativeai.GenerativeModel")
def test_pdf_parser_encrypted_wrong_password(mock_model_class, monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "fake_key")
    parser = PDFParser()
    pdf_bytes = get_minimal_pdf_bytes(encrypted=True, password="correctpassword")
    txns = parser.parse_pdf(pdf_bytes, password="wrongpassword")
    assert txns == []


@patch("src.pdf_parser.PdfReader")
@patch("google.generativeai.GenerativeModel")
def test_pdf_parser_native_fails_fallback_success(
    mock_model_class, mock_reader_class, monkeypatch
):
    monkeypatch.setenv("GEMINI_API_KEY", "fake_key")
    mock_model = MagicMock()
    mock_model_class.return_value = mock_model

    # Mock reader and extracted text
    mock_reader = MagicMock()
    mock_reader_class.return_value = mock_reader
    mock_reader.is_encrypted = False
    mock_page = MagicMock()
    mock_page.extract_text.return_value = "mocked extracted transaction text"
    mock_reader.pages = [mock_page]

    # Native PDF call raises an error
    mock_response = MagicMock()
    mock_response.text = json.dumps(
        [
            {
                "date": "2026-05-02",
                "description": "fallback ride",
                "debit": "20.00",
                "credit": "",
            }
        ]
    )
    mock_model.generate_content.side_effect = [Exception("API limits"), mock_response]

    parser = PDFParser()
    pdf_bytes = get_minimal_pdf_bytes()
    txns = parser.parse_pdf(pdf_bytes)

    assert len(txns) == 1
    assert txns[0]["description"] == "fallback ride"
    assert mock_model.generate_content.call_count == 2


def test_pdf_parser_encrypted_no_password(monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "fake_key")
    parser = PDFParser()
    pdf_bytes = get_minimal_pdf_bytes(encrypted=True, password="somepassword")
    txns = parser.parse_pdf(pdf_bytes, password="")
    assert txns == []


@patch("src.pdf_parser.PdfReader")
@patch("google.generativeai.GenerativeModel")
def test_pdf_parser_fallback_empty_text(
    mock_model_class, mock_reader_class, monkeypatch
):
    monkeypatch.setenv("GEMINI_API_KEY", "fake_key")
    mock_model = MagicMock()
    mock_model_class.return_value = mock_model
    mock_model.generate_content.side_effect = Exception("API limits")

    mock_reader = MagicMock()
    mock_reader_class.return_value = mock_reader
    mock_reader.is_encrypted = False
    mock_page = MagicMock()
    mock_page.extract_text.return_value = ""  # Empty text
    mock_reader.pages = [mock_page]

    parser = PDFParser()
    txns = parser.parse_pdf(b"pdf_bytes")
    assert txns == []


@patch("google.generativeai.GenerativeModel")
def test_pdf_parser_json_decode_error(mock_model_class, monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "fake_key")
    mock_model = MagicMock()
    mock_model_class.return_value = mock_model

    mock_response = MagicMock()
    mock_response.text = "invalid json response"
    mock_model.generate_content.return_value = mock_response

    parser = PDFParser()
    txns = parser.parse_pdf(get_minimal_pdf_bytes())
    assert txns == []


@patch("src.pdf_parser.PdfReader")
def test_pdf_parser_general_exception(mock_reader_class, monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "fake_key")
    mock_reader_class.side_effect = Exception("Unexpected PDF corrupt error")
    parser = PDFParser()
    txns = parser.parse_pdf(b"dummy pdf bytes")
    assert txns == []
