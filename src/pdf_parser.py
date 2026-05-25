import os
import json
import logging
import io
from pypdf import PdfReader, PdfWriter
import google.generativeai as genai

logger = logging.getLogger(__name__)


def load_gemini_api_key() -> str:
    # 1. Try secrets/gemini.json
    json_path = os.path.join(os.getcwd(), "secrets", "gemini.json")
    if os.path.exists(json_path):
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
            # If missing curly braces, wrap it
            if not content.startswith("{"):
                content = "{" + content + "}"
            data = json.loads(content)
            if "api_key" in data:
                return str(data["api_key"]).strip()
        except Exception:
            # Fallback regex extraction if JSON parsing is still problematic
            import re

            match = re.search(r'"api_key"\s*:\s*"([^"]+)"', content)
            if match:
                return match.group(1).strip()

    # 2. Try environment variable
    return os.environ.get("GEMINI_API_KEY", "")


class PDFParser:
    def __init__(self, model_name: str = "gemini-3.5-flash"):
        api_key = load_gemini_api_key()
        if api_key:
            genai.configure(api_key=api_key)
            self.model = genai.GenerativeModel(model_name)
            logger.info(f"PDFParser initialized with model: {model_name}")
        else:
            logger.warning(
                "GEMINI_API_KEY environment variable or secrets/gemini.json not set. PDF parsing will fail."
            )
            self.model = None

    def parse_pdf(self, pdf_bytes: bytes, password: str = "") -> list[dict]:
        if not self.model:
            logger.error("GEMINI_API_KEY not configured. Cannot parse PDF.")
            return []

        try:
            reader = PdfReader(io.BytesIO(pdf_bytes))
            if reader.is_encrypted:
                if password:
                    res = reader.decrypt(password)
                    if res == 0:
                        logger.error("Failed to decrypt PDF. Incorrect password.")
                        return []
                else:
                    logger.error("PDF is encrypted but no password provided.")
                    return []

            # If it was encrypted, write the decrypted PDF to bytes
            if reader.is_encrypted:
                writer = PdfWriter()
                for page in reader.pages:
                    writer.add_page(page)
                out_buf = io.BytesIO()
                writer.write(out_buf)
                pdf_payload_bytes = out_buf.getvalue()
            else:
                pdf_payload_bytes = pdf_bytes

            prompt = """
You are a precise data extraction assistant.
Extract all financial transactions from the provided bank statement document.
Only extract actual transactions. Ignore opening/closing balances, rewards points, page headers, or other summary text.

For each transaction, extract:
- 'date': The transaction date in YYYY-MM-DD format.
- 'description': The exact transaction details, description, narration, or payee.
- 'debit': The outgoing amount (withdrawal / debit). Use empty string if no debit occurred.
  Do not include currency symbols or commas.
- 'credit': The incoming amount (deposit / credit). Use empty string if no credit occurred.
  Do not include currency symbols or commas.
"""

            # Define standard schema for structured JSON output
            generation_config = {
                "response_mime_type": "application/json",
                "response_schema": {
                    "type": "ARRAY",
                    "description": "List of extracted transactions",
                    "items": {
                        "type": "OBJECT",
                        "properties": {
                            "date": {
                                "type": "STRING",
                                "description": "Transaction date in YYYY-MM-DD format",
                            },
                            "description": {
                                "type": "STRING",
                                "description": "Transaction description/narration",
                            },
                            "debit": {
                                "type": "STRING",
                                "description": "Debit amount as string",
                            },
                            "credit": {
                                "type": "STRING",
                                "description": "Credit amount as string",
                            },
                        },
                        "required": ["date", "description", "debit", "credit"],
                    },
                },
            }

            response_text = ""
            # Try native PDF modality first
            try:
                logger.info(
                    "Attempting to parse PDF using Gemini native PDF modality..."
                )
                response = self.model.generate_content(
                    contents=[
                        {
                            "mime_type": "application/pdf",
                            "data": pdf_payload_bytes,
                        },
                        prompt,
                    ],
                    generation_config=generation_config,
                )
                response_text = response.text.strip()
                transactions = json.loads(response_text)
                logger.info(
                    f"Successfully parsed {len(transactions)} transactions via Gemini PDF modality."
                )
                return transactions

            except Exception as pdf_err:
                logger.warning(
                    f"Native PDF modality parsing failed or timed out: {pdf_err}. Falling back to text extraction..."
                )
                # Fallback: Extract text using pypdf
                text = ""
                for page in reader.pages:
                    extracted = page.extract_text()
                    if extracted:
                        text += extracted + "\n"

                if not text.strip():
                    logger.error(
                        "No text could be extracted from PDF using pypdf. Cannot parse."
                    )
                    return []

                logger.info("Attempting to parse extracted text using Gemini...")
                text_prompt = f"{prompt}\n\nStatement Text:\n{text}"
                response = self.model.generate_content(
                    contents=text_prompt, generation_config=generation_config
                )
                response_text = response.text.strip()
                transactions = json.loads(response_text)
                logger.info(
                    f"Successfully parsed {len(transactions)} transactions via extracted text."
                )
                return transactions

        except json.JSONDecodeError as e:
            logger.error(
                f"Failed to decode JSON from Gemini response: {e}\nResponse: {response_text}"
            )
            return []
        except Exception as e:
            logger.error(f"Failed to parse PDF statement: {e}")
            return []
