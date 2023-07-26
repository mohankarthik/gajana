def sanitize_currency(input: str) -> float:
  return float(input.strip().replace(",","").replace("₹",""))
