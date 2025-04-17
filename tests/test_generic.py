import re

def parse_currency(value):
    try:
        if value is None:
            return 0

        value = str(value).strip()

        # Case 1: comma as decimal separator (European-style)
        if ',' in value and not '.' in value:
            cleaned = re.sub(r'[^\d,]', '', value).replace(',', '.')
        # Case 2: both dot and comma present (European-style with thousands separator)
        elif '.' in value and ',' in value:
            cleaned = re.sub(r'[^\d,\.]', '', value).replace('.', '').replace(',', '.')
        # Case 3: US-style (dot is decimal separator)
        else:
            cleaned = re.sub(r'[^\d.-]', '', value)

        return float(cleaned) if cleaned else 0
    except (ValueError, TypeError):
        return 0

print(parse_currency('$690,76'))