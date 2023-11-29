import re

pattern = re.compile("@\[([a-zA-Z0-9]+)\]@\\\n")
pattern01 = re.compile("@\[([a-zA-Z0-9]+)\]@")

def delete_image(value):
    if value is not None and len(value) > 0:
        if "]\n" in value:
            value = re.sub(pattern, "", value)
        if "]" in value:
            value = re.sub(pattern01, "", value)
    return value