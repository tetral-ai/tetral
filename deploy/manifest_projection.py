"""Shared identity parser for locally rendered repository-owned YAML."""
import re
import json

def documents(text):
    for block in re.split(r"(?m)^---\s*$", text):
        block = re.sub(r"(?m)^# Source:.*\n", "", block).strip()
        if block:
            yield block

def scalar(text):
    text = text.strip()
    if text.startswith('"'):
        value = json.loads(text)
        if not isinstance(value, str):
            raise ValueError("rendered identity must be a string")
        return value
    if text.startswith("'") and text.endswith("'"):
        return text[1:-1].replace("''", "'")
    return text

def identity(block):
    api = re.search(r"(?m)^apiVersion: (.+)$", block)
    kind = re.search(r"(?m)^kind: (.+)$", block)
    metadata = re.search(r"(?ms)^metadata:\n(.*?)(?=^\S|\Z)", block)
    if not api or not kind or not metadata:
        raise ValueError("rendered document lacks an object identity")
    name = re.search(r"(?m)^  name: (.+)$", metadata[1])
    namespace = re.search(r"(?m)^  namespace: (.+)$", metadata[1])
    if not name:
        raise ValueError("rendered object lacks a name")
    return (scalar(api[1]), scalar(kind[1]), scalar(namespace[1]) if namespace else "", scalar(name[1]))
