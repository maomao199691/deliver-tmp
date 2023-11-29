import difflib

def contrastText(textA, textB):
    matcher = difflib.SequenceMatcher(None, textA, textB)

    similarity_ratio = matcher.ratio()
    return similarity_ratio