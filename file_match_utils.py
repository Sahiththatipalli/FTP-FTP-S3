import fnmatch

def match_files(files, include_patterns=None):
    """
    Returns files matching any of the glob-style include patterns.
    """
    if not include_patterns:
        return files
    matched = []
    for pattern in include_patterns:
        matched.extend(fnmatch.filter(files, pattern))
    # Remove duplicates
    return list(set(matched))
