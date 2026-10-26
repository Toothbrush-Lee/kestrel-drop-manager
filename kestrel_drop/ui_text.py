"""Terminal-cell text layout shared by the TUI; no curses or I/O side effects."""
import unicodedata


def cell_width(char):
    if unicodedata.combining(char):
        return 0
    return 2 if unicodedata.east_asian_width(char) in 'WF' else 1


def text_width(text):
    return sum(cell_width(char) for char in str(text))


def clip(text, width):
    result, used = [], 0
    for char in str(text):
        if unicodedata.category(char).startswith('C'):
            char = ' '
        size = cell_width(char)
        if used + size > width:
            break
        result.append(char)
        used += size
    return ''.join(result)


def wrap_lines(text, width):
    """Hard-wrap Chinese, paths and logs without dropping text or splitting cells."""
    width = max(1, width)
    result = []
    for source in str(text).expandtabs(4).split('\n'):
        line, used = '', 0
        for char in source:
            if unicodedata.category(char).startswith('C'):
                char = ' '
            size = cell_width(char)
            if size > width:
                char, size = '?', 1
            if used + size > width:
                result.append(line)
                line, used = '', 0
            line += char
            used += size
        result.append(line)
    return result


def input_window(value, cursor, width):
    """Return a visible editable window and its terminal-cell cursor position."""
    width = max(2, width)
    cursor = max(0, min(cursor, len(value)))
    start, used = cursor, 0
    while start > 0:
        size = cell_width(value[start - 1])
        if used + size > width - 2:
            break
        start -= 1
        used += size
    prefix = '‹' if start else ''
    visible = prefix + clip(value[start:], width - len(prefix))
    column = len(prefix) + text_width(value[start:cursor])
    return visible, min(column, width - 1)
