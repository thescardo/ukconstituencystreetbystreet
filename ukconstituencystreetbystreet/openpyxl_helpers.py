from copy import copy

from openpyxl.styles.borders import Border, Side

def set_border(ws, cell_range, style="thin"):
    rows = ws[cell_range]
    for row in rows:
        temp_row = copy(row[0].border)
        row[0].border = Border(left=Side(style=style), right=temp_row.right, top=temp_row.top, bottom=temp_row.bottom)

        temp_row = copy(row[-1].border)
        row[-1].border = Border(right=Side(style=style), left=temp_row.left, top=temp_row.top, bottom=temp_row.bottom)
    for c in rows[0]:
        temp_row = copy(c.border)
        c.border = Border(top=Side(style=style), left=temp_row.left, bottom=temp_row.bottom, right=temp_row.right)
    for c in rows[-1]:
        temp_row = copy(c.border)
        c.border = Border(bottom=Side(style=style), left=temp_row.left, top=temp_row.top, right=temp_row.right)