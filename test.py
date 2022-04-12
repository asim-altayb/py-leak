str = " asim@gm.com    12 34"
delims = [':',';',' ']
for d in delims:
    result = str.strip().split(d, maxsplit=1)
    if len(result) == 2:
        result[1] = result[1].strip()
        print(result)