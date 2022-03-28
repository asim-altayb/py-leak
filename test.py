from collections import Counter

with open('source.txt') as infile:
    counts = Counter()
    for x in infile:
        counts += Counter(x.strip())

for line, count in counts.most_common():
    with open('result.txt', 'a') as the_file:
        res = str(line)+":"+str(count)+"\n"
        the_file.write(res)