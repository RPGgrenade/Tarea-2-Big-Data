from __future__ import print_function
import sys

if len(sys.argv) != 3:
    print("Usage: first_n_lines.py <input_file> <N>")
    sys.exit(1)

input_file = sys.argv[1]
n = int(sys.argv[2])

output_file = "%s.%d" % (input_file, n)

with open(input_file, encoding='utf-8') as fIn:
    with open(output_file, "w", encoding='utf-8') as fOut:
        count = 0
        for line in fIn:
            fOut.write(line)
            count += 1
            if count >= n:
                break

print("Done")

