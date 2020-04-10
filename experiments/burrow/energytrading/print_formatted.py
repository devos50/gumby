with open("agent6.txt") as in_file:
    content = in_file.read().strip()
    parts = content.split(" ")
    parts = [str(int(float(part) * 10E5)) for part in parts]
    out = ", ".join(parts)
    print("[%s]" % out)
