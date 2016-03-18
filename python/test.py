
aa = [(a, b, c) for a in range(1, 11) for b in range(a+1, 12) for c in range(a, 11) if a**2 + b**2 == c**2]

print(aa)

