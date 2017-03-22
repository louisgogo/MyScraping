import re

a = '08001 东方汇财证券'

print(re.search(re.compile("([0-9]+)(.+)"), a).group(1))
print(re.search(re.compile("([0-9]+)(.+)"), a).group(2).strip())
