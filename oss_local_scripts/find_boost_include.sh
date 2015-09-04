#/bin/bash
grep -hir "#include\ *<boost" $@ | sed 's/\(<\|>\)//g' | cut -d " " -f 2 | sort | uniq
