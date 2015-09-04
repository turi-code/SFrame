import sys
from code import InteractiveInterpreter


def main():
    """
    Print lines of input along with output.
    """
    source_lines = (line.rstrip() for line in sys.stdin)
    console = InteractiveInterpreter()
    console.runsource('import graphlab')
    source = ''
    try:
        while True:
            source = source_lines.next()
            more = console.runsource(source)
            while more:
                next_line = source_lines.next()
                print '...', next_line
                source += '\n' + next_line
                more = console.runsource(source)
    except StopIteration:
        if more:
            print '... '
            more = console.runsource(source + '\n')



if __name__ == '__main__':
    main()


# vim: set expandtab shiftwidth=4 softtabstop=4 :
