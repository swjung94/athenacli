import os
import re
import locale
import logging
import subprocess
import shlex
from io import open
from time import sleep

import click
import sqlparse

from athenacli.packages.prompt_utils import confirm_destructive_query
from . import export
from .main import special_command, NO_QUERY, PARSED_QUERY
from .utils import handle_cd_command


TIMING_ENABLED = False
use_expanded_output = False
PAGER_ENABLED = True
tee_file = None
once_file = written_to_once_file = None


@export
def set_timing_enabled(val):
    global TIMING_ENABLED
    TIMING_ENABLED = val

@export
def set_pager_enabled(val):
    global PAGER_ENABLED
    PAGER_ENABLED = val

@export
def is_pager_enabled():
    return PAGER_ENABLED

@export
@special_command('pager', '\\P [command]',
                 'Set PAGER. Print the query results via PAGER.',
                 arg_type=PARSED_QUERY, aliases=('\\P', ), case_sensitive=True)
def set_pager(arg, **_):
    if arg:
        os.environ['PAGER'] = arg
        msg = 'PAGER set to %s.' % arg
        set_pager_enabled(True)
    else:
        if 'PAGER' in os.environ:
            msg = 'PAGER set to %s.' % os.environ['PAGER']
        else:
            # This uses click's default per echo_via_pager.
            msg = 'Pager enabled.'
        set_pager_enabled(True)

    return [(None, None, None, msg)]

@export
@special_command('nopager', '\\n', 'Disable pager, print to stdout.',
                 arg_type=NO_QUERY, aliases=('\\n', ), case_sensitive=True)
def disable_pager():
    set_pager_enabled(False)
    return [(None, None, None, 'Pager disabled.')]

@special_command('\\timing', '\\t', 'Toggle timing of commands.', arg_type=NO_QUERY, aliases=('\\t', ), case_sensitive=True)
def toggle_timing():
    global TIMING_ENABLED
    TIMING_ENABLED = not TIMING_ENABLED
    message = "Timing is "
    message += "on." if TIMING_ENABLED else "off."
    return [(None, None, None, message)]

@export
def is_timing_enabled():
    return TIMING_ENABLED

@export
def set_expanded_output(val):
    global use_expanded_output
    use_expanded_output = val

@export
def is_expanded_output():
    return use_expanded_output

_logger = logging.getLogger(__name__)

@export
def editor_command(command):
    """
    Is this an external editor command?
    :param command: string
    """
    # It is possible to have `\e filename` or `SELECT * FROM \e`. So we check
    # for both conditions.
    return command.strip().endswith('\\e') or command.strip().startswith('\\e')

@export
def get_filename(sql):
    if sql.strip().startswith('\\e'):
        command, _, filename = sql.partition(' ')
        return filename.strip() or None


@export
def get_editor_query(sql):
    """Get the query part of an editor command."""
    sql = sql.strip()

    # The reason we can't simply do .strip('\e') is that it strips characters,
    # not a substring. So it'll strip "e" in the end of the sql also!
    # Ex: "select * from style\e" -> "select * from styl".
    pattern = re.compile('(^\\\e|\\\e$)')
    while pattern.search(sql):
        sql = pattern.sub('', sql)

    return sql


@export
def open_external_editor(filename=None, sql=None):
    """Open external editor, wait for the user to type in their query, return
    the query.
    :return: list with one tuple, query as first element.
    """

    message = None
    filename = filename.strip().split(' ', 1)[0] if filename else None

    sql = sql or ''
    MARKER = '# Type your query above this line.\n'

    # Populate the editor buffer with the partial sql (if available) and a
    # placeholder comment.
    query = click.edit(u'{sql}\n\n{marker}'.format(sql=sql, marker=MARKER),
                       filename=filename, extension='.sql')

    if filename:
        try:
            with open(filename, encoding='utf-8') as f:
                query = f.read()
        except IOError:
            message = 'Error reading file: %s.' % filename

    if query is not None:
        query = query.split(MARKER, 1)[0].rstrip('\n')
    else:
        # Don't return None for the caller to deal with.
        # Empty string is ok.
        query = sql

    return (query, message)


@special_command('system', 'system [command]',
                 'Execute a system shell commmand.')
def execute_system_command(arg, **_):
    """Execute a system shell command."""
    usage = "Syntax: system [command].\n"

    if not arg:
      return [(None, None, None, usage)]

    try:
        command = arg.strip()
        if command.startswith('cd'):
            ok, error_message = handle_cd_command(arg)
            if not ok:
                return [(None, None, None, error_message)]
            return [(None, None, None, '')]

        args = arg.split(' ')
        process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        output, error = process.communicate()
        response = output if not error else error

        # Python 3 returns bytes. This needs to be decoded to a string.
        if isinstance(response, bytes):
            encoding = locale.getpreferredencoding(False)
            response = response.decode(encoding)

        return [(None, None, None, response)]
    except OSError as e:
        return [(None, None, None, 'OSError: %s' % e.strerror)]


def parseargfile(arg):
    if arg.startswith('-o '):
        mode = "w"
        filename = arg[3:]
    else:
        mode = 'a'
        filename = arg

    if not filename:
        raise TypeError('You must provide a filename.')

    return {'file': os.path.expanduser(filename), 'mode': mode}


@special_command('tee', 'tee [-o] filename',
                 'Append all results to an output file (overwrite using -o).')
def set_tee(arg, **_):
    global tee_file

    try:
        tee_file = open(**parseargfile(arg))
    except (IOError, OSError) as e:
        raise OSError("Cannot write to file '{}': {}".format(e.filename, e.strerror))

    return [(None, None, None, "")]

@export
def close_tee():
    global tee_file
    if tee_file:
        tee_file.close()
        tee_file = None


@special_command('notee', 'notee', 'Stop writing results to an output file.')
def no_tee(arg, **_):
    close_tee()
    return [(None, None, None, "")]

@export
def write_tee(output):
    global tee_file
    if tee_file:
        click.echo(output, file=tee_file, nl=False)
        click.echo(u'\n', file=tee_file, nl=False)
        tee_file.flush()


@special_command('\\once', '\\o [-o] filename',
                 'Append next result to an output file (overwrite using -o).',
                 aliases=('\\o', ))
def set_once(arg, **_):
    global once_file

    once_file = parseargfile(arg)

    return [(None, None, None, "")]


@export
def write_once(output):
    global once_file, written_to_once_file
    if output and once_file:
        try:
            f = open(**once_file)
        except (IOError, OSError) as e:
            once_file = None
            raise OSError("Cannot write to file '{}': {}".format(
                e.filename, e.strerror))

        with f:
            click.echo(output, file=f, nl=False)
            click.echo(u"\n", file=f, nl=False)
        written_to_once_file = True


@export
def unset_once_if_written():
    """Unset the once file, if it has been written to."""
    global once_file
    if written_to_once_file:
        once_file = None


@special_command(
    'watch',
    'watch [seconds] [-c] query',
    'Executes the query every [seconds] seconds (by default 5).'
)
def watch_query(arg, **kwargs):
    usage = """Syntax: watch [seconds] [-c] query.
    * seconds: The interval at the query will be repeated, in seconds.
               By default 5.
    * -c: Clears the screen between every iteration.
"""
    if not arg:
        yield (None, None, None, usage)
        return
    seconds = 5
    clear_screen = False
    statement = None
    while statement is None:
        arg = arg.strip()
        if not arg:
            # Oops, we parsed all the arguments without finding a statement
            yield (None, None, None, usage)
            return
        (current_arg, _, arg) = arg.partition(' ')
        try:
            seconds = float(current_arg)
            continue
        except ValueError:
            pass
        if current_arg == '-c':
            clear_screen = True
            continue
        statement = '{0!s} {1!s}'.format(current_arg, arg)
    destructive_prompt = confirm_destructive_query(statement)
    if destructive_prompt is False:
        click.secho("Wise choice!")
        return
    elif destructive_prompt is True:
        click.secho("Your call!")
    cur = kwargs['cur']
    sql_list = [
        (sql.rstrip(';'), "> {0!s}".format(sql))
        for sql in sqlparse.split(statement)
    ]
    old_pager_enabled = is_pager_enabled()
    while True:
        if clear_screen:
            click.clear()
        try:
            # Somewhere in the code the pager its activated after every yield,
            # so we disable it in every iteration
            set_pager_enabled(False)
            for (sql, title) in sql_list:
                cur.execute(sql)
                if cur.description:
                    headers = [x[0] for x in cur.description]
                    yield (title, cur, headers, None)
                else:
                    yield (title, None, None, None)
            sleep(seconds)
        except KeyboardInterrupt:
            # This prints the Ctrl-C character in its own line, which prevents
            # to print a line with the cursor positioned behind the prompt
            click.secho("", nl=True)
            return
        finally:
            set_pager_enabled(old_pager_enabled)