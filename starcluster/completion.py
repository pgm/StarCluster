# Copyright 2009-2014 Justin Riley
#
# This file is part of StarCluster.
#
# StarCluster is free software: you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License as published by the Free
# Software Foundation, either version 3 of the License, or (at your option) any
# later version.
#
# StarCluster is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with StarCluster. If not, see <http://www.gnu.org/licenses/>.

#  Bash Protocol Description
#  -------------------------
#
#  `COMP_CWORD'
#       An index into `${COMP_WORDS}' of the word containing the current
#       cursor position.  This variable is available only in shell
#       functions invoked by the programmable completion facilities (*note
#       Programmable Completion::).
#
#  `COMP_LINE'
#       The current command line.  This variable is available only in
#       shell functions and external commands invoked by the programmable
#       completion facilities (*note Programmable Completion::).
#
#  `COMP_POINT'
#       The index of the current cursor position relative to the beginning
#       of the current command.  If the current cursor position is at the
#       end of the current command, the value of this variable is equal to
#       `${#COMP_LINE}'.  This variable is available only in shell
#       functions and external commands invoked by the programmable
#       completion facilities (*note Programmable Completion::).
#
#  `COMP_WORDS'
#       An array variable consisting of the individual words in the
#       current command line.  This variable is available only in shell
#       functions invoked by the programmable completion facilities (*note
#       Programmable Completion::).
#
#  `COMPREPLY'
#       An array variable from which Bash reads the possible completions
#       generated by a shell function invoked by the programmable
#       completion facility (*note Programmable Completion::).


import os
import re
import sys
import copy
import types
import logging
import optparse

from pprint import pformat
from optparse import OptionParser

import optcomplete
from optcomplete import AllCompleter
from optcomplete import DirCompleter
from optcomplete import ListCompleter
from optcomplete import NoneCompleter
from optcomplete import RegexCompleter

from starcluster import static

debugfn = os.path.join(static.STARCLUSTER_LOG_DIR, 'completion-debug.log')


def autocomplete(parser,
                 arg_completer=None,  # means use default.
                 opt_completer=None,
                 subcmd_completer=None,
                 subcommands=None):

    """Automatically detect if we are requested completing and if so generate
    completion automatically from given parser.

    'parser' is the options parser to use.

    'arg_completer' is a callable object that gets invoked to produce a list of
    completions for arguments completion (oftentimes files).

    'opt_completer' is the default completer to the options that require a
    value. 'subcmd_completer' is the default completer for the subcommand
    arguments.

    If 'subcommands' is specified, the script expects it to be a map of
    command-name to an object of any kind.  We are assuming that this object is
    a map from command name to a pair of (options parser, completer) for the
    command. If the value is not such a tuple, the method
    'autocomplete(completer)' is invoked on the resulting object.

    This will attempt to match the first non-option argument into a subcommand
    name and if so will use the local parser in the corresponding map entry's
    value.  This is used to implement completion for subcommand syntax and will
    not be needed in most cases."""

    # If we are not requested for complete, simply return silently, let the
    # code caller complete. This is the normal path of execution.
    if 'OPTPARSE_AUTO_COMPLETE' not in os.environ:
        return

    # Set default completers.
    if arg_completer is None:
        arg_completer = NoneCompleter()
    if opt_completer is None:
        opt_completer = NoneCompleter()
    if subcmd_completer is None:
        # subcmd_completer = arg_completer
        subcmd_completer = NoneCompleter()

    # By default, completion will be arguments completion, unless we find out
    # later we're trying to complete for an option.
    completer = arg_completer

    #
    # Completing...
    #

    # Fetching inputs... not sure if we're going to use these.

    # zsh's bashcompinit does not pass COMP_WORDS, replace with
    # COMP_LINE for now...
    if 'COMP_WORDS' not in os.environ:
        os.environ['COMP_WORDS'] = os.environ['COMP_LINE']

    cwords = os.environ['COMP_WORDS'].split()
    cline = os.environ['COMP_LINE']
    cpoint = int(os.environ['COMP_POINT'])
    cword = int(os.environ['COMP_CWORD'])

    # If requested, try subcommand syntax to find an options parser for that
    # subcommand.
    if subcommands:
        assert isinstance(subcommands, types.DictType)
        value = guess_first_nonoption(parser, subcommands)
        if value:
            if isinstance(value, (types.ListType, types.TupleType)):
                parser = value[0]
                if len(value) > 1 and value[1]:
                    # override completer for command if it is present.
                    completer = value[1]
                else:
                    completer = subcmd_completer
                return autocomplete(parser, completer)
            else:
                # Call completion method on object. This should call
                # autocomplete() recursively with appropriate arguments.
                if hasattr(value, 'autocomplete'):
                    return value.autocomplete(subcmd_completer)
                else:
                    sys.exit(1)  # no completions for that command object

    # Extract word enclosed word.
    prefix, suffix = optcomplete.extract_word(cline, cpoint)
    # The following would be less exact, but will work nonetheless .
    # prefix, suffix = cwords[cword], None

    # Look at previous word, if it is an option and it requires an argument,
    # check for a local completer.  If there is no completer, what follows
    # directly cannot be another option, so mark to not add those to
    # completions.
    optarg = False
    try:
        # Look for previous word, which will be containing word if the option
        # has an equals sign in it.
        prev = None
        if cword < len(cwords):
            mo = re.search('(--.*)=(.*)', cwords[cword])
            if mo:
                prev, prefix = mo.groups()
        if not prev:
            prev = cwords[cword - 1]

        if prev and prev.startswith('-'):
            option = parser.get_option(prev)
            if option:
                if option.nargs > 0:
                    optarg = True
                    if hasattr(option, 'completer'):
                        completer = option.completer
                    elif option.type != 'string':
                        completer = NoneCompleter()
                    else:
                        completer = opt_completer
                # Warn user at least, it could help him figure out the problem.
                elif hasattr(option, 'completer'):
                    raise SystemExit(
                        "Error: optparse option with a completer "
                        "does not take arguments: %s" % str(option))
    except KeyError:
        pass

    completions = []

    # Options completion.
    if not optarg and (not prefix or prefix.startswith('-')):
        completions += parser._short_opt.keys()
        completions += parser._long_opt.keys()
        # Note: this will get filtered properly below.

    # File completion.
    if completer and (not prefix or not prefix.startswith('-')):

        # Call appropriate completer depending on type.
        if isinstance(completer, (types.StringType, types.ListType,
                                  types.TupleType)):
            completer = RegexCompleter(completer)
            completions += completer(os.getcwd(), cline,
                                     cpoint, prefix, suffix)
        elif isinstance(completer, (types.FunctionType, types.LambdaType,
                                    types.ClassType, types.ObjectType)):
            completions += completer(os.getcwd(), cline,
                                     cpoint, prefix, suffix)

    # Filter using prefix.
    if prefix:
        completions = filter(lambda x: x.startswith(prefix), completions)

    # Print result.
    print ' '.join(completions)

    # Print debug output (if needed).  You can keep a shell with 'tail -f' to
    # the log file to monitor what is happening.
    if debugfn:
        f = open(debugfn, 'a')
        print >> f, '---------------------------------------------------------'
        print >> f, 'CWORDS', cwords
        print >> f, 'CLINE', cline
        print >> f, 'CPOINT', cpoint
        print >> f, 'CWORD', cword
        print >> f, '\nShort options'
        print >> f, pformat(parser._short_opt)
        print >> f, '\nLong options'
        print >> f, pformat(parser._long_opt)
        print >> f, 'Prefix/Suffix:', prefix, suffix
        print >> f, 'completions', completions
        f.close()

    # Exit with error code (we do not let the caller continue on purpose, this
    # is a run for completions only.)
    sys.exit(1)


def error_override(self, msg):
    """Hack to keep OptionParser from writing to sys.stderr when
    calling self.exit from self.error"""
    self.exit(2, msg=None)


def guess_first_nonoption(gparser, subcmds_map):

    """Given a global options parser, try to guess the first non-option without
    generating an exception. This is used for scripts that implement a
    subcommand syntax, so that we can generate the appropriate completions for
    the subcommand."""

    gparser = copy.deepcopy(gparser)

    def print_usage_nousage(self, file=None):
        pass
    gparser.print_usage = print_usage_nousage

    # save state to restore
    prev_interspersed = gparser.allow_interspersed_args
    gparser.disable_interspersed_args()

    cwords = os.environ['COMP_WORDS'].split()

    # save original error_func so we can put it back after the hack
    error_func = gparser.error
    try:
        try:
            instancemethod = type(OptionParser.error)
            # hack to keep OptionParser from wrinting to sys.stderr
            gparser.error = instancemethod(error_override,
                                           gparser, OptionParser)
            gopts, args = gparser.parse_args(cwords[1:])
        except SystemExit:
            return None
    finally:
        # undo the hack and restore original OptionParser error function
        gparser.error = instancemethod(error_func, gparser, OptionParser)

    value = None
    if args:
        subcmdname = args[0]
        try:
            value = subcmds_map[subcmdname]
        except KeyError:
            pass

    gparser.allow_interspersed_args = prev_interspersed  # restore state

    return value  # can be None, indicates no command chosen.


class CmdComplete(optcomplete.CmdComplete):
    """Simple default base class implementation for a subcommand that supports
    command completion.  This class is assuming that there might be a method
    addopts(self, parser) to declare options for this subcommand, and an
    optional completer data member to contain command-specific completion.  Of
    course, you don't really have to use this, but if you do it is convenient
    to have it here."""

    def autocomplete(self, completer):
        logging.disable(logging.CRITICAL)
        parser = optparse.OptionParser(self.__doc__.strip())
        if hasattr(self, 'addopts'):
            self.addopts(parser)
        if hasattr(self, 'completer'):
            completer = self.completer
        logging.disable(logging.NOTSET)
        return autocomplete(parser, completer)

NoneCompleter = NoneCompleter
ListCompleter = ListCompleter
AllCompleter = AllCompleter
DirCompleter = DirCompleter
RegexCompleter = RegexCompleter


if __name__ == '__main__':
    optcomplete.test()
