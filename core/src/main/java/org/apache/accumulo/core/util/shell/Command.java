package org.apache.accumulo.core.util.shell;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * The Command class represents a command to be run in the shell. It contains the methods to execute along with some methods to help tab completion, and
 * return the command name, help, and usage.
 */
public abstract class Command {
  // Helper methods for completion
  public enum CompletionSet {
    TABLENAMES, USERNAMES, COMMANDS
  }

  static Set<String> getCommandNames(Map<CompletionSet,Set<String>> objects) {
    return objects.get(CompletionSet.COMMANDS);
  }

  static Set<String> getTableNames(Map<CompletionSet,Set<String>> objects) {
    return objects.get(CompletionSet.TABLENAMES);
  }

  static Set<String> getUserNames(Map<CompletionSet,Set<String>> objects) {
    return objects.get(CompletionSet.USERNAMES);
  }

  public void registerCompletionGeneral(Token root, Set<String> args, boolean caseSens) {
    Token t = new Token(args);
    t.setCaseSensitive(caseSens);

    Token command = new Token(getName());
    command.addSubcommand(t);

    root.addSubcommand(command);
  }

  public void registerCompletionForTables(Token root, Map<CompletionSet,Set<String>> completionSet) {
    registerCompletionGeneral(root, completionSet.get(CompletionSet.TABLENAMES), true);
  }

  public void registerCompletionForUsers(Token root, Map<CompletionSet,Set<String>> completionSet) {
    registerCompletionGeneral(root, completionSet.get(CompletionSet.USERNAMES), true);
  }

  public void registerCompletionForCommands(Token root, Map<CompletionSet,Set<String>> completionSet) {
    registerCompletionGeneral(root, completionSet.get(CompletionSet.COMMANDS), false);
  }

  // abstract methods to override
  public abstract int execute(String fullCommand, CommandLine cl, Shell shellState) throws Exception;

  public abstract String description();

  /**
   * If the number of arguments is not always zero (not including those arguments handled through Options), make sure to override the {@link #usage()} method.
   * Otherwise, {@link #usage()} does need to be overridden.
   */
  public abstract int numArgs();

  // OPTIONAL methods to override:

  // the general version of getname uses reflection to get the class name
  // and then cuts off the suffix -Command to get the name of the command
  public String getName() {
    String s = this.getClass().getName();
    int st = Math.max(s.lastIndexOf('$'), s.lastIndexOf('.'));
    int i = s.indexOf("Command");
    return i > 0 ? s.substring(st + 1, i).toLowerCase(Locale.ENGLISH) : null;
  }

  // The general version of this method adds the name
  // of the command to the completion tree
  public void registerCompletion(Token root, Map<CompletionSet,Set<String>> completion_set) {
    root.addSubcommand(new Token(getName()));
  }

  // The general version of this method uses the HelpFormatter
  // that comes with the apache Options package to print out the help
  public final void printHelp(Shell shellState) {
    shellState.printHelp(usage(), "description: " + this.description(), getOptionsWithHelp());
  }

  public final void printHelp(Shell shellState, int width) {
    shellState.printHelp(usage(), "description: " + this.description(), getOptionsWithHelp(), width);
  }

  // Get options with help
  public final Options getOptionsWithHelp() {
    Options opts = getOptions();
    opts.addOption(new Option(ShellOptions.helpOption, ShellOptions.helpLongOption, false, "display this help"));
    return opts;
  }

  // General usage is just the command
  public String usage() {
    return getName();
  }

  // General Options are empty
  public Options getOptions() {
    return new Options();
  }
}
