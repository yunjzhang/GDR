package org.apache.gdr.mapreduce;

import org.apache.commons.cli.*;

public class OptionsHelper {
    private CommandLine commandLine;

    public void parseOptions(Options options, String[] args) throws ParseException {
        CommandLineParser parser = new GnuParser();
        commandLine = parser.parse(options, args);
    }

    public Option[] getOptions() {
        return commandLine.getOptions();
    }

    public String getOptionsAsString() {
        StringBuilder buf = new StringBuilder();
        for (Option option : commandLine.getOptions()) {
            buf.append(" ");
            buf.append(option.getOpt());
            if (option.hasArg()) {
                buf.append("=");
                buf.append(option.getValue());
            }
        }
        return buf.toString();
    }

    public String getOptionValue(Option option) {
        return commandLine.getOptionValue(option.getOpt());
    }

    public boolean hasOption(Option option) {
        return commandLine.hasOption(option.getOpt());
    }

    public void printUsage(String programName, Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(programName, options);
    }
}
