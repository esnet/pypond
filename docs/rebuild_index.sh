#/bin/bash

# Re-scan the pypond module to create the .rst files.
# Mostly so I don't need to remember this command.

sphinx-apidoc -f -o source ../pypond/
